
const QueueWorker = require('./queue_worker.js')

module.exports = Queue

const DEFAULT_HEARTBEAT_INTERVAL = 30000

function Queue({
  tasksRef,
  processTask,
  reportError,
  options: {
    spec: {
      startState = null,
      inProgressState = 'in_progress',
      finishedState = null,
      errorState = 'error'
    } = {},
    errorToErrorDetails = null,
    numWorkers = 1,
    heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL,
    lifecycle = null,
    retry = null,
    maxConcurrent = null
  } = {}
}) {
  if (!(this instanceof Queue)) throw new Error('You forgot the `new` keyword: `new Queue(...)`')

  const spec = { startState, inProgressState, finishedState, errorState }
  check(tasksRef, isFirebaseRef,
    'tasksRef must be a Firebase reference')

  check(processTask, isFunction,
    'processTask must be a function')

  check(reportError, isFunction,
    'reportError must be a function')

  check(inProgressState, isString,
    'options.spec.inProgressState must be a string')

  check(startState, isNull, [isString, not(inProgressState)],
    'options.spec.startState must be null or a string that !== inProgressState')

  check(finishedState, isNull, [isString, not(inProgressState), not(startState)],
    'options.spec.finishedState must be null or a string that !== inProgressState and !== startState')

  check(errorState, [isString, not(inProgressState), not(startState), not(finishedState)],
    'options.spec.errorState must be a string that !== inProgressState and !== startState and !== finishedState')

  check(numWorkers, isPositiveInteger,
    'options.numWorkers must be a positive integer')

  check(heartbeatInterval, isNull, isPositiveInteger,
    'options.heartbeatInterval must be null or a positive integer')

  check(maxConcurrent, isNull, isPositiveInteger,
    'options.maxConcurrent must be null or a positive integer')

  // Normalize retry configuration
  const retryConfig = normalizeRetryConfig(retry)

  const queueId = tasksRef.push().key
  const stats = { processed: 0, failed: 0, retried: 0 }
  let shutdownStarted = null
  let paused = false
  let workers = createWorkers()

  this.shutdown = shutdown
  this.getStats = getStats
  this.pause = pause
  this.resume = resume
  this.isPaused = () => paused

  async function shutdown() {
    if (shutdownStarted) return shutdownStarted
    shutdownStarted = Promise.all(workers.map(worker => worker.shutdown()))
    const result = await shutdownStarted
    workers = null // allow garbage collection
    return result
  }

  function pause() {
    if (paused) return
    paused = true
    if (lifecycle?.onQueuePaused) {
      try { lifecycle.onQueuePaused() } catch (_) {}
    }
  }

  function resume() {
    if (!paused) return
    paused = false
    for (const w of workers) w.resume()
    if (lifecycle?.onQueueResumed) {
      try { lifecycle.onQueueResumed() } catch (_) {}
    }
  }

  function getStats() {
    const busyWorkers = workers ? workers.filter(w => w.isBusy()).length : 0
    return {
      numWorkers,
      busyWorkers,
      totalProcessed: stats.processed,
      totalFailed: stats.failed,
      totalRetried: stats.retried,
      isPaused: paused
    }
  }

  function createWorkers() {
    return [...Array(numWorkers).keys()].map(createWorker)

    function createWorker(index) {
      return new QueueWorker({
        processId: `${queueId}:${index}`,
        tasksRef,
        spec,
        errorToErrorDetails,
        processTask,
        reportError,
        heartbeatInterval,
        lifecycle,
        stats,
        retryConfig,
        maxConcurrent,
        isPaused: () => paused
      })
    }
  }


  function isFunction(x) { return typeof x === 'function' }
  function isFirebaseRef(x) { return x && [x.on, x.off, x.transaction, x.orderByChild, x.push].every(isFunction) }
  function isString(x) { return typeof x === 'string' }
  function isNull(x) { return x === null }
  function not(y) { return x => x !== y }
  function isPositiveInteger(x) { return typeof x === 'number' && x >= 1 && x % 1 === 0 }

  function check(val, ...rest) {
    const message = rest[rest.length - 1]
    const or = rest.slice(0, rest.length -1)
    const valid = or.reduce(
      (result, and) => result || [].concat(and).reduce(
        (result, isValid) => result && isValid(val),
        true
      ),
      false
    )
    if (!valid) throw new Error(message)
  }
}

function normalizeRetryConfig(retry) {
  if (!retry) return null

  const {
    maxAttempts = 5,
    backoff = 'exponential',
    initialDelayMs = 1000,
    maxDelayMs = 60 * 60 * 1000,
    retryableErrors = null,
  } = retry

  return {
    maxAttempts,
    backoff: typeof backoff === 'function' ? backoff : getBackoffStrategy(backoff),
    initialDelayMs,
    maxDelayMs,
    retryableErrors,
  }
}

function getBackoffStrategy(name) {
  const strategies = {
    fixed: (attempt, initial) => initial,
    linear: (attempt, initial) => initial * attempt,
    exponential: (attempt, initial) => initial * (2 ** (attempt - 1)),
  }
  return strategies[name] || strategies.exponential
}
