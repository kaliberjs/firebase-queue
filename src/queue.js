
const { createWorker } = require('./queue_worker.js')
const { createObservability, noopObservability } = require('./observability.js')

module.exports = { createQueue }

const DEFAULT_HEARTBEAT_INTERVAL = 30000

function createQueue({
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
    maxConcurrent = null,
    observability: observabilityConfig = null
  } = {}
}) {
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

  const retryConfig = normalizeRetryConfig(retry)
  const queueId = tasksRef.push().key
  const stats = { processed: 0, failed: 0, retried: 0 }
  let shutdownStarted = null
  let paused = false
  
  const obs = observabilityConfig 
    ? createObservability({ ...observabilityConfig, queueId })
    : noopObservability
  
  let workers = createWorkers()
  
  obs.log('info', 'queue.started', { numWorkers })
  obs.gauge('queue.workers.total', numWorkers)
  obs.gauge('queue.workers.busy', 0)
  obs.gauge('queue.paused', 0)

  return {
    shutdown,
    getStats,
    pause,
    resume,
    isPaused: () => paused
  }

  async function shutdown() {
    if (shutdownStarted) return shutdownStarted
    shutdownStarted = Promise.all(workers.map(worker => worker.shutdown()))
    const result = await shutdownStarted
    workers = null
    obs.log('info', 'queue.shutdown')
    obs.gauge('queue.workers.total', 0)
    obs.gauge('queue.workers.busy', 0)
    return result
  }

  function pause() {
    if (paused) return
    paused = true
    obs.log('info', 'queue.paused')
    obs.gauge('queue.paused', 1)
    if (lifecycle?.onQueuePaused) {
      try { lifecycle.onQueuePaused() } catch (_) {}
    }
  }

  function resume() {
    if (!paused) return
    paused = false
    for (const w of workers) w.resume()
    obs.log('info', 'queue.resumed')
    obs.gauge('queue.paused', 0)
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
    return [...Array(numWorkers).keys()].map(index =>
      createWorker({
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
        isPaused: () => paused,
        observability: obs
      })
    )
  }
}

// --- Validation helpers ---

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

// --- Retry config ---

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
