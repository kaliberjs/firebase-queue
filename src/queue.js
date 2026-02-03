/** @import { Config, Options, Spec, SpecWithDefaults, Lifecycle, RetryConfig, BackoffFunction, Queue, QueueStats, ProcessTask, ReportError, ErrorToErrorDetails, ObservabilityConfig } from './types.ts' */
/** @import { database } from 'firebase-admin' */

const { createWorker } = require('./queue_worker.js')
const { createObservability, noopObservability } = require('./observability.js')

module.exports = { createQueue }

const DEFAULT_HEARTBEAT_INTERVAL = 30000

/**
 * @typedef {Object} NormalizedRetryConfig
 * @property {number} maxAttempts
 * @property {BackoffFunction} backoff
 * @property {number} initialDelayMs
 * @property {number} maxDelayMs
 * @property {((error: Error) => boolean) | null} retryableErrors
 */

/**
 * Creates a new task queue for processing Firebase tasks.
 * 
 * @param {Config} config
 * @returns {Queue}
 */
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
  /** @type {SpecWithDefaults} */
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

  /** @type {NormalizedRetryConfig | null} */
  const retryConfig = normalizeRetryConfig(retry)
  const queueId = tasksRef.push().key
  const stats = { processed: 0, failed: 0, retried: 0 }
  /** @type {Promise<void[]> | null} */
  let shutdownStarted = null
  let paused = false
  
  const obs = observabilityConfig 
    ? createObservability({ ...observabilityConfig, queueId })
    : noopObservability
  
  /** @type {ReturnType<typeof createWorker>[] | null} */
  let workers = initWorkers()
  
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

  /**
   * Shuts down all workers and the queue.
   * @returns {Promise<void>}
   */
  async function shutdown() {
    if (shutdownStarted) return shutdownStarted.then(() => {})
    if (!workers) return
    shutdownStarted = Promise.all(workers.map(worker => worker.shutdown()))
    await shutdownStarted
    workers = null
    obs.log('info', 'queue.shutdown')
    obs.gauge('queue.workers.total', 0)
    obs.gauge('queue.workers.busy', 0)
  }

  /**
   * Pauses processing of new tasks.
   * @returns {void}
   */
  function pause() {
    if (paused) return
    paused = true
    obs.log('info', 'queue.paused')
    obs.gauge('queue.paused', 1)
    if (lifecycle?.onQueuePaused) {
      try { lifecycle.onQueuePaused() } catch (_) {}
    }
  }

  /**
   * Resumes processing of tasks.
   * @returns {void}
   */
  function resume() {
    if (!paused) return
    paused = false
    if (workers) {
      for (const w of workers) w.resume()
    }
    obs.log('info', 'queue.resumed')
    obs.gauge('queue.paused', 0)
    if (lifecycle?.onQueueResumed) {
      try { lifecycle.onQueueResumed() } catch (_) {}
    }
  }

  /**
   * Returns queue statistics.
   * @returns {QueueStats}
   */
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

  /**
   * Creates all worker instances.
   * @returns {ReturnType<typeof createWorker>[]}
   */
  function initWorkers() {
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

/** @param {any} x @returns {x is Function} */
function isFunction(x) { return typeof x === 'function' }

/** @param {any} x @returns {x is database.Reference} */
function isFirebaseRef(x) { return x && [x.on, x.off, x.transaction, x.orderByChild, x.push].every(isFunction) }

/** @param {any} x @returns {x is string} */
function isString(x) { return typeof x === 'string' }

/** @param {any} x @returns {x is null} */
function isNull(x) { return x === null }

/** @param {any} y @returns {(x: any) => boolean} */
function not(y) { return x => x !== y }

/** @param {any} x @returns {boolean} */
function isPositiveInteger(x) { return typeof x === 'number' && x >= 1 && x % 1 === 0 }

/**
 * Validates a value against a set of predicates.
 * @param {any} val
 * @param {...(((x: any) => boolean) | ((x: any) => boolean)[] | string)} rest
 * @returns {void}
 * @throws {Error}
 */
function check(val, ...rest) {
  const message = /** @type {string} */(rest[rest.length - 1])
  const or = rest.slice(0, rest.length - 1)
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

/**
 * Normalizes retry configuration with defaults.
 * @param {RetryConfig | null} retry
 * @returns {NormalizedRetryConfig | null}
 */
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

/**
 * Returns a backoff function by name.
 * @param {'fixed' | 'linear' | 'exponential'} name
 * @returns {BackoffFunction}
 */
function getBackoffStrategy(name) {
  /** @type {Record<string, BackoffFunction>} */
  const strategies = {
    fixed: (attempt, initial) => initial,
    linear: (attempt, initial) => initial * attempt,
    exponential: (attempt, initial) => initial * (2 ** (attempt - 1)),
  }
  return strategies[name] || strategies.exponential
}
