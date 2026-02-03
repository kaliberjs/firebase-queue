/** @import { Task, Lifecycle } from './types.ts' */
/** @import { database } from 'firebase-admin' */

/** @typedef {ReturnType<typeof import('./transaction_helper').createTransactionHelper>} TransactionHelper */

/**
 * Properties reserved by the queue system
 * @type {readonly string[]}
 */
const QUEUE_PROPERTIES = ['_state', '_state_changed', '_owner', '_progress', '_error_details', '_heartbeat', '_started_at', '_duration_ms', '_retry_attempt', '_retry_at', '_last_error']

/**
 * @typedef {Object} TaskContext
 * @property {string} taskId
 * @property {number | null} claimTime
 * @property {database.Reference} ref
 * @property {database.DataSnapshot} snapshot
 * @property {TransactionHelper} transactionHelper
 * @property {string | null} startState
 * @property {{ processed: number, failed: number, retried: number }} stats
 * @property {NormalizedRetryConfig | null} retryConfig
 * @property {Lifecycle | null} lifecycle
 * @property {Object} observability
 * @property {string} processId
 */

/**
 * @typedef {Object} NormalizedRetryConfig
 * @property {number} maxAttempts
 * @property {(attempt: number, initialDelayMs: number) => number} backoff
 * @property {number} initialDelayMs
 * @property {number} maxDelayMs
 * @property {((error: Error) => boolean) | null} retryableErrors
 */

/**
 * @typedef {Object} HeartbeatManager
 * @property {() => void} stop
 */

/**
 * @typedef {Object} Deferred<T>
 * @property {(...args: any[]) => void} resolve
 * @property {Promise<T>} promise
 * @template T
 */

/**
 * Creates a task context object.
 * @param {TaskContext} props
 * @returns {TaskContext}
 */
function createTaskContext(props) {
  return { ...props }
}

/**
 * Removes queue reserved properties from a task object.
 * @param {Task} task
 * @returns {void}
 */
function removeQueueProperties(task) {
  for (const prop of QUEUE_PROPERTIES) { delete task[prop] }
}

/**
 * Creates a heartbeat manager that updates the task heartbeat periodically.
 * @param {database.Reference} ref
 * @param {number | null} heartbeatInterval
 * @param {TransactionHelper} transactionHelper
 * @returns {HeartbeatManager}
 */
function createHeartbeatManager(ref, heartbeatInterval, transactionHelper) {
  /** @type {ReturnType<typeof setInterval> | null} */
  let timer = null
  
  if (heartbeatInterval) {
    timer = setInterval(async () => {
      try {
        await transactionHelper.updateHeartbeat(ref)
      } catch (_) {
        if (timer) {
          clearInterval(timer)
          timer = null
        }
      }
    }, heartbeatInterval)
  }

  return {
    stop() {
      if (timer) {
        clearInterval(timer)
        timer = null
      }
    }
  }
}

/**
 * Handles successful task completion.
 * @param {TaskContext} context
 * @param {Task | null} newTask
 * @returns {Promise<void>}
 */
async function handleTaskResolved(context, newTask) {
  const { taskId, claimTime, ref, transactionHelper, stats, lifecycle, observability, processId } = context
  
  const { committed } = await transactionHelper.resolveWith(ref, newTask)
  if (!committed) throw new Error(`Could not resolve task:\n${JSON.stringify(newTask, null, 2)}`)
  
  const durationMs = claimTime ? Date.now() - claimTime : null
  if (stats) stats.processed++
  
  if (observability) {
    observability.log('info', 'task.completed', { taskId, durationMs })
    observability.increment('queue.tasks.completed', { worker: processId })
    if (durationMs !== null) {
      observability.histogram('queue.task.duration_ms', durationMs, { status: 'completed' })
    }
  }
  
  if (lifecycle?.onTaskCompleted) {
    try { lifecycle.onTaskCompleted(taskId, durationMs, newTask) } catch (_) {}
  }
}

/**
 * Handles task failure with optional retry scheduling.
 * @param {TaskContext} context
 * @param {Error} error
 * @returns {Promise<void>}
 */
async function handleTaskRejected(context, error) {
  const { taskId, claimTime, ref, snapshot, transactionHelper, startState, stats, retryConfig, lifecycle, observability, processId } = context
  
  const durationMs = claimTime ? Date.now() - claimTime : null
  
  if (retryConfig && shouldRetry(error, snapshot, retryConfig)) {
    const retryResult = await scheduleRetry(ref, error, snapshot, startState, retryConfig)
    if (retryResult.scheduled) {
      if (stats) stats.retried++
      
      if (observability) {
        observability.log('info', 'task.retry_scheduled', { taskId, attempt: retryResult.attempt, delayMs: retryResult.delayMs, error: error?.message })
        observability.increment('queue.tasks.retried', { worker: processId, attempt: retryResult.attempt })
      }
      
      if (lifecycle?.onTaskRetryScheduled) {
        try { lifecycle.onTaskRetryScheduled(taskId, retryResult.attempt, retryResult.delayMs, error) } catch (_) {}
      }
      return
    }
  }
  
  const { committed } = await transactionHelper.rejectWith(ref, error)
  if (!committed) throw new Error(`Could not reject task with error:\n${error}`)
  
  if (stats) stats.failed++
  
  if (observability) {
    observability.log('warn', 'task.failed', { taskId, durationMs, error: error?.message })
    observability.increment('queue.tasks.failed', { worker: processId })
    if (durationMs !== null) {
      observability.histogram('queue.task.duration_ms', durationMs, { status: 'failed' })
    }
  }
  
  if (lifecycle?.onTaskFailed) {
    try { lifecycle.onTaskFailed(taskId, durationMs, error) } catch (_) {}
  }
}

/**
 * Determines if an error should trigger a retry.
 * @param {Error} error
 * @param {database.DataSnapshot} snapshot
 * @param {NormalizedRetryConfig} retryConfig
 * @returns {boolean}
 */
function shouldRetry(error, snapshot, retryConfig) {
  const data = snapshot.val()
  const attempt = (data._retry_attempt || 0) + 1
  
  if (attempt > retryConfig.maxAttempts) return false
  if (retryConfig.retryableErrors && !retryConfig.retryableErrors(error)) return false
  
  return true
}

/**
 * @typedef {Object} RetryResult
 * @property {boolean} scheduled
 * @property {number} attempt
 * @property {number} delayMs
 */

/**
 * Schedules a task for retry with exponential backoff.
 * @param {database.Reference} ref
 * @param {Error} error
 * @param {database.DataSnapshot} snapshot
 * @param {string | null} startState
 * @param {NormalizedRetryConfig} retryConfig
 * @returns {Promise<RetryResult>}
 */
async function scheduleRetry(ref, error, snapshot, startState, retryConfig) {
  const data = snapshot.val()
  const attempt = (data._retry_attempt || 0) + 1
  
  const delayMs = Math.min(
    retryConfig.backoff(attempt, retryConfig.initialDelayMs),
    retryConfig.maxDelayMs
  )
  
  const retryAt = Date.now() + delayMs
  
  await ref.update({
    _state: startState,
    _state_changed: { '.sv': 'timestamp' },
    _owner: null,
    _retry_attempt: attempt,
    _retry_at: retryAt,
    _last_error: error?.message || String(error),
  })
  
  return { scheduled: true, attempt, delayMs }
}

/**
 * Creates a deferred promise.
 * @returns {{ resolve: (...args: any[]) => void, promise: Promise<void> }}
 */
function createDeferred() {
  /** @type {((...args: any[]) => void) | null} */
  let resolve = null
  return {
    resolve: (...args) => resolve?.(...args),
    promise: new Promise(res => { resolve = res })
  }
}

module.exports = {
  QUEUE_PROPERTIES,
  createTaskContext,
  removeQueueProperties, 
  createHeartbeatManager,
  handleTaskResolved,
  handleTaskRejected,
  shouldRetry,
  scheduleRetry,
  createDeferred
}
