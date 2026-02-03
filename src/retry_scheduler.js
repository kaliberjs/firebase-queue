/** @import { ReportError, RetryScheduler as RetrySchedulerType } from './types.ts' */
/** @import { database } from 'firebase-admin' */

module.exports = { createRetryScheduler }

/**
 * Creates a retry scheduler that polls for tasks ready to be retried.
 * This handles the case where a task was scheduled for retry but the
 * original worker died before the retry time.
 * 
 * @param {Object} options
 * @param {database.Reference} options.tasksRef - Firebase reference to the tasks
 * @param {string | null} options.startState - The start state for tasks
 * @param {number} [options.pollIntervalMs=60000] - How often to poll for retries (ms)
 * @param {ReportError | null} [options.reportError] - Optional error handler for polling failures
 * @returns {RetrySchedulerType}
 */
function createRetryScheduler({ tasksRef, startState, pollIntervalMs = 60000, reportError = null }) {
  /** @type {ReturnType<typeof setInterval> | null} */
  let intervalId = null
  let stopped = false

  return {
    start,
    stop,
    isRunning: () => intervalId !== null
  }

  /**
   * Starts polling for retryable tasks.
   * @returns {void}
   */
  function start() {
    if (intervalId) return
    stopped = false
    intervalId = setInterval(pollForRetries, pollIntervalMs)
    pollForRetries()
  }

  /**
   * Stops polling for retryable tasks.
   * @returns {Promise<void>}
   */
  async function stop() {
    stopped = true
    if (intervalId) {
      clearInterval(intervalId)
      intervalId = null
    }
  }

  /**
   * Polls Firebase for tasks that have passed their retry time.
   * @returns {Promise<void>}
   */
  async function pollForRetries() {
    if (stopped) return
    
    try {
      const now = Date.now()
      const snapshot = await tasksRef
        .orderByChild('_retry_at')
        .startAt(1)
        .endAt(now)
        .once('value')
      
      if (!snapshot.exists()) return
      
      /** @type {Record<string, null>} */
      const updates = {}
      const tasks = snapshot.val() || {}
      for (const [key, task] of Object.entries(tasks)) {
        // @ts-ignore - task is any from Firebase
        if ((task._state || null) === startState && task._retry_at && task._retry_at <= now) {
          updates[`${key}/_retry_at`] = null
        }
      }
      
      if (Object.keys(updates).length > 0) {
        await tasksRef.update(updates)
      }
    } catch (error) {
      if (reportError) {
        try { reportError(/** @type {Error} */(error)) } catch (_) {}
      }
    }
  }
}
