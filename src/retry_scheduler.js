
module.exports = { createRetryScheduler }

/**
 * Polls for tasks that are ready to be retried.
 * This handles the case where a task was scheduled for retry
 * but the original worker died before the retry time.
 * 
 * @param {Object} options
 * @param {Object} options.tasksRef - Firebase reference to the tasks
 * @param {string|null} options.startState - The start state for tasks (null or string)
 * @param {number} options.pollIntervalMs - How often to poll for retries (default: 60000)
 * @param {Function} options.reportError - Optional error handler for polling failures
 */
function createRetryScheduler({ tasksRef, startState, pollIntervalMs = 60000, reportError = null }) {
  let intervalId = null
  let stopped = false

  return {
    start,
    stop,
    isRunning: () => intervalId !== null
  }

  function start() {
    if (intervalId) return
    stopped = false
    intervalId = setInterval(pollForRetries, pollIntervalMs)
    pollForRetries()
  }

  async function stop() {
    stopped = true
    if (intervalId) {
      clearInterval(intervalId)
      intervalId = null
    }
  }

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
      
      const updates = {}
      const tasks = snapshot.val() || {}
      for (const [key, task] of Object.entries(tasks)) {
        if ((task._state || null) === startState && task._retry_at && task._retry_at <= now) {
          updates[`${key}/_retry_at`] = null
        }
      }
      
      if (Object.keys(updates).length > 0) {
        await tasksRef.update(updates)
      }
    } catch (error) {
      if (reportError) {
        try { reportError(error) } catch (_) {}
      }
    }
  }
}
