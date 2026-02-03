
module.exports = RetryScheduler

/**
 * Polls for tasks that are ready to be retried.
 * This handles the case where a task was scheduled for retry
 * but the original worker died before the retry time.
 * 
 * @param {Object} options
 * @param {Object} options.tasksRef - Firebase reference to the tasks
 * @param {string|null} options.startState - The start state for tasks (null or string)
 * @param {number} options.pollIntervalMs - How often to poll for retries (default: 60000)
 */
function RetryScheduler({ tasksRef, startState, pollIntervalMs = 60000 }) {
  let intervalId = null
  let stopped = false

  this.start = start
  this.stop = stop
  this.isRunning = () => intervalId !== null

  function start() {
    if (intervalId) return
    stopped = false
    intervalId = setInterval(pollForRetries, pollIntervalMs)
    pollForRetries() // Initial poll
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
        .startAt(1) // Exclude null/undefined
        .endAt(now)
        .once('value')
      
      if (!snapshot.exists()) return
      
      const updates = {}
      const tasks = snapshot.val() || {}
      for (const [key, task] of Object.entries(tasks)) {
        // Only touch tasks in start state with past retry_at
        if ((task._state || null) === startState && task._retry_at && task._retry_at <= now) {
          // Clear _retry_at to allow claiming
          updates[`${key}/_retry_at`] = null
        }
      }
      
      if (Object.keys(updates).length > 0) {
        await tasksRef.update(updates)
      }
    } catch (e) {
      // Silently fail - will retry on next poll
    }
  }
}
