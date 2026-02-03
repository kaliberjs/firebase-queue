'use strict'

const TransactionHelper = require('./transaction_helper')

module.exports = QueueWorker

function QueueWorker({ processId, tasksRef, spec, errorToErrorDetails, processTask, reportError, heartbeatInterval, lifecycle, stats, retryConfig, maxConcurrent, isPaused }) {
  const { startState } = spec
  const newTaskRef = tasksRef.orderByChild('_state').equalTo(startState).limitToFirst(1)

  let transactionHelper = new TransactionHelper({ 
    processId, 
    spec, 
    errorToErrorDetails,
    onTransactionRetry: lifecycle?.onTransactionRetry
  })
  let shutdownStarted = null
  let busy = false
  let resumeResolver = null

  this.shutdown = shutdown
  this.isBusy = () => busy
  this.resume = signalResume

  waitForNextTask()

  function signalResume() {
    if (resumeResolver) {
      resumeResolver()
      resumeResolver = null
    }
  }

  async function waitWhilePaused() {
    if (!isPaused || !isPaused()) return
    await new Promise(resolve => { resumeResolver = resolve })
  }

  function waitForNextTask() {
    // Check if paused before listening for new tasks
    if (isPaused?.()) {
      new Promise(resolve => { resumeResolver = resolve })
        .then(() => setImmediate(waitForNextTask))
      return
    }
    newTaskRef.on('child_added', tryToProcessAndCatchError, reportError)
  }

  function stopWaitingForNextTask() {
    newTaskRef.off('child_added', tryToProcessAndCatchError)
  }

  async function tryToProcessAndCatchError({ ref }) {
    stopWaitingForNextTask()

    busy = true
    await claimAndProcess(ref).catch(reportError)
    busy = false

    if (shutdownStarted) finishShutdown()
    else setImmediate(waitForNextTask) // let node.js breathe
  }

  async function claimAndProcess(ref) {
    const nextTransactionHelper = transactionHelper.cloneForNextTask()
    const { committed, snapshot } = await nextTransactionHelper.claim(ref)

    if (committed && snapshot.exists()) {
      transactionHelper = nextTransactionHelper
      
      const taskId = snapshot.key
      if (lifecycle?.onTaskClaimed) {
        try { lifecycle.onTaskClaimed(taskId, processId) } catch (_) {}
      }
      
      await process(snapshot)
    }
  }

  async function process(snapshot) {
    const { ref, key: taskId } = snapshot
    const claimTime = transactionHelper.getClaimTime()

    // Start heartbeat interval
    let heartbeatTimer = null
    if (heartbeatInterval) {
      heartbeatTimer = setInterval(async () => {
        try {
          await transactionHelper.updateHeartbeat(ref)
        } catch (e) {
          // Heartbeat failed - task may have been taken over or removed
          // Don't report as error, just stop the heartbeat
          if (heartbeatTimer) {
            clearInterval(heartbeatTimer)
            heartbeatTimer = null
          }
        }
      }, heartbeatInterval)
    }

    const stopHeartbeat = () => {
      if (heartbeatTimer) {
        clearInterval(heartbeatTimer)
        heartbeatTimer = null
      }
    }

    const data = snapshot.val()
    removeQueueProperties(data)

    await new Promise(resolve => resolve(processTask(data, { snapshot, setProgress })))
      .then(
        newTask => resolve(newTask, claimTime),
        error => reject(error, claimTime, snapshot)
      )

    function removeQueueProperties(task) {
      const properties = ['_state', '_state_changed', '_owner', '_progress', '_error_details', '_heartbeat', '_started_at', '_duration_ms', '_retry_attempt', '_retry_at', '_last_error']
      for (const prop of properties) { delete task[prop] }
    }

    async function resolve(newTask, claimTime) {
      stopHeartbeat()
      const { committed } = await transactionHelper.resolveWith(ref, newTask)
      if (!committed) throw new Error(`Could not resolve task:\n${JSON.stringify(newTask, null, 2)}`)
      
      const durationMs = claimTime ? Date.now() - claimTime : null
      if (stats) stats.processed++
      if (lifecycle?.onTaskCompleted) {
        try { lifecycle.onTaskCompleted(taskId, durationMs, newTask) } catch (_) {}
      }
    }

    async function reject(error, claimTime, snapshot) {
      stopHeartbeat()
      
      const durationMs = claimTime ? Date.now() - claimTime : null
      
      // Check if should retry
      if (retryConfig && shouldRetry(error, snapshot)) {
        const retryResult = await scheduleRetry(ref, error, snapshot)
        if (retryResult.scheduled) {
          if (stats) stats.retried++
          if (lifecycle?.onTaskRetryScheduled) {
            try { lifecycle.onTaskRetryScheduled(taskId, retryResult.attempt, retryResult.delayMs, error) } catch (_) {}
          }
          return
        }
      }
      
      const { committed } = await transactionHelper.rejectWith(ref, error)
      if (!committed) throw new Error(`Could not reject task with error:\n${error}`)
      
      if (stats) stats.failed++
      if (lifecycle?.onTaskFailed) {
        try { lifecycle.onTaskFailed(taskId, durationMs, error) } catch (_) {}
      }
    }

    function shouldRetry(error, snapshot) {
      const data = snapshot.val()
      const attempt = (data._retry_attempt || 0) + 1
      
      if (attempt > retryConfig.maxAttempts) return false
      if (retryConfig.retryableErrors && !retryConfig.retryableErrors(error)) return false
      
      return true
    }

    async function scheduleRetry(ref, error, snapshot) {
      const data = snapshot.val()
      const attempt = (data._retry_attempt || 0) + 1
      
      if (attempt > retryConfig.maxAttempts) {
        return { scheduled: false }
      }
      
      const delayMs = Math.min(
        retryConfig.backoff(attempt, retryConfig.initialDelayMs),
        retryConfig.maxDelayMs
      )
      
      const retryAt = Date.now() + delayMs
      
      await ref.update({
        _state: startState,          // Reset to start state for retry
        _state_changed: { '.sv': 'timestamp' },
        _owner: null,
        _retry_attempt: attempt,
        _retry_at: retryAt,
        _last_error: error?.message || String(error),
      })
      
      return { scheduled: true, attempt, delayMs }
    }

    async function setProgress(progress) {
      const { committed, snapshot } = await transactionHelper.updateProgressWith(ref, progress)

      if (!committed || !snapshot.exists()) throw new Error('Can\'t update progress - ' +
        'current task no longer owned by this process, ' +
        'task no longer in progress, ' +
        'task has been removed or ' +
        'network communication failure'
      )
    }
  }

  async function shutdown() {
    /* istanbul ignore if - we could return the promise but rather signal the flaw at the caller */
    if (shutdownStarted) throw new Error(`Shutdown was already called`)

    shutdownStarted = createDeferred()

    // If waiting for resume, resolve it to unblock
    if (resumeResolver) {
      resumeResolver()
      resumeResolver = null
    }

    if (!busy) finishShutdown()

    return shutdownStarted.promise
  }

  function finishShutdown() {
    stopWaitingForNextTask()
    shutdownStarted.resolve()
  }
}

function createDeferred() {
  let resolve = null
  return {
    resolve: (...args) => resolve(...args),
    promise: new Promise(res => { resolve = res })
  }
}
