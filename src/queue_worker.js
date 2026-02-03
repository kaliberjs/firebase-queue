
const { createTransactionHelper } = require('./transaction_helper')
const {
  createTaskContext,
  removeQueueProperties,
  createHeartbeatManager,
  handleTaskResolved,
  handleTaskRejected,
  createDeferred
} = require('./task_helpers')

module.exports = { createWorker }

function createWorker(config) {
  const state = createWorkerState(config)
  
  waitForNextTask(state)
  
  return {
    shutdown: () => workerShutdown(state),
    isBusy: () => state.busy,
    resume: () => signalResume(state)
  }
}

// --- State ---

function createWorkerState(config) {
  const { processId, tasksRef, spec, errorToErrorDetails, processTask, reportError, heartbeatInterval, lifecycle, stats, retryConfig, maxConcurrent, isPaused, observability } = config
  const { startState } = spec
  
  return {
    // Config (immutable)
    processId,
    tasksRef,
    spec,
    startState,
    errorToErrorDetails,
    processTask,
    reportError,
    heartbeatInterval,
    lifecycle,
    stats,
    retryConfig,
    maxConcurrent,
    isPaused,
    observability,
    
    // Derived
    newTaskRef: tasksRef.orderByChild('_state').equalTo(startState).limitToFirst(1),
    transactionHelper: createTransactionHelper({ 
      processId, 
      spec, 
      errorToErrorDetails,
      onTransactionRetry: lifecycle?.onTransactionRetry
    }),
    
    // Mutable state
    shutdownStarted: null,
    busy: false,
    resumeResolver: null,
  }
}

// --- Worker lifecycle ---

function signalResume(state) {
  if (state.resumeResolver) {
    state.resumeResolver()
    state.resumeResolver = null
  }
}

function waitForNextTask(state) {
  if (state.isPaused?.()) {
    new Promise(resolve => { state.resumeResolver = resolve })
      .then(() => setImmediate(() => waitForNextTask(state)))
    return
  }
  state.newTaskRef.on('child_added', snapshot => tryToProcessAndCatchError(state, snapshot), state.reportError)
}

function stopWaitingForNextTask(state) {
  state.newTaskRef.off('child_added')
}

async function tryToProcessAndCatchError(state, { ref }) {
  stopWaitingForNextTask(state)

  state.busy = true
  await claimAndProcess(state, ref).catch(state.reportError)
  state.busy = false

  if (state.shutdownStarted) finishShutdown(state)
  else setImmediate(() => waitForNextTask(state))
}

async function claimAndProcess(state, ref) {
  const nextTransactionHelper = state.transactionHelper.cloneForNextTask()
  const { committed, snapshot } = await nextTransactionHelper.claim(ref)

  if (committed && snapshot.exists()) {
    state.transactionHelper = nextTransactionHelper
    
    const taskId = snapshot.key
    
    if (state.observability) {
      state.observability.log('debug', 'task.claimed', { taskId, workerId: state.processId })
      state.observability.increment('queue.tasks.claimed', { worker: state.processId })
    }
    
    if (state.lifecycle?.onTaskClaimed) {
      try { state.lifecycle.onTaskClaimed(taskId, state.processId) } catch (_) {}
    }
    
    await processTask$(state, snapshot)
  }
}

async function processTask$(state, snapshot) {
  const { ref, key: taskId } = snapshot
  const claimTime = state.transactionHelper.getClaimTime()
  const heartbeat = createHeartbeatManager(ref, state.heartbeatInterval, state.transactionHelper)

  const data = snapshot.val()
  removeQueueProperties(data)

  const context = createTaskContext({
    taskId, claimTime, ref, snapshot,
    transactionHelper: state.transactionHelper,
    startState: state.startState,
    stats: state.stats,
    retryConfig: state.retryConfig,
    lifecycle: state.lifecycle,
    observability: state.observability,
    processId: state.processId
  })

  async function setProgress(progress) {
    const { committed, snapshot } = await state.transactionHelper.updateProgressWith(ref, progress)
    if (!committed || !snapshot.exists()) {
      throw new Error('Can\'t update progress - task no longer owned by this process, no longer in progress, removed, or network failure')
    }
  }

  try {
    const newTask = await state.processTask(data, { snapshot, setProgress })
    heartbeat.stop()
    await handleTaskResolved(context, newTask)
  } catch (error) {
    heartbeat.stop()
    await handleTaskRejected(context, error)
  }
}

// --- Shutdown ---

async function workerShutdown(state) {
  /* istanbul ignore if */
  if (state.shutdownStarted) throw new Error(`Shutdown was already called`)

  state.shutdownStarted = createDeferred()

  if (state.resumeResolver) {
    state.resumeResolver()
    state.resumeResolver = null
  }

  if (!state.busy) finishShutdown(state)

  return state.shutdownStarted.promise
}

function finishShutdown(state) {
  stopWaitingForNextTask(state)
  state.shutdownStarted.resolve()
}
