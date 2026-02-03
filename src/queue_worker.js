/** @import { ErrorToErrorDetails, ProcessTask, ReportError, SpecWithDefaults, Task, Lifecycle } from './types.ts' */
/** @import { database } from 'firebase-admin' */

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

/**
 * @typedef {Object} WorkerConfig
 * @property {string} processId - Unique worker identifier
 * @property {database.Reference} tasksRef - Firebase reference to tasks
 * @property {SpecWithDefaults} spec - Queue spec with state names
 * @property {ErrorToErrorDetails | null} errorToErrorDetails - Error detail extractor
 * @property {ProcessTask} processTask - Task processing function
 * @property {ReportError} reportError - Error reporter
 * @property {number | null} heartbeatInterval - Heartbeat interval in ms
 * @property {Lifecycle | null} lifecycle - Lifecycle hooks
 * @property {{ processed: number, failed: number, retried: number }} stats - Shared stats object
 * @property {Object | null} retryConfig - Retry configuration
 * @property {number | null} maxConcurrent - Max concurrent tasks (unused currently)
 * @property {() => boolean} isPaused - Returns true if queue is paused
 * @property {Object} observability - Observability instance
 */

/**
 * @typedef {Object} Worker
 * @property {() => Promise<void>} shutdown - Shuts down the worker
 * @property {() => boolean} isBusy - Returns true if worker is processing
 * @property {() => void} resume - Signals resume from pause
 */

/**
 * @typedef {Object} WorkerState
 * @property {string} processId
 * @property {database.Reference} tasksRef
 * @property {SpecWithDefaults} spec
 * @property {string | null} startState
 * @property {ErrorToErrorDetails | null} errorToErrorDetails
 * @property {ProcessTask} processTask
 * @property {ReportError} reportError
 * @property {number | null} heartbeatInterval
 * @property {Lifecycle | null} lifecycle
 * @property {{ processed: number, failed: number, retried: number }} stats
 * @property {Object | null} retryConfig
 * @property {number | null} maxConcurrent
 * @property {() => boolean} isPaused
 * @property {Object} observability
 * @property {database.Query} newTaskRef
 * @property {ReturnType<typeof createTransactionHelper>} transactionHelper
 * @property {boolean} busy
 * @property {{ promise: Promise<void>, resolve: () => void } | null} shutdownStarted
 * @property {(() => void) | null} resumeResolver
 */

/**
 * Creates a queue worker that processes tasks.
 * @param {WorkerConfig} config
 * @returns {Worker}
 */
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

/**
 * Creates the initial worker state.
 * @param {WorkerConfig} config
 * @returns {WorkerState}
 */
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
    busy: false,
    shutdownStarted: null,
    resumeResolver: null
  }
}

// --- Worker lifecycle ---

/**
 * Starts listening for new tasks.
 * @param {WorkerState} state
 * @returns {void}
 */
function waitForNextTask(state) {
  state.newTaskRef.on('child_added', 
    snapshot => tryToProcessAndCatchError(state, snapshot), 
    state.reportError
  )
}

/**
 * Stops listening for new tasks.
 * @param {WorkerState} state
 * @returns {void}
 */
function stopWaitingForNextTask(state) {
  state.newTaskRef.off('child_added')
}

/**
 * Handles a new task, catching any errors.
 * @param {WorkerState} state
 * @param {database.DataSnapshot} snapshot
 * @returns {Promise<void>}
 */
async function tryToProcessAndCatchError(state, snapshot) {
  stopWaitingForNextTask(state)
  
  // Check pause before processing
  if (state.isPaused()) {
    await waitWhilePaused(state)
  }
  
  state.busy = true
  state.observability.gauge('queue.workers.busy', 1, { worker: state.processId })
  
  await claimAndProcess(state, snapshot.ref).catch(state.reportError)
  
  state.busy = false
  state.observability.gauge('queue.workers.busy', 0, { worker: state.processId })
  
  if (state.shutdownStarted) {
    finishShutdown(state)
  } else {
    setImmediate(() => waitForNextTask(state))
  }
}

/**
 * Waits while the queue is paused.
 * @param {WorkerState} state
 * @returns {Promise<void>}
 */
async function waitWhilePaused(state) {
  while (state.isPaused() && !state.shutdownStarted) {
    await new Promise(resolve => { state.resumeResolver = resolve })
    state.resumeResolver = null
  }
}

/**
 * Signals resume from pause.
 * @param {WorkerState} state
 * @returns {void}
 */
function signalResume(state) {
  if (state.resumeResolver) state.resumeResolver()
}

/**
 * Attempts to claim and process a task.
 * @param {WorkerState} state
 * @param {database.Reference} ref
 * @returns {Promise<void>}
 */
async function claimAndProcess(state, ref) {
  const nextTransactionHelper = state.transactionHelper.cloneForNextTask()
  const { committed, snapshot } = await nextTransactionHelper.claim(ref)
  
  if (committed && snapshot.exists()) {
    state.transactionHelper = nextTransactionHelper
    await processTask$(state, snapshot)
  }
}

/**
 * Processes a claimed task.
 * @param {WorkerState} state
 * @param {database.DataSnapshot} snapshot
 * @returns {Promise<void>}
 */
async function processTask$(state, snapshot) {
  const { ref, key: taskId } = snapshot
  const { heartbeatInterval, processTask, lifecycle, stats, observability, processId } = state
  const claimTime = state.transactionHelper.getClaimTime()
  const heartbeat = createHeartbeatManager(ref, heartbeatInterval, state.transactionHelper)

  lifecycle?.onTaskClaimed?.(taskId, processId)
  observability.log('debug', 'task.claimed', { taskId, worker: processId })
  observability.increment('queue.tasks.claimed', { worker: processId })

  const data = snapshot.val()
  removeQueueProperties(data)

  const context = createTaskContext({
    taskId, claimTime, ref, snapshot,
    transactionHelper: state.transactionHelper,
    startState: state.startState,
    stats, retryConfig: state.retryConfig, lifecycle, observability, processId
  })

  try {
    const newTask = await processTask(data, { snapshot, setProgress })
    heartbeat.stop()
    await handleTaskResolved(context, newTask)
  } catch (error) {
    heartbeat.stop()
    await handleTaskRejected(context, error)
  }

  /**
   * Updates task progress.
   * @param {number} progress
   * @returns {Promise<void>}
   */
  async function setProgress(progress) {
    const { committed, snapshot } = await state.transactionHelper.updateProgressWith(ref, progress)
    if (!committed || !snapshot.exists()) {
      throw new Error('Can\'t update progress - task no longer owned by this process, no longer in progress, removed, or network failure')
    }
  }
}

// --- Shutdown ---

/**
 * Initiates worker shutdown.
 * @param {WorkerState} state
 * @returns {Promise<void>}
 */
async function workerShutdown(state) {
  if (state.shutdownStarted) return state.shutdownStarted.promise
  
  state.shutdownStarted = createDeferred()
  
  // Signal resume in case paused, so it can check shutdown flag
  signalResume(state)
  
  if (!state.busy) {
    finishShutdown(state)
  }
  
  return state.shutdownStarted.promise
}

/**
 * Completes worker shutdown.
 * @param {WorkerState} state
 * @returns {void}
 */
function finishShutdown(state) {
  stopWaitingForNextTask(state)
  state.shutdownStarted.resolve()
}
