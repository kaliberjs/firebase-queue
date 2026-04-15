const TransactionHelper = require('./transaction_helper')
/** @import { ErrorToErrorDetails, ProcessTask, ReportError, SpecWithDefaults, Task } from './types.ts' */
/** @import { database } from 'firebase-admin' */

module.exports = QueueWorker

/**
 * @arg {{
 *   processId: string,
 *   tasksRef: database.Reference,
 *   spec: SpecWithDefaults,
 *   errorToErrorDetails: ErrorToErrorDetails | null,
 *   processTask: ProcessTask,
 *   reportError: ReportError,
 * }} props
 */
function QueueWorker({ processId, tasksRef, spec, errorToErrorDetails, processTask, reportError }) {
  const { startState } = spec
  const newTaskRef = tasksRef.orderByChild('_state').equalTo(startState).limitToFirst(1)

  let transactionHelper = new TransactionHelper({ processId, spec, errorToErrorDetails })
  /** @type {null | { resolve(): void, promise: Promise<void> }} */
  let shutdownStarted = null
  let busy = false

  this.shutdown = shutdown

  waitForNextTask()

  function waitForNextTask() {
    newTaskRef.on('child_added', tryToProcessAndCatchError, reportError)
  }

  function stopWaitingForNextTask() {
    newTaskRef.off('child_added', tryToProcessAndCatchError)
  }

  /** @arg {{ ref: database.Reference }} props */
  async function tryToProcessAndCatchError({ ref }) {
    stopWaitingForNextTask()

    busy = true
    await claimAndProcess(ref).catch(reportError)
    busy = false

    if (shutdownStarted) finishShutdown()
    else setImmediate(waitForNextTask) // let node.js breathe
  }

  /** @arg {database.Reference} ref */
  async function claimAndProcess(ref) {
    const nextTransactionHelper = transactionHelper.cloneForNextTask()
    const { committed, snapshot } = await nextTransactionHelper.claim(ref)

    if (committed && snapshot.exists()) {
      transactionHelper = nextTransactionHelper
      await process(snapshot)
    }
  }

  /** @arg {database.DataSnapshot} snapshot */
  async function process(snapshot) {
    const { ref } = snapshot

    const data = snapshot.val()
    removeQueueProperties(data)

    await new Promise(resolve => resolve(processTask(data, { snapshot, setProgress })))
      .then(resolve, reject)

    /** @arg {Task} task */
    function removeQueueProperties(task) {
      const properties = ['_state', '_state_changed', '_owner', '_progress', '_error_details']
      properties.forEach(properties => { delete task[properties] })
    }

    /** @arg {Record<string, any>} newTask */
    async function resolve(newTask) {
      const { committed } = await transactionHelper.resolveWith(ref, newTask)
      if (!committed) throw new Error(`Could not resolve task:\n${JSON.stringify(newTask, null, 2)}`)
    }

    /** @arg {Error} error */
    async function reject(error) {
      const { committed } = await transactionHelper.rejectWith(ref, error)
      if (!committed) throw new Error(`Could not reject task with error:\n${error}`)
    }

    /** @arg {number} progress */
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
    if (shutdownStarted) throw new Error(`Shutdown was already called`)

    shutdownStarted = createDeferred()

    if (!busy) finishShutdown()

    return shutdownStarted.promise
  }

  function finishShutdown() {
    stopWaitingForNextTask()
    // @ts-expect-error
    shutdownStarted.resolve()
  }
}

function createDeferred() {
  /** @type {(value: any) => void} */
  let resolve
  return {
    resolve: () => resolve(undefined),
    promise: new Promise(res => { resolve = res })
  }
}
