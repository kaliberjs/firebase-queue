'use strict'

const TransactionHelper = require('./transaction_helper')

module.exports = QueueWorker

function QueueWorker({ processId, tasksRef, spec, processTask, reportError }) {

  const { startState } = spec
  const newTaskRef = tasksRef.orderByChild('_state').equalTo(startState).limitToFirst(1)

  let transactionHelper = new TransactionHelper({ processId, spec })
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
      await process(snapshot)
    }
  }

  async function process(snapshot) {
    const { ref } = snapshot

    const data = snapshot.val()
    removeQueueProperties(data)

    await new Promise(resolve => resolve(processTask(data, { snapshot, setProgress })))
      .then(resolve, reject)

    function removeQueueProperties(task) {
      const properties = ['_state', '_state_changed', '_owner', '_progress', '_error_details']
      properties.forEach(properties => { delete task[properties] })
    }

    async function resolve(newTask) {
      const { committed } = await transactionHelper.resolveWith(ref, newTask)
      if (!committed) throw new Error(`Could not resolve task:\n${JSON.stringify(newTask, null, 2)}`)
    }

    async function reject(error) {
      const { committed } = await transactionHelper.rejectWith(ref, error)
      if (!committed) throw new Error(`Could not reject task with error:\n${error}`)
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
