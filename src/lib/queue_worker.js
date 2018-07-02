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

    if (shutdownStarted) return finishShutdown()
    busy = true

    await claimAndProcess(ref).catch(reportError)

    busy = false
    if (shutdownStarted) return finishShutdown()

    setImmediate(waitForNextTask) // let node.js breathe
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
      .then(
        newTask => transactionHelper.resolveWith(ref, newTask),
        error   => transactionHelper.rejectWith (ref, error)
      )

    function removeQueueProperties(task) {
      const properties = ['_state', '_state_changed', '_owner', '_progress', '_error_details']
      properties.forEach(properties => { delete task[properties] })
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
    if (shutdownStarted) return shutdownStarted.promise

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
