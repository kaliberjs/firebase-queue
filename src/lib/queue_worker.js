'use strict';

const uuid = require('uuid')
const TransactionHelper = require('./transaction_helper')

function throwError(message) { throw new Error(message) }

function createDeferred() {
  let resolve = null
  let reject = null
  return {
    resolve: (...args) => resolve(...args),
    reject: (...args) => reject(...args),
    promise: new Promise((res, rej) => {
      resolve = res
      reject = rej
    })
  }
}

module.exports = QueueWorker

function QueueWorker({ tasksRef, serverOffset, sanitize, processingFunction, spec }) {

  if (!tasksRef) throwError('No tasks reference provided.')
  if (Boolean(sanitize) !== sanitize) throwError('Invalid sanitize option.')
  if (typeof processingFunction !== 'function') throwError('No processing function provided.')
  if (!isValidTaskSpec(spec)) throwError('Invalid task spec provided')

  const processId = uuid.v4()

  const { startState, inProgressState, errorState, timeout } = spec
  const newTaskRef = tasksRef.orderByChild('_state').equalTo(startState).limitToFirst(1)

  let transactionHelper = new TransactionHelper({ serverOffset, processId, spec })
  let shutdownStarted = null
  let busy = false

  this.shutdown = shutdown

  const stop = start()

  function start() {
    const hasTimeout = !!timeout
    waitForNextTask()
    const removeTimeouts = hasTimeout && _setUpTimeouts()
    return () => {
      stopWaitingForNextTask()
      if (removeTimeouts) removeTimeouts()
    }
  }

  function waitForNextTask() {
    newTaskRef.on('child_added', tryToProcessAndCatchError/*, report error */) // should not be once because that can not be cancelled
  }

  function stopWaitingForNextTask() {
    newTaskRef.off('child_added', tryToProcessAndCatchError)
  }

  async function tryToProcessAndCatchError({ ref }) {
    stopWaitingForNextTask()

    if (shutdownStarted) return finishShutdown()
    busy = true

    await claimAndProcess(ref).catch(_ => { /* report error */})

    busy = false
    if (shutdownStarted) return finishShutdown()

    waitForNextTask()
  }

  async function claimAndProcess(ref) {
    const nextTransactionHelper = transactionHelper.cloneForNextTask()
    const { committed, snapshot } = await nextTransactionHelper.claim(ref)
    if (committed && snapshot.exists() && !isInErrorState(snapshot)) {
      transactionHelper = nextTransactionHelper
      await process(snapshot)
    }

    function isInErrorState(x) { return x.child('_state').val() === errorState }
  }

  async function process(snapshot) {
    const { ref } = snapshot

    const data = snapshot.val()
    if (sanitize) sanitize(data)

    await processingFunction(data, { ref, setProgress })
      .then(
        newTask  => transactionHelper.resolveWith(ref, newTask),
        error    => transactionHelper.rejectWith (ref, error)
      )

    function sanitize(task) {
      const fields = ['_state', '_state_changed', '_owner', '_progress', '_error_details']
      fields.forEach(field => { delete task[field] })
      return task
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

  /**
   * Sets up timeouts to reclaim tasks that fail due to taking too long.
   */
  function _setUpTimeouts() {
    const expiryTimeouts = {}

    const ref = tasksRef.orderByChild('_state').equalTo(inProgressState)

    const onChildAdded = ref.on('child_added', addTimeoutHandling)
    const onChildRemoved = ref.on('child_removed', removeTimeoutHandling)
    const onChildChanged = ref.on('child_changed', snapshot => {
      // This catches de-duped events from the server - if the task was removed
      // and added in quick succession, the server may squash them into a
      // single update
      if (getOwner(snapshot) !== expiryTimeouts[snapshot.key].owner) {
        removeTimeoutHandling(snapshot)
        addTimeoutHandling(snapshot)
      }
    })

    return () => {
      ref.off('child_added', onChildAdded)
      ref.off('child_removed', onChildRemoved)
      ref.off('child_changed', onChildChanged)

      Object.keys(expiryTimeouts).forEach(key => { removeTimeoutHandling({ key }) })
    }

    function addTimeoutHandling(snapshot) {
      expiryTimeouts[snapshot.key] = {
        owner: getOwner(snapshot),
        timeout: setTimeout(
          () => transactionHelper.resetIfTimedOut(snapshot.ref),
          expiresIn(snapshot)
        )
      }
    }

    function removeTimeoutHandling({ key }) {
      clearTimeout(expiryTimeouts[key].timeout)
      delete expiryTimeouts[key]
    }

    function getOwner(snapshot) {
      return snapshot.child('_owner').val()
    }

    function expiresIn(snapshot) {
      const now = Date.now() + serverOffset
      const startTime = (snapshot.child('_state_changed').val() || now)
      return Math.max(0, startTime - now + timeout)
    }
  }

  async function shutdown() {
    if (shutdownStarted) return shutdownStarted.promise

    // Set the global shutdown deferred promise, which signals we're shutting down
    shutdownStarted = createDeferred()

    // We can report success immediately if we're not busy
    if (!busy) finishShutdown()

    return shutdownStarted.promise
  }

  function finishShutdown() {
    stop()
    // finished shutdown
    shutdownStarted.resolve()
  }
}

function isValidTaskSpec(taskSpec) {
  const { inProgressState, startState, finishedState, errorState, timeout } = taskSpec

  return (
    typeof inProgressState === 'string' &&
    check(startState   , undefinedOrNull, stringAndNot(inProgressState)) &&
    check(finishedState, undefinedOrNull, stringAndNot(inProgressState, startState)) &&
    check(errorState   , undefinedOrNull, stringAndNot(inProgressState)) &&
    check(timeout, undefinedOrNull, positiveInteger({ min: 1 }))
  )

  function check(val, ...checks) {
    return checks.reduce((result, check) => result || check(val), false)
  }
  function undefinedOrNull(val) { return val === undefined || val === null }
  function positiveInteger({ min }) {
    return val => typeof val === 'number' && val >= min && val % 1 === 0
  }
  function stringAndNot(...vals) {
    return val => typeof val === 'string' &&
      vals.reduce((result, other) => result && val !== other, true)
  }
}