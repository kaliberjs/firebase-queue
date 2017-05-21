'use strict';

const uuid = require('uuid')
const _ = require('lodash')
const DefaultTransactionHelper = require('./transaction_helper')
const TaskUtilities = require('./task_utilities')

const MAX_TRANSACTION_ATTEMPTS = 10
const DEFAULT_ERROR_STATE = 'error'
const DEFAULT_RETRIES = 0

const SERVER_TIMESTAMP = {'.sv': 'timestamp'}

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

/**
 * @param {firebase.database.Reference} tasksRef the Firebase Realtime Database
 *   reference for queue tasks.
 * @param {String} processId the ID of the current worker process.
 * @param {Function} processingFunction the function to be called each time a
 *   task is claimed.
 * @return {Object}
 */
module.exports = QueueWorker

QueueWorker.isValidTaskSpec = isValidTaskSpec

function QueueWorker({ tasksRef, processIdBase, sanitize, suppressStack, processingFunction, spec, TransactionHelper = DefaultTransactionHelper }) {

  if (!tasksRef) throwError('No tasks reference provided.')
  if (typeof processIdBase !== 'string') throwError('Invalid process ID provided.')
  if (Boolean(sanitize) !== sanitize) throwError('Invalid sanitize option.')
  if (Boolean(suppressStack) !== suppressStack) throwError('Invalid suppressStack option.')
  if (typeof processingFunction !== 'function') throwError('No processing function provided.')
  if (!isValidTaskSpec(spec)) throwError('Invalid task spec provided')

  const serverOffset = 0 // this should be passed in by the queue
  const processId = processIdBase + ':' + uuid.v4()

  let shutdownStarted = null

  const sanitizedSpec = getSanitizedTaskSpec(spec)
  const { startState, inProgressState, timeout } = sanitizedSpec
  const taskUtilities = new TaskUtilities({ serverOffset, spec: sanitizedSpec })
  let transactionHelper = new TransactionHelper({ serverOffset, processId, spec: sanitizedSpec })

  const newTaskRef = tasksRef.orderByChild('_state').equalTo(startState).limitToFirst(1)
  const hasTimeout = !!timeout

  let stop = null
  let busy = false

  this.shutdown = shutdown

  start()

  return this

  function _resetTaskIfTimedOut(taskRef) {
    transactionWithRetries({
      ref: taskRef,
      transaction: transactionHelper.resetIfTimedOut
    })
  }

  function _resolve(taskRef) {
    const deferred = createDeferred()

    return [resolve, deferred.promise]

    function resolve(newTask) {
      return transactionWithRetries({
        ref: taskRef, 
        transaction: transactionHelper.resolveWith(newTask),
        deferred
      })
    }
  }

  function _reject(taskRef) {
    const deferred = createDeferred()

    return [reject, deferred.promise]

    function reject(error) {
      const errorString =
        _.isError(error) ? error.message
        : typeof error === 'string' ? error
        : error !== undefined && error !== null ? error.toString()
        : null

      const errorStack = (!suppressStack && error && error.stack) || null

      return transactionWithRetries({
        ref: taskRef,
        transaction: transactionHelper.rejectWith(errorString, errorStack),
        deferred
      })
    }
  }

  function _updateProgress(taskRef) {

    return updateProgress

    function updateProgress(progress) {
      if (typeof progress !== 'number' || _.isNaN(progress) || progress < 0 || progress > 100) 
        return Promise.reject(new Error('Invalid progress'))

      return transactionWithRetries({
        ref: taskRef,
        transaction: transactionHelper.updateProgressWith(progress)
      }).then(({ committed, snapshot }) => committed && snapshot.exists()
          ? Promise.resolve()
          : Promise.reject(new Error('Can\'t update progress - current task no longer owned by this process or task no longer in progress'))
        )
    }
  }

  /**
   * Attempts to claim the next task in the queue.
   */
  function _tryToProcess(taskSnapshot) {
    let retries = 0

    if (shutdownStarted) {
      finishShutdown()
      return
    }

    const nextTransactionHelper = transactionHelper.cloneForNextTask()
    
    return transactionWithRetries({
      ref: taskSnapshot.ref,
      transaction: nextTransactionHelper.claim
    }).then(({ committed, snapshot }) => {
        if (committed && snapshot.exists() && !taskUtilities.isInErrorState(snapshot)) {
          busy = true
          transactionHelper = nextTransactionHelper

          const data = snapshot.val()
          if (sanitize) taskUtilities.sanitize(data)
          else { data._id = snapshot.key } // this should be independent of `sanitize` and behind the flag `includeKey` or similar

          const currentTaskRef = snapshot.ref
          const progress = _updateProgress(currentTaskRef)
          const [resolve, resolvePromise] = _resolve(currentTaskRef)
          const [reject, rejectPromise] = _reject(currentTaskRef)

          Promise.race([resolvePromise, rejectPromise])
            .then(_ => {
              busy = false
              if (shutdownStarted) finishShutdown()
              else newTaskRef.once('child_added', _tryToProcess)
            })
            .catch(_ => {
              // the original implementation did not handle this situation
              // we should probably set the error and free ourselves:
              // busy = false and _tryToProcess
            })

          setImmediate(() => {
            try { processingFunction.call(null, data, progress, resolve, reject) }
            catch (err) { reject(err) }
          })
        }
      })
      //.catch(e => { /* transaction failed */ })
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
      if (taskUtilities.getOwner(snapshot) !== expiryTimeouts[snapshot.key].owner) {
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
        owner: taskUtilities.getOwner(snapshot),
        timeout: setTimeout(
          () => _resetTaskIfTimedOut(snapshot.ref),
          taskUtilities.expiresIn(snapshot)
        )
      }
    }

    function removeTimeoutHandling({ key }) {
      clearTimeout(expiryTimeouts[key].timeout)
      delete expiryTimeouts[key]
    }
  }

  function start() {
    newTaskRef.once('child_added', _tryToProcess)
    const removeTimeouts = hasTimeout && _setUpTimeouts()
    stop = () => {
      stop = null
      newTaskRef.off('child_added', _tryToProcess)
      if (removeTimeouts) removeTimeouts()
    }
  }

  function getSanitizedTaskSpec({
    startState = null,
    inProgressState = null,
    finishedState = null,
    errorState = DEFAULT_ERROR_STATE,
    timeout = null,
    retries = DEFAULT_RETRIES
  }) { return { startState, inProgressState, finishedState, errorState, timeout, retries } }

  function shutdown() {
    if (shutdownStarted) return shutdownStarted.promise

    // Set the global shutdown deferred promise, which signals we're shutting down
    shutdownStarted = createDeferred()

    // We can report success immediately if we're not busy
    if (!busy) finishShutdown()

    return shutdownStarted.promise
  }

  function finishShutdown() {
    if (stop) stop()
    // finished shutdown
    shutdownStarted.resolve()
  }
}

function transactionWithRetries({ ref, transaction, deferred = createDeferred(), attempts = 0 }) {
  ref.transaction(transaction, undefined, false)
    .then(x => { deferred.resolve(x) })
    .catch(e => {
      if (attempts < MAX_TRANSACTION_ATTEMPTS) {
        setImmediate(() => transactionWithRetries({ ref, transaction, deferred, attempts: attempts + 1 }))
      } else deferred.reject(new Error('transaction failed too many times, no longer retrying, original error: ' + e.message))
    })

  return deferred.promise
}

/**
 * Validates a task spec contains meaningful parameters.
 * @param {Object} taskSpec The specification for the task.
 * @returns {Boolean} Whether the taskSpec is valid.
 */
function isValidTaskSpec(taskSpec) {
  if (!_.isPlainObject(taskSpec)) return false

  const {
    inProgressState, startState, finishedState, errorState,
    timeout, retries
  } = taskSpec

  return (
    typeof inProgressState === 'string' &&
    check(startState   , undefinedOrNull, stringAndNot(inProgressState)) &&
    check(finishedState, undefinedOrNull, stringAndNot(inProgressState, startState)) &&
    check(errorState   , undefinedOrNull, stringAndNot(inProgressState)) &&
    check(timeout, undefinedOrNull, positiveInteger({ min: 1 })) &&
    check(retries, undefinedOrNull, positiveInteger({ min: 0 }))
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