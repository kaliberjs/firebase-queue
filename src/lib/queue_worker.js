'use strict';

const uuid = require('uuid')
const _ = require('lodash')
const DefaultTaskWorker = require('./task_worker')

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

function QueueWorker({ tasksRef, processIdBase, sanitize, suppressStack, processingFunction, spec, TaskWorker = DefaultTaskWorker }) {

  if (!tasksRef) throwError('No tasks reference provided.')
  if (typeof processIdBase !== 'string') throwError('Invalid process ID provided.')
  if (Boolean(sanitize) !== sanitize) throwError('Invalid sanitize option.')
  if (Boolean(suppressStack) !== suppressStack) throwError('Invalid suppressStack option.')
  if (typeof processingFunction !== 'function') throwError('No processing function provided.')
  if (!isValidTaskSpec(spec)) throwError('Invalid task spec provided')

  const processId = processIdBase + ':' + uuid.v4()

  let shutdownDeferred = null
  let taskWorker = new TaskWorker({ serverOffset: 0, processId, spec: getSanitizedTaskSpec(spec) })
  // we should probably move the non-owner related stuff away from the TaskWorker
  // once we fixed some issues we can move the creation of newTaskRef to the constructor
  const newTaskRef = taskWorker.getNextFrom(tasksRef)

  let stop = null

  let busy = false

  this.start = start
  this.shutdown = shutdown

  // used in tests
  this._tryToProcess = _tryToProcess
  this._setUpTimeouts = _setUpTimeouts
  this._resetTaskIfTimedOut = _resetTaskIfTimedOut
  this._resolve = _resolve
  this._updateProgress = _updateProgress
  this._reject = _reject
  this._processId = processId
  this._busy = () => busy


  return this

  /**
   * Returns the state of a task to the start state if timedout.
   * @param {firebase.database.Reference} taskRef Firebase Realtime Database
   *   reference to the Firebase location of the task that's timed out.
   * @returns {Promise} Whether the task was able to be reset.
   */
  function _resetTaskIfTimedOut(taskRef, deferred = createDeferred()) {
    const retries = 0;

    taskRef
      .transaction(taskWorker.resetIfTimedOut, undefined, false)
      .then(_ => { deferred.resolve() })
      .catch(_ => {
        // reset task errored, retrying
        if ((retries + 1) < MAX_TRANSACTION_ATTEMPTS) setImmediate(() => _resetTaskIfTimedOut(taskRef, deferred))
        else deferred.reject(new Error('reset task errored too many times, no longer retrying'))
      })

    return deferred.promise
  }

  /**
   * Creates a resolve callback function, storing the current task number.
   * @returns {Function} the resolve callback function.
   */
  function _resolve(taskRef) {
    let retries = 0
    const deferred = createDeferred()

    return [resolve, deferred.promise]

    /*
     * Resolves the current task and changes the state to the finished state.
     * @param {Object} newTask The new data to be stored at the location.
     * @returns {Promise} Whether the task was able to be resolved.
     */
    function resolve(newTask) {

      taskRef
        .transaction(taskWorker.resolveWith(newTask), undefined, false)
        .then(_ => { deferred.resolve() })
        .catch(_ => {
          // resolve task errored, retrying
          if (++retries < MAX_TRANSACTION_ATTEMPTS) setImmediate(resolve, newTask)
          else deferred.reject(new Error('resolve task errored too many times, no longer retrying'))
        })

      return deferred.promise
    }
  }

  /**
   * Creates a reject callback function, storing the current task number.
   * @returns {Function} the reject callback function.
   */
  function _reject(taskRef) {
    let retries = 0
    const deferred = createDeferred()

    return [reject, deferred.promise]

    /**
     * Rejects the current task and changes the state to errorState,
     * adding additional data to the '_error_details' sub key.
     * @param {Object} error The error message or object to be logged.
     * @returns {Promise} Whether the task was able to be rejected.
     */
    function reject(error) {
      const errorString =
        _.isError(error) ? error.message
        : typeof error === 'string' ? error
        : error !== undefined && error !== null ? error.toString()
        : null

      const errorStack = (!suppressStack && error && error.stack) || null

      taskRef
        .transaction(taskWorker.rejectWith(errorString, errorStack), undefined, false)
        .then(_ => { deferred.resolve() })
        .catch(_ => {
          // reject task errored, retrying
          if (++retries < MAX_TRANSACTION_ATTEMPTS) setImmediate(reject, error)
          else deferred.reject(new Error('reject task errored too many times, no longer retrying'))
        })

      return deferred.promise
    }
  }

  /**
   * Creates an update callback function, storing the current task number.
   * @returns {Function} the update callback function.
   */
  function _updateProgress(taskRef) {

    return updateProgress

    /**
     * Updates the progress state of the task.
     * @param {Number} progress The progress to report.
     * @returns {Promise} Whether the progress was updated.
     */
    function updateProgress(progress) {
      if (typeof progress !== 'number' || _.isNaN(progress) || progress < 0 || progress > 100) 
        return Promise.reject(new Error('Invalid progress'))

      return new Promise((resolve, reject) => {
        taskRef.transaction(taskWorker.updateProgressWith(progress), undefined, false)
        .then(({ committed, snapshot }) => { 
          if (committed && snapshot.exists()) resolve()
          else reject(new Error('Can\'t update progress - current task no longer owned by this process'))
        })
        .catch(_ => { reject(new Error('errored while attempting to update progress')) })
      })
    }
  }

  /**
   * Attempts to claim the next task in the queue.
   */
  function _tryToProcess(taskSnapshot, deferred = createDeferred()) {
    let retries = 0

    if (shutdownDeferred) {
      deferred.reject(new Error('Shutting down - can no longer process new tasks'))
      if (stop) stop()
      // finished shutdown
      shutdownDeferred.resolve()
    } else {
      taskSnapshot.ref.transaction(taskWorker/* cloneForNextTask().claim */.claimFor(() => taskWorker.nextOwner), undefined, false)
        .then(({ committed, snapshot }) => {
          if (committed && snapshot.exists() && !taskWorker.isInErrorState(snapshot)) {
            // Worker has become busy while the transaction was processing
            // so give up the task for now so another worker can claim it
            /* istanbul ignore if */
            busy = true
            taskWorker = taskWorker.cloneForNextTask()

            const currentTaskRef = snapshot.ref

            const data = snapshot.val()
            if (sanitize) taskWorker.sanitize(data)
            else { data._id = snapshot.key } // this should be independent of `sanitize` and behind the flag `includeKey` or similar

            const progress = _updateProgress(currentTaskRef)
            const [resolve, resolvePromise] = _resolve(currentTaskRef)
            const [reject, rejectPromise] = _reject(currentTaskRef)

            Promise.race([resolvePromise, rejectPromise])
              .then(_ => {
                busy = false
                if (shutdownDeferred) {
                  deferred.reject(new Error('Shutting down - can no longer process new tasks'))
                  if (stop) stop()
                  // finished shutdown
                  shutdownDeferred.resolve()
                } else {
                  newTaskRef.once('child_added', _tryToProcess)
                }
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
          return deferred.resolve()
        })
        .catch(_ => {
          // errored while attempting to claim a new task, retrying
          if (++retries < MAX_TRANSACTION_ATTEMPTS) return setImmediate(_tryToProcess, taskSnapshot, deferred)
          else return deferred.reject(new Error('errored while attempting to claim a new task too many times, no longer retrying'))
        })
    }

    return deferred.promise
  }

  /**
   * Sets up timeouts to reclaim tasks that fail due to taking too long.
   */
  function _setUpTimeouts() {
    const expiryTimeouts = {}
    const owners = {}

    const ref = taskWorker.getInProgressFrom(tasksRef)

    const onChildAdded = ref.on('child_added', setUpTimeout)
    const onChildRemoved = ref.on('child_removed', ({ key }) => {
      clearTimeout(expiryTimeouts[key])
      delete expiryTimeouts[key]
      delete owners[key]
    })
    const onChildChanged = ref.on('child_changed', snapshot => {
      // This catches de-duped events from the server - if the task was removed
      // and added in quick succession, the server may squash them into a
      // single update
      if (taskWorker.getOwner(snapshot) !== owners[snapshot.key])
        setUpTimeout(snapshot)
    })

    return () => {
      ref.off('child_added', onChildAdded)
      ref.off('child_removed', onChildRemoved)
      ref.off('child_changed', onChildChanged)

      Object.keys(expiryTimeouts).forEach(key => {
        clearTimeout(expiryTimeouts[key])
      })
    }

    function setUpTimeout(snapshot) {
      const taskName = snapshot.key
      const expires = taskWorker.expiresIn(snapshot)
      owners[taskName] = taskWorker.getOwner(snapshot)
      expiryTimeouts[taskName] = setTimeout(
        () => _resetTaskIfTimedOut(snapshot.ref),
        expires
      )
    }
  }

  function start() {
    newTaskRef.once('child_added', _tryToProcess)
    const removeTimeouts = taskWorker.hasTimeout() ? _setUpTimeouts() : null
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
  } = {}) { return { startState, inProgressState, finishedState, errorState, timeout, retries } }

  function shutdown() {
    if (shutdownDeferred) return shutdownDeferred.promise

    // Set the global shutdown deferred promise, which signals we're shutting down
    shutdownDeferred = createDeferred()

    // We can report success immediately if we're not busy
    if (!busy) {
      if (stop) stop()
      // finished shutdown
      shutdownDeferred.resolve()
    }

    return shutdownDeferred.promise
  }
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
