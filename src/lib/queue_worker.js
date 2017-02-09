'use strict';

var uuid = require('uuid');
var _ = require('lodash');

var MAX_TRANSACTION_ATTEMPTS = 10;
var DEFAULT_ERROR_STATE = 'error';
var DEFAULT_RETRIES = 0;

var SERVER_TIMESTAMP = {'.sv': 'timestamp'};

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

function QueueWorker(tasksRef, processIdBase, sanitize, suppressStack, processingFunction) {

  if (!tasksRef) throwError('No tasks reference provided.')
  if (typeof processIdBase !== 'string') throwError('Invalid process ID provided.')
  if (Boolean(sanitize) !== sanitize) throwError('Invalid sanitize option.')
  if (Boolean(suppressStack) !== suppressStack) throwError('Invalid suppressStack option.')
  if (typeof processingFunction !== 'function') throwError('No processing function provided.')

  const processId = processIdBase + ':' + uuid.v4()
  let shutdownDeferred = null

  const expiryTimeouts = {}
  const owners = {}

  let processingTasksRef = null
  let currentTaskRef = null // this can be removed as soon as we have converted the _tryToProcess unit tests
  let newTaskRef = null

  let stopWatchingOwnerAndReset = null
  let newTaskListener = null
  let processingTaskAddedListener = null
  let processingTaskRemovedListener = null

  let busy = false
  let taskNumber = 0
  let errorState = DEFAULT_ERROR_STATE;

  let taskTimeout = null
  let inProgressState = null
  let finishedState = null
  let taskRetries = null
  let startState = null


  this.setTaskSpec = setTaskSpec
  this.shutdown = shutdown

  // we can not remove usage of `self` here because
  // the tests either override or spy on these methods 
  const self = this
  this._resetTask = _resetTask
  this._tryToProcess = _tryToProcess
  this._setUpTimeouts = _setUpTimeouts

  // used in tests
  this._resolve = _resolve
  this._updateProgress = _updateProgress
  this._reject = _reject
  this._processId = processId
  this._expiryTimeouts = expiryTimeouts
  this._processingTasksRef = () => processingTasksRef
  this._currentTaskRef = () => currentTaskRef
  this._newTaskRef = (val) => val ? (newTaskRef = val, undefined) : newTaskRef
  this._busy = (val) => val ? (busy = val, undefined) : busy
  this._suppressStack = (val) => val ? (suppressStack = val, undefined) : suppressStack
  this._newTaskListener = () => newTaskListener
  this._processingTaskAddedListener = () => processingTaskAddedListener
  this._processingTaskRemovedListener = () => processingTaskRemovedListener
  this._taskNumber = (val) => val ? (taskNumber = val, undefined) : taskNumber
  this._taskTimeout = () => taskTimeout
  this._inProgressState = (val) => val ? (inProgressState = val, undefined) : inProgressState
  this._finishedState = (val) => val ? (finishedState = val, undefined) : finishedState
  this._taskRetries = (val) => { taskRetries = val }
  this._startState = (val) => val !== undefined ? (startState = val, undefined) : startState
  this._currentId = currentId

  return this

  function currentId() { return processId + ':' + taskNumber }
  function inProgress({ _state }) { return _state === inProgressState }
  function isOwner({ _owner }) { return _owner === currentId() }

  function isInvalidTask(requestedTaskNumber) {
    const notCurrentTask = taskNumber !== requestedTaskNumber

    return notCurrentTask
  }

  /**
   * Returns the state of a task to the start state.
   * @param {firebase.database.Reference} taskRef Firebase Realtime Database
   *   reference to the Firebase location of the task that's timed out.
   * @param {Boolean} forceReset Whether this is an immediate update to a task we
   *   expect this worker to own, or whether it's a timeout reset that we don't
   *   necessarily expect this worker to own.
   * @returns {Promise} Whether the task was able to be reset.
   */
  function _resetTask(taskRef, forceReset, deferred = createDeferred()) {
    const retries = 0;

    taskRef
      .transaction(
        task => {
          /* istanbul ignore if */
          if (task === null) return task

          const timeSinceUpdate = /* use offset */ Date.now() - task._state_changed || 0
          const timedOut = (taskTimeout && timeSinceUpdate >= taskTimeout)
          
          const allowReset = (forceReset && isOwner(task)) || timedOut

          if (inProgress(task) && allowReset) {
            task._state = startState
            task._state_changed = SERVER_TIMESTAMP
            task._owner = null
            task._progress = null
            task._error_details = null
            return task
          }
        }, 
        undefined,
        false
      )
      .then(_ => { deferred.resolve() })
      .catch(_ => {
        // reset task errored, retrying
        if ((retries + 1) < MAX_TRANSACTION_ATTEMPTS) setImmediate(() => _resetTask(taskRef, forceReset, deferred))
        else deferred.reject(new Error('reset task errored too many times, no longer retrying'))
      })

    return deferred.promise
  }

  /**
   * Creates a resolve callback function, storing the current task number.
   * @param {Number} taskNumber the current task number
   * @returns {Function} the resolve callback function.
   */
  function _resolve(taskRef, requestedTaskNumber) {
    let retries = 0
    const deferred = createDeferred()

    return [resolve, deferred.promise]

    /*
     * Resolves the current task and changes the state to the finished state.
     * @param {Object} newTask The new data to be stored at the location.
     * @returns {Promise} Whether the task was able to be resolved.
     */
    function resolve(newTask) {

      if (isInvalidTask(requestedTaskNumber)) deferred.resolve()
      else {
        taskRef
          .transaction(
            task => {
              if (task === null) return task

              if (inProgress(task) && isOwner(task)) {
                let outputTask = _.clone(newTask)
                if (!_.isPlainObject(outputTask)) outputTask = {}

                const newState = outputTask._new_state
                delete outputTask._new_state

                const invalidNewState = newState !== null && typeof newState !== 'string'
                const shouldRemove = (invalidNewState && finishedState === null) || newState === false

                if (shouldRemove) return null

                outputTask._state = invalidNewState ? finishedState : newState
                outputTask._state_changed = SERVER_TIMESTAMP
                outputTask._owner = null
                outputTask._progress = 100
                outputTask._error_details = null
                return outputTask
              }
            }, 
            undefined,
            false
          )
          .then(_ => { deferred.resolve() })
          .catch(_ => {
            // resolve task errored, retrying
            if (++retries < MAX_TRANSACTION_ATTEMPTS) setImmediate(resolve, newTask)
            else deferred.reject(new Error('resolve task errored too many times, no longer retrying'))
          })
      }

      return deferred.promise
    }
  }

  /**
   * Creates a reject callback function, storing the current task number.
   * @param {Number} taskNumber the current task number
   * @returns {Function} the reject callback function.
   */
  function _reject(taskRef, requestedTaskNumber) {
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
      if (isInvalidTask(requestedTaskNumber)) deferred.resolve()
      else {
        const errorString = 
          _.isError(error) ? error.message
          : typeof error === 'string' ? error
          : error !== undefined && error !== null ? error.toString()
          : null

        const errorStack = (!suppressStack && error && error.stack) || null

        taskRef
          .transaction(
            task => {
              if (task === null) return task

              if (inProgress(task) && isOwner(task)) {

                const { 
                  attempts: previousAttempts = 0, 
                  previous_state: previousState
                } = task._error_details || {}

                const attempts = previousState === inProgressState
                  ? previousAttempts + 1
                  : 1

                task._state = attempts > taskRetries ? errorState : startState
                task._state_changed = SERVER_TIMESTAMP;
                task._owner = null;
                task._error_details = {
                  previous_state: inProgressState,
                  error: errorString,
                  error_stack: errorStack,
                  attempts
                }
                return task
              }
            }, 
            undefined,
            false
          )
          .then(_ => { deferred.resolve() })
          .catch(_ => {
            // reject task errored, retrying
            if (++retries < MAX_TRANSACTION_ATTEMPTS) setImmediate(reject, error)
            else deferred.reject(new Error('reject task errored too many times, no longer retrying'))
          })
      }

      return deferred.promise
    }
  }

  /**
   * Creates an update callback function, storing the current task number.
   * @param {Number} taskNumber the current task number
   * @returns {Function} the update callback function.
   */
  function _updateProgress(taskRef, requestedTaskNumber) {

    return updateProgress

    /**
     * Updates the progress state of the task.
     * @param {Number} progress The progress to report.
     * @returns {Promise} Whether the progress was updated.
     */
    function updateProgress(progress) {
      if (typeof progress !== 'number' || _.isNaN(progress) || progress < 0 || progress > 100) 
        return Promise.reject(new Error('Invalid progress'))

      if (isInvalidTask(requestedTaskNumber)) return Promise.reject(new Error('Can\'t update progress - no task currently being processed'))

      return new Promise((resolve, reject) => {
        taskRef.transaction(
          task => {
            /* istanbul ignore if */
            if (task === null) return task
            if (inProgress(task) && isOwner(task)) {
              task._progress = progress
              return task
            }
          },
          undefined, 
          false
        )
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
  function _tryToProcess(deferred = createDeferred()) {
    let retries = 0

    if (busy) deferred.resolve()
    else {
      if (shutdownDeferred) {
        deferred.reject(new Error('Shutting down - can no longer process new tasks'))
        setTaskSpec(null);
        // finished shutdown
        shutdownDeferred.resolve()
      } else {
        if (!newTaskRef) deferred.resolve()
        else newTaskRef.once('value')
          .then(taskSnap => {
            if (!taskSnap.exists()) return deferred.resolve() //<-- this is problematic
            
            let nextTaskRef = null
            taskSnap.forEach(childSnap => { nextTaskRef = childSnap.ref })
            return nextTaskRef.transaction(
              task => {
                /* istanbul ignore if */
                if (task === null) return task

                if (!_.isPlainObject(task)) {
                  const error = new Error('Task was malformed')
                  const errorStack = suppressStack ? null : error.stack
                  
                  return {
                    _state: errorState,
                    _state_changed: SERVER_TIMESTAMP,
                    _error_details: {
                      error: error.message,
                      original_task: task,
                      error_stack: errorStack
                    }
                  }
                }
                if ((task._state || null) === startState) {
                  task._state = inProgressState;
                  task._state_changed = SERVER_TIMESTAMP;
                  task._owner = processId + ':' + (taskNumber + 1);
                  task._progress = 0;
                  return task;
                }
              },
              undefined,
              false
            )
          })
          .then(result => {
            if (!result) return
            const { committed, snapshot } = result
            if (committed && snapshot.exists() && snapshot.child('_state').val() !== errorState) {
              // Worker has become busy while the transaction was processing
              // so give up the task for now so another worker can claim it
              /* istanbul ignore if */
              if (busy) _resetTask(snapshot.ref, true)
              else {
                busy = true
                taskNumber += 1

                /* const */ currentTaskRef = snapshot.ref

                startWatchingOwner(currentTaskRef)

                const data = snapshot.val()
                if (sanitize) {
                  [
                    '_state',
                    '_state_changed',
                    '_owner',
                    '_progress',
                    '_error_details'
                  ].forEach(reserved => { delete data[reserved] });
                } else { data._id = snapshot.key } // this should be independent of `sanitize` and behind the flag `includeKey` or similar

                const progress = _updateProgress(currentTaskRef, taskNumber);
                const [resolve, resolvePromise] = _resolve(currentTaskRef, taskNumber);
                const [reject, rejectPromise] = _reject(currentTaskRef, taskNumber);

                Promise.race([resolvePromise, rejectPromise])
                  .then(_ => {
                    busy = false
                    _tryToProcess()
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
            }
            return deferred.resolve()
          })
          .catch(_ => {
            // errored while attempting to claim a new task, retrying
            if (++retries < MAX_TRANSACTION_ATTEMPTS) return setImmediate(_tryToProcess, deferred)
            else return deferred.reject(new Error('errored while attempting to claim a new task too many times, no longer retrying'))
          })
      }
    }

    return deferred.promise
  }

  function startWatchingOwner(taskRef) {
    const ownerRef = taskRef.child('_owner')
    const ownerChanged = ownerRef.on('value', snapshot => {
      /* istanbul ignore else */
      if (snapshot.val() !== currentId()) {
        ownerRef.off('value', ownerChanged)
        stopWatchingOwnerAndReset = null
        currentTaskRef = null
        // should this also reset? original implementation did not, might be a bug
      }
    })

    stopWatchingOwnerAndReset = () => {
      // This function should probably be named 'stopProcessing', problem is: we're not doing that.
      // This also highlights a problematic scenario. When we are already processing using the 
      // `processingFunction` which we can not cancel. So when we reset here, the `processingFunction`
      // will most likely process the task a second time
      stopWatchingOwnerAndReset = null
      currentTaskRef = null
      ownerRef.off('value', ownerChanged)
      _resetTask(taskRef, true)
    }
  } 

  /**
   * Sets up timeouts to reclaim tasks that fail due to taking too long.
   */
  function _setUpTimeouts() {
    if (processingTaskAddedListener !== null) {
      processingTasksRef.off('child_added', processingTaskAddedListener)
      processingTaskAddedListener = null
    }
    if (processingTaskRemovedListener !== null) {
      processingTasksRef.off('child_removed', processingTaskRemovedListener)
      processingTaskRemovedListener = null
    }

    Object.keys(expiryTimeouts).forEach(key => { 
      clearTimeout(expiryTimeouts[key])
      delete expiryTimeouts[key]
    })
    Object.keys(owners).forEach(key => { delete owners[key] })

    if (!taskTimeout) processingTasksRef = null
    else {
      processingTasksRef = tasksRef.orderByChild('_state').equalTo(inProgressState);

      processingTaskAddedListener = processingTasksRef.on('child_added', setUpTimeout)
      processingTaskRemovedListener = processingTasksRef.on('child_removed', ({ key }) => {
        clearTimeout(expiryTimeouts[key])
        delete expiryTimeouts[key]
        delete owners[key]
      })
      // possible problem, this listener is never removed:
      processingTasksRef.on('child_changed', snapshot => {
        // This catches de-duped events from the server - if the task was removed
        // and added in quick succession, the server may squash them into a
        // single update
        if (snapshot.child('_owner').val() !== owners[snapshot.key]) 
          setUpTimeout(snapshot)
      })
    }

    function setUpTimeout(snapshot) {
      var taskName = snapshot.key
      var now = new Date().getTime() // use server offset
      var startTime = (snapshot.child('_state_changed').val() || now);
      var expires = Math.max(0, startTime - now + taskTimeout);
      owners[taskName] = snapshot.child('_owner').val();
      expiryTimeouts[taskName] = setTimeout(
        () => _resetTask(snapshot.ref, false),
        expires
      )
    }
  }

  /**
   * Sets up the listeners to claim tasks and reset them if they timeout. Called
   *   any time the task spec changes.
   * @param {Object} taskSpec The specification for the task.
   */
  function setTaskSpec(taskSpec) {
    // Increment the taskNumber so that a task being processed before the change
    // doesn't continue to use incorrect data
    taskNumber += 1

    if (newTaskListener !== null) newTaskRef.off('child_added', newTaskListener)

    if (stopWatchingOwnerAndReset !== null) stopWatchingOwnerAndReset()

    if (isValidTaskSpec(taskSpec)) {
      startState = taskSpec.startState || null
      inProgressState = taskSpec.inProgressState
      finishedState = taskSpec.finishedState || null
      errorState = taskSpec.errorState || DEFAULT_ERROR_STATE
      taskTimeout = taskSpec.timeout || null
      taskRetries = taskSpec.retries || DEFAULT_RETRIES

      newTaskRef = tasksRef.orderByChild('_state').equalTo(startState).limitToFirst(1)
      newTaskListener = newTaskRef.on('child_added', () => { self._tryToProcess() })
    } else {
      // invalid task spec, not listening for new tasks
      startState = null
      inProgressState = null
      finishedState = null
      errorState = DEFAULT_ERROR_STATE
      taskTimeout = null
      taskRetries = DEFAULT_RETRIES

      newTaskRef = null
      newTaskListener = null
    }

    self._setUpTimeouts()
  }

  function shutdown() {
    if (shutdownDeferred) return shutdownDeferred.promise

    // Set the global shutdown deferred promise, which signals we're shutting down
    shutdownDeferred = createDeferred()

    // We can report success immediately if we're not busy
    if (!busy) {
      setTaskSpec(null)
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
