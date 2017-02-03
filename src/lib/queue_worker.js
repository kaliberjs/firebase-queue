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
module.exports = function QueueWorker(tasksRef, processIdBase, sanitize, suppressStack, processingFunction) {

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
  let currentTaskRef = null
  let newTaskRef = null

  let currentTaskListener = null
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
  this._isValidTaskSpec = _isValidTaskSpec
  this._resolve = _resolve
  this._updateProgress = _updateProgress
  this._reject = _reject
  this._processId = processId
  this._expiryTimeouts = expiryTimeouts
  this._processingTasksRef = () => processingTasksRef
  this._currentTaskRef = (val) => val ? (currentTaskRef = val, undefined) : currentTaskRef
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

  return this

  /**
   * Returns the state of a task to the start state.
   * @param {firebase.database.Reference} taskRef Firebase Realtime Database
   *   reference to the Firebase location of the task that's timed out.
   * @param {Boolean} immediate Whether this is an immediate update to a task we
   *   expect this worker to own, or whether it's a timeout reset that we don't
   *   necessarily expect this worker to own.
   * @returns {Promise} Whether the task was able to be reset.
   */
  function _resetTask(taskRef, immediate, deferred) {
    var retries = 0;

    /* istanbul ignore else */
    if (_.isUndefined(deferred)) {
      deferred = createDeferred();
    }

    taskRef.transaction(function(task) {
      /* istanbul ignore if */
      if (_.isNull(task)) {
        return task;
      }
      var id = processId + ':' + taskNumber;
      var correctState = (task._state === inProgressState);
      var correctOwner = (task._owner === id || !immediate);
      var timeSinceUpdate = Date.now() - _.get(task, '_state_changed', 0);
      var timedOut = ((taskTimeout && timeSinceUpdate > taskTimeout) || immediate);
      if (correctState && correctOwner && timedOut) {
        task._state = startState;
        task._state_changed = SERVER_TIMESTAMP;
        task._owner = null;
        task._progress = null;
        task._error_details = null;
        return task;
      }
      return undefined;
    }, function(error, committed, snapshot) {
      /* istanbul ignore if */
      if (error) {
        if (++retries < MAX_TRANSACTION_ATTEMPTS) {
          // reset task errored, retrying
          setImmediate(self._resetTask.bind(self), taskRef, immediate, deferred);
        } else {
          deferred.reject(new Error('reset task errored too many times, no longer retrying'))
        }
      } else {
        if (committed && snapshot.exists()) {
          // 'reset ' + snapshot.key;
        }
        deferred.resolve();
      }
    }, false);

    return deferred.promise;
  };

  /**
   * Creates a resolve callback function, storing the current task number.
   * @param {Number} taskNumber the current task number
   * @returns {Function} the resolve callback function.
   */
  function _resolve(requestedTaskNumber) {
    var retries = 0;
    var deferred = createDeferred();

    /*
     * Resolves the current task and changes the state to the finished state.
     * @param {Object} newTask The new data to be stored at the location.
     * @returns {Promise} Whether the task was able to be resolved.
     */
    var resolve = function(newTask) {
      if ((taskNumber !== requestedTaskNumber) || _.isNull(currentTaskRef)) {
        if (_.isNull(currentTaskRef)) {
          // Can't resolve task - no task  currently being processed
        } else {
          // Can't resolve task - no longer processing current task
        }
        deferred.resolve();
        busy = false;
        self._tryToProcess();
      } else {
        var existedBefore;
        currentTaskRef.transaction(function(task) {
          existedBefore = true;
          if (_.isNull(task)) {
            existedBefore = false;
            return task;
          }
          var id = processId + ':' + taskNumber;
          if (task._state === inProgressState &&
              task._owner === id) {
            var outputTask = _.clone(newTask);
            if (!_.isPlainObject(outputTask)) {
              outputTask = {};
            }
            outputTask._state = _.get(outputTask, '_new_state');
            delete outputTask._new_state;
            if (!_.isNull(outputTask._state) && !_.isString(outputTask._state)) {
              if (_.isNull(finishedState) || outputTask._state === false) {
                // Remove the item if no `finished_state` set in the spec or
                // _new_state is explicitly set to `false`.
                return null;
              }
              outputTask._state = finishedState;
            }
            outputTask._state_changed = SERVER_TIMESTAMP;
            outputTask._owner = null;
            outputTask._progress = 100;
            outputTask._error_details = null;
            return outputTask;
          }
          return undefined;
        }, function(error, committed, snapshot) {
          /* istanbul ignore if */
          if (error) {
            if (++retries < MAX_TRANSACTION_ATTEMPTS) {
              // resolve task errored, retrying
              setImmediate(resolve, newTask);
            } else {
              deferred.reject(new Error('resolve task errored too many times, no longer retrying'));
            }
          } else {
            if (committed && existedBefore) {
              // 'completed ' + snapshot.key
            } else {
              // Can't resolve task - current task no longer owned by this process
            }
            deferred.resolve();
            busy = false;
            self._tryToProcess();
          }
        }, false);
      }

      return deferred.promise;
    };

    return resolve;
  };

  /**
   * Creates a reject callback function, storing the current task number.
   * @param {Number} taskNumber the current task number
   * @returns {Function} the reject callback function.
   */
  function _reject(requestedTaskNumber) {
    var retries = 0;
    var errorString = null;
    var errorStack = null;
    var deferred = createDeferred();

    /**
     * Rejects the current task and changes the state to errorState,
     * adding additional data to the '_error_details' sub key.
     * @param {Object} error The error message or object to be logged.
     * @returns {Promise} Whether the task was able to be rejected.
     */
    var reject = function(error) {
      if ((taskNumber !== requestedTaskNumber) || _.isNull(currentTaskRef)) {
        if (_.isNull(currentTaskRef)) {
          // Can't reject task - no task currently being processed
        } else {
          // Can't reject task - no longer processing current task
        }
        deferred.resolve();
        busy = false;
        self._tryToProcess();
      } else {
        if (_.isError(error)) {
          errorString = error.message;
        } else if (_.isString(error)) {
          errorString = error;
        } else if (!_.isUndefined(error) && !_.isNull(error)) {
          errorString = error.toString();
        }

        if (!suppressStack) {
          errorStack = _.get(error, 'stack', null);
        }

        var existedBefore;
        currentTaskRef.transaction(function(task) {
          existedBefore = true;
          if (_.isNull(task)) {
            existedBefore = false;
            return task;
          }
          var id = processId + ':' + taskNumber;
          if (task._state === inProgressState &&
              task._owner === id) {
            var attempts = 0;
            var currentAttempts = _.get(task, '_error_details.attempts', 0);
            var currentPrevState = _.get(task, '_error_details.previous_state');
            if (currentAttempts > 0 &&
                currentPrevState === inProgressState) {
              attempts = currentAttempts;
            }
            if (attempts >= taskRetries) {
              task._state = errorState;
            } else {
              task._state = startState;
            }
            task._state_changed = SERVER_TIMESTAMP;
            task._owner = null;
            task._error_details = {
              previous_state: inProgressState,
              error: errorString,
              error_stack: errorStack,
              attempts: attempts + 1
            };
            return task;
          }
          return undefined;
        }, function(transactionError, committed, snapshot) {
          /* istanbul ignore if */
          if (transactionError) {
            if (++retries < MAX_TRANSACTION_ATTEMPTS) {
              // reject task errored, retrying
              setImmediate(reject, error); // <-- shouldn't this be transactionError?
            } else {
              deferred.reject(new Error('reject task errored too many times, no longer retrying'));
            }
          } else {
            if (committed && existedBefore) {
              // 'errored while attempting to complete ' + snapshot.key
            } else {
              // Can't reject task - current task no longer owned by this process
            }
            deferred.resolve();
            busy = false;
            self._tryToProcess();
          }
        }, false);
      }
      return deferred.promise;
    };

    return reject;
  };

  /**
   * Creates an update callback function, storing the current task number.
   * @param {Number} taskNumber the current task number
   * @returns {Function} the update callback function.
   */
  function _updateProgress(requestedTaskNumber) {
    /**
     * Updates the progress state of the task.
     * @param {Number} progress The progress to report.
     * @returns {Promise} Whether the progress was updated.
     */
    var updateProgress = function(progress) {
      if (!_.isNumber(progress) ||
          _.isNaN(progress) ||
          progress < 0 ||
          progress > 100) {
        return Promise.reject(new Error('Invalid progress'));
      }
      if ((taskNumber !== requestedTaskNumber)  || _.isNull(currentTaskRef)) {
        return Promise.reject(new Error('Can\'t update progress - no task currently being processed'));
      }
      return new Promise(function(resolve, reject) {
        currentTaskRef.transaction(function(task) {
          /* istanbul ignore if */
          if (_.isNull(task)) {
            return task;
          }
          var id = processId + ':' + taskNumber;
          if (task._state === inProgressState &&
              task._owner === id) {
            task._progress = progress;
            return task;
          }
          return undefined;
        }, function(transactionError, committed, snapshot) {
          /* istanbul ignore if */
          if (transactionError) {
            return reject(new Error('errored while attempting to update progress'));
          }
          if (committed && snapshot.exists()) {
            return resolve();
          }
          return reject(new Error('Can\'t update progress - current task no longer owned by this process'));
        }, false);
      });
    };

    return updateProgress;
  };

  /**
   * Attempts to claim the next task in the queue.
   */
  function _tryToProcess(deferred) {
    var retries = 0;
    var malformed = false;

    /* istanbul ignore else */
    if (_.isUndefined(deferred)) {
      deferred = createDeferred();
    }

    if (!busy) {
      if (shutdownDeferred) {
        deferred.reject(new Error('Shutting down - can no longer process new ' +
          'tasks'));
        self.setTaskSpec(null);
        // finished shutdown
        shutdownDeferred.resolve()
      } else {
        if (!newTaskRef) {
          deferred.resolve();
        } else {
          newTaskRef.once('value', function(taskSnap) {
            if (!taskSnap.exists()) {
              return deferred.resolve();
            }
            var nextTaskRef;
            taskSnap.forEach(function(childSnap) {
              nextTaskRef = childSnap.ref;
            });
            return nextTaskRef.transaction(function(task) {
              /* istanbul ignore if */
              if (_.isNull(task)) {
                return task;
              }
              if (!_.isPlainObject(task)) {
                malformed = true;
                var error = new Error('Task was malformed');
                var errorStack = null;
                if (!suppressStack) {
                  errorStack = error.stack;
                }
                return {
                  _state: errorState,
                  _state_changed: SERVER_TIMESTAMP,
                  _error_details: {
                    error: error.message,
                    original_task: task,
                    error_stack: errorStack
                  }
                };
              }
              if (_.isUndefined(task._state)) {
                task._state = null;
              }
              if (task._state === startState) {
                task._state = inProgressState;
                task._state_changed = SERVER_TIMESTAMP;
                task._owner = processId + ':' + (taskNumber + 1);
                task._progress = 0;
                return task;
              }
              // task no longer in correct state: expected ' + startState + ', got ' + task._state
              return undefined;
            }, function(error, committed, snapshot) {
              /* istanbul ignore if */
              if (error) {
                if (++retries < MAX_TRANSACTION_ATTEMPTS) {
                  // errored while attempting to claim a new task, retrying
                  return setImmediate(self._tryToProcess.bind(self), deferred);
                }
                return deferred.reject(new Error('errored while attempting to claim a new task too many times, no longer retrying'));
              } else if (committed && snapshot.exists()) {
                if (malformed) {
                  // found malformed entry ' + snapshot.key
                } else {
                  /* istanbul ignore if */
                  if (busy) {
                    // Worker has become busy while the transaction was processing
                    // so give up the task for now so another worker can claim it
                    self._resetTask(nextTaskRef, true);
                  } else {
                    busy = true;
                    taskNumber += 1;
                    // 'claimed ' + snapshot.key
                    currentTaskRef = snapshot.ref;
                    currentTaskListener = currentTaskRef
                        .child('_owner').on('value', function(ownerSnapshot) {
                          var id = processId + ':' + taskNumber;
                          /* istanbul ignore else */
                          if (ownerSnapshot.val() !== id &&
                              !_.isNull(currentTaskRef) &&
                              !_.isNull(currentTaskListener)) {
                            currentTaskRef.child('_owner').off(
                              'value',
                              currentTaskListener);
                            currentTaskRef = null;
                            currentTaskListener = null;
                          }
                        });
                    var data = snapshot.val();
                    if (sanitize) {
                      [
                        '_state',
                        '_state_changed',
                        '_owner',
                        '_progress',
                        '_error_details'
                      ].forEach(function(reserved) {
                        if (snapshot.hasChild(reserved)) {
                          delete data[reserved];
                        }
                      });
                    } else {
                      data._id = snapshot.key;
                    }
                    var progress = _updateProgress(taskNumber);
                    var resolve = _resolve(taskNumber);
                    var reject = _reject(taskNumber);
                    setImmediate(function() {
                      try {
                        processingFunction.call(null, data, progress, resolve, reject);
                      } catch (err) {
                        reject(err);
                      }
                    });
                  }
                }
              }
              return deferred.resolve();
            }, false);
          });
        }
      }
    } else {
      deferred.resolve();
    }

    return deferred.promise;
  };

  /**
   * Sets up timeouts to reclaim tasks that fail due to taking too long.
   */
  function _setUpTimeouts() {
    if (!_.isNull(processingTaskAddedListener)) {
      processingTasksRef.off(
        'child_added',
        processingTaskAddedListener);
      processingTaskAddedListener = null;
    }
    if (!_.isNull(processingTaskRemovedListener)) {
      processingTasksRef.off(
        'child_removed',
        processingTaskRemovedListener);
      processingTaskRemovedListener = null;
    }

    Object.keys(expiryTimeouts).forEach(key => { 
      clearTimeout(expiryTimeouts[key])
      delete expiryTimeouts[key]
    })
    Object.keys(owners).forEach(key => { delete owners[key] })

    if (taskTimeout) {
      processingTasksRef = tasksRef.orderByChild('_state')
        .equalTo(inProgressState);

      var setUpTimeout = function(snapshot) {
        var taskName = snapshot.key;
        var now = new Date().getTime();
        var startTime = (snapshot.child('_state_changed').val() || now);
        var expires = Math.max(0, startTime - now + taskTimeout);
        var ref = snapshot.ref;
        owners[taskName] = snapshot.child('_owner').val();
        expiryTimeouts[taskName] = setTimeout(
          self._resetTask.bind(self),
          expires,
          ref, false);
      };

      processingTaskAddedListener = processingTasksRef.on('child_added',
        setUpTimeout,
        /* istanbul ignore next */ function(error) {
          // errored listening to Firebase
        });
      processingTaskRemovedListener = processingTasksRef.on(
        'child_removed',
        function(snapshot) {
          var taskName = snapshot.key;
          clearTimeout(expiryTimeouts[taskName]);
          delete expiryTimeouts[taskName];
          delete owners[taskName];
        }, /* istanbul ignore next */ function(error) {
          // errored listening to Firebase
        });
      processingTasksRef.on('child_changed', function(snapshot) {
        // This catches de-duped events from the server - if the task was removed
        // and added in quick succession, the server may squash them into a
        // single update
        var taskName = snapshot.key;
        if (snapshot.child('_owner').val() !== owners[taskName]) {
          setUpTimeout(snapshot);
        }
      }, /* istanbul ignore next */ function(error) {
        // errored listening to Firebase
      });
    } else {
      processingTasksRef = null;
    }
  };

  /**
   * Validates a task spec contains meaningful parameters.
   * @param {Object} taskSpec The specification for the task.
   * @returns {Boolean} Whether the taskSpec is valid.
   */
  function _isValidTaskSpec(taskSpec) {
    if (!_.isPlainObject(taskSpec)) {
      return false;
    }
    if (!_.isString(taskSpec.inProgressState)) {
      return false;
    }
    if (!_.isUndefined(taskSpec.startState) &&
        !_.isNull(taskSpec.startState) &&
        (
          !_.isString(taskSpec.startState) ||
          taskSpec.startState === taskSpec.inProgressState
        )) {
      return false;
    }
    if (!_.isUndefined(taskSpec.finishedState) &&
        !_.isNull(taskSpec.finishedState) &&
        (
          !_.isString(taskSpec.finishedState) ||
          taskSpec.finishedState === taskSpec.inProgressState ||
          taskSpec.finishedState === taskSpec.startState
        )) {
      return false;
    }
    if (!_.isUndefined(taskSpec.errorState) &&
        !_.isNull(taskSpec.errorState) &&
        (
          !_.isString(taskSpec.errorState) ||
          taskSpec.errorState === taskSpec.inProgressState
        )) {
      return false;
    }
    if (!_.isUndefined(taskSpec.timeout) &&
        !_.isNull(taskSpec.timeout) &&
        (
          !_.isNumber(taskSpec.timeout) ||
          taskSpec.timeout <= 0 ||
          taskSpec.timeout % 1 !== 0
        )) {
      return false;
    }
    if (!_.isUndefined(taskSpec.retries) &&
        !_.isNull(taskSpec.retries) &&
        (
          !_.isNumber(taskSpec.retries) ||
          taskSpec.retries < 0 ||
          taskSpec.retries % 1 !== 0
        )) {
      return false;
    }
    return true;
  };

  /**
   * Sets up the listeners to claim tasks and reset them if they timeout. Called
   *   any time the task spec changes.
   * @param {Object} taskSpec The specification for the task.
   */
  function setTaskSpec(taskSpec) {
    // Increment the taskNumber so that a task being processed before the change
    // doesn't continue to use incorrect data
    taskNumber += 1;

    if (!_.isNull(newTaskListener)) {
      newTaskRef.off('child_added', newTaskListener);
    }

    if (!_.isNull(currentTaskListener)) {
      currentTaskRef.child('_owner').off(
        'value',
        currentTaskListener);
      self._resetTask(currentTaskRef, true);
      currentTaskRef = null;
      currentTaskListener = null;
    }

    if (_isValidTaskSpec(taskSpec)) {
      startState = taskSpec.startState || null;
      inProgressState = taskSpec.inProgressState;
      finishedState = taskSpec.finishedState || null;
      errorState = taskSpec.errorState || DEFAULT_ERROR_STATE;
      taskTimeout = taskSpec.timeout || null;
      taskRetries = taskSpec.retries || DEFAULT_RETRIES;

      newTaskRef = tasksRef
                            .orderByChild('_state')
                            .equalTo(startState)
                            .limitToFirst(1);
      // listening
      newTaskListener = newTaskRef.on(
        'child_added',
        function() {
          self._tryToProcess();
        }, /* istanbul ignore next */ function(error) {
          // errored listening to Firebase
        });
    } else {
      // invalid task spec, not listening for new tasks
      startState = null;
      inProgressState = null;
      finishedState = null;
      errorState = DEFAULT_ERROR_STATE;
      taskTimeout = null;
      taskRetries = DEFAULT_RETRIES;

      newTaskRef = null;
      newTaskListener = null;
    }

    self._setUpTimeouts();
  };

  function shutdown() {
    if (shutdownDeferred) return shutdownDeferred.promise

    // shutting down

    // Set the global shutdown deferred promise, which signals we're shutting down
    shutdownDeferred = createDeferred()

    // We can report success immediately if we're not busy
    if (!busy) {
      self.setTaskSpec(null);
      // finished shutdown
      shutdownDeferred.resolve()
    }

    return shutdownDeferred.promise;
  };
}
