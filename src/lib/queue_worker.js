'use strict';

var logger = require('winston');
var uuid = require('uuid');
var _ = require('lodash');

var MAX_TRANSACTION_ATTEMPTS = 10;
var DEFAULT_ERROR_STATE = 'error';
var DEFAULT_RETRIES = 0;

var SERVER_TIMESTAMP = {'.sv': 'timestamp'};

function _getKey(snapshot) {
  return _.isFunction(snapshot.key) ? snapshot.key() : snapshot.key;
}

function _getRef(snapshot) {
  return _.isFunction(snapshot.ref) ? snapshot.ref() : snapshot.ref;
}

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

  this._getLogEntry = _getLogEntry
  this._resetTask = _resetTask
  this._resolve = _resolve
  this._reject = _reject
  this._updateProgress = _updateProgress
  this._tryToProcess = _tryToProcess
  this._setUpTimeouts = _setUpTimeouts
  this._isValidTaskSpec = _isValidTaskSpec
  this.setTaskSpec = setTaskSpec
  this.shutdown = shutdown

  // used in tests
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

  return this


  /**
   * Logs an info message with a worker-specific prefix.
   * @param {String} message The message to log.
   */
  function _getLogEntry(message) {
    return 'QueueWorker ' + processId + ' ' + message;
  };

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
    var self = this;
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
      var correctState = (task._state === self.inProgressState);
      var correctOwner = (task._owner === id || !immediate);
      var timeSinceUpdate = Date.now() - _.get(task, '_state_changed', 0);
      var timedOut = ((self.taskTimeout && timeSinceUpdate > self.taskTimeout) || immediate);
      if (correctState && correctOwner && timedOut) {
        task._state = self.startState;
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
          logger.debug(self._getLogEntry('reset task errored, retrying'), error);
          setImmediate(self._resetTask.bind(self), taskRef, immediate, deferred);
        } else {
          var errorMsg = 'reset task errored too many times, no longer retrying';
          logger.debug(self._getLogEntry(errorMsg), error);
          deferred.reject(new Error(errorMsg));
        }
      } else {
        if (committed && snapshot.exists()) {
          logger.debug(self._getLogEntry('reset ' + _getKey(snapshot)));
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
    var self = this;
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
          logger.debug(self._getLogEntry('Can\'t resolve task - no task ' +
            'currently being processed'));
        } else {
          logger.debug(self._getLogEntry('Can\'t resolve task - no longer ' +
            'processing current task'));
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
          if (task._state === self.inProgressState &&
              task._owner === id) {
            var outputTask = _.clone(newTask);
            if (!_.isPlainObject(outputTask)) {
              outputTask = {};
            }
            outputTask._state = _.get(outputTask, '_new_state');
            delete outputTask._new_state;
            if (!_.isNull(outputTask._state) && !_.isString(outputTask._state)) {
              if (_.isNull(self.finishedState) || outputTask._state === false) {
                // Remove the item if no `finished_state` set in the spec or
                // _new_state is explicitly set to `false`.
                return null;
              }
              outputTask._state = self.finishedState;
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
              logger.debug(self._getLogEntry('resolve task errored, retrying'),
                error);
              setImmediate(resolve, newTask);
            } else {
              var errorMsg = 'resolve task errored too many times, no longer ' +
                'retrying';
              logger.debug(self._getLogEntry(errorMsg), error);
              deferred.reject(new Error(errorMsg));
            }
          } else {
            if (committed && existedBefore) {
              logger.debug(self._getLogEntry('completed ' + _getKey(snapshot)));
            } else {
              logger.debug(self._getLogEntry('Can\'t resolve task - current ' +
                'task no longer owned by this process'));
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
    var self = this;
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
          logger.debug(self._getLogEntry('Can\'t reject task - no task ' +
            'currently being processed'));
        } else {
          logger.debug(self._getLogEntry('Can\'t reject task - no longer ' +
            'processing current task'));
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
          if (task._state === self.inProgressState &&
              task._owner === id) {
            var attempts = 0;
            var currentAttempts = _.get(task, '_error_details.attempts', 0);
            var currentPrevState = _.get(task, '_error_details.previous_state');
            if (currentAttempts > 0 &&
                currentPrevState === self.inProgressState) {
              attempts = currentAttempts;
            }
            if (attempts >= self.taskRetries) {
              task._state = errorState;
            } else {
              task._state = self.startState;
            }
            task._state_changed = SERVER_TIMESTAMP;
            task._owner = null;
            task._error_details = {
              previous_state: self.inProgressState,
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
              logger.debug(self._getLogEntry('reject task errored, retrying'),
                transactionError);
              setImmediate(reject, error);
            } else {
              var errorMsg = 'reject task errored too many times, no longer ' +
                'retrying';
              logger.debug(self._getLogEntry(errorMsg), transactionError);
              deferred.reject(new Error(errorMsg));
            }
          } else {
            if (committed && existedBefore) {
              logger.debug(self._getLogEntry('errored while attempting to ' +
                'complete ' + _getKey(snapshot)));
            } else {
              logger.debug(self._getLogEntry('Can\'t reject task - current task' +
                ' no longer owned by this process'));
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
    var self = this;
    var errorMsg;

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
        errorMsg = 'Can\'t update progress - no task currently being processed';
        logger.debug(self._getLogEntry(errorMsg));
        return Promise.reject(new Error(errorMsg));
      }
      return new Promise(function(resolve, reject) {
        currentTaskRef.transaction(function(task) {
          /* istanbul ignore if */
          if (_.isNull(task)) {
            return task;
          }
          var id = processId + ':' + taskNumber;
          if (task._state === self.inProgressState &&
              task._owner === id) {
            task._progress = progress;
            return task;
          }
          return undefined;
        }, function(transactionError, committed, snapshot) {
          /* istanbul ignore if */
          if (transactionError) {
            errorMsg = 'errored while attempting to update progress';
            logger.debug(self._getLogEntry(errorMsg), transactionError);
            return reject(new Error(errorMsg));
          }
          if (committed && snapshot.exists()) {
            return resolve();
          }
          errorMsg = 'Can\'t update progress - current task no longer owned ' +
            'by this process';
          logger.debug(self._getLogEntry(errorMsg));
          return reject(new Error(errorMsg));
        }, false);
      });
    };

    return updateProgress;
  };

  /**
   * Attempts to claim the next task in the queue.
   */
  function _tryToProcess(deferred) {
    var self = this;
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
        logger.debug(self._getLogEntry('finished shutdown'));
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
              nextTaskRef = _getRef(childSnap);
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
              if (task._state === self.startState) {
                task._state = self.inProgressState;
                task._state_changed = SERVER_TIMESTAMP;
                task._owner = processId + ':' + (taskNumber + 1);
                task._progress = 0;
                return task;
              }
              logger.debug(self._getLogEntry('task no longer in correct state: ' +
                'expected ' + self.startState + ', got ' + task._state));
              return undefined;
            }, function(error, committed, snapshot) {
              /* istanbul ignore if */
              if (error) {
                if (++retries < MAX_TRANSACTION_ATTEMPTS) {
                  logger.debug(self._getLogEntry('errored while attempting to ' +
                    'claim a new task, retrying'), error);
                  return setImmediate(self._tryToProcess.bind(self), deferred);
                }
                var errorMsg = 'errored while attempting to claim a new task ' +
                  'too many times, no longer retrying';
                logger.debug(self._getLogEntry(errorMsg), error);
                return deferred.reject(new Error(errorMsg));
              } else if (committed && snapshot.exists()) {
                if (malformed) {
                  logger.debug(self._getLogEntry('found malformed entry ' +
                    _getKey(snapshot)));
                } else {
                  /* istanbul ignore if */
                  if (busy) {
                    // Worker has become busy while the transaction was processing
                    // so give up the task for now so another worker can claim it
                    self._resetTask(nextTaskRef, true);
                  } else {
                    busy = true;
                    taskNumber += 1;
                    logger.debug(self._getLogEntry('claimed ' + _getKey(snapshot)));
                    currentTaskRef = _getRef(snapshot);
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
                      data._id = _getKey(snapshot);
                    }
                    var progress = self._updateProgress(taskNumber);
                    var resolve = self._resolve(taskNumber);
                    var reject = self._reject(taskNumber);
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
    var self = this;

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

    if (self.taskTimeout) {
      processingTasksRef = tasksRef.orderByChild('_state')
        .equalTo(self.inProgressState);

      var setUpTimeout = function(snapshot) {
        var taskName = _getKey(snapshot);
        var now = new Date().getTime();
        var startTime = (snapshot.child('_state_changed').val() || now);
        var expires = Math.max(0, startTime - now + self.taskTimeout);
        var ref = _getRef(snapshot);
        owners[taskName] = snapshot.child('_owner').val();
        expiryTimeouts[taskName] = setTimeout(
          self._resetTask.bind(self),
          expires,
          ref, false);
      };

      processingTaskAddedListener = processingTasksRef.on('child_added',
        setUpTimeout,
        /* istanbul ignore next */ function(error) {
          logger.debug(self._getLogEntry('errored listening to Firebase'), error);
        });
      processingTaskRemovedListener = processingTasksRef.on(
        'child_removed',
        function(snapshot) {
          var taskName = _getKey(snapshot);
          clearTimeout(expiryTimeouts[taskName]);
          delete expiryTimeouts[taskName];
          delete owners[taskName];
        }, /* istanbul ignore next */ function(error) {
          logger.debug(self._getLogEntry('errored listening to Firebase'), error);
        });
      processingTasksRef.on('child_changed', function(snapshot) {
        // This catches de-duped events from the server - if the task was removed
        // and added in quick succession, the server may squash them into a
        // single update
        var taskName = _getKey(snapshot);
        if (snapshot.child('_owner').val() !== owners[taskName]) {
          setUpTimeout(snapshot);
        }
      }, /* istanbul ignore next */ function(error) {
        logger.debug(self._getLogEntry('errored listening to Firebase'), error);
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
    var self = this;

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

    if (self._isValidTaskSpec(taskSpec)) {
      self.startState = taskSpec.startState || null;
      self.inProgressState = taskSpec.inProgressState;
      self.finishedState = taskSpec.finishedState || null;
      errorState = taskSpec.errorState || DEFAULT_ERROR_STATE;
      self.taskTimeout = taskSpec.timeout || null;
      self.taskRetries = taskSpec.retries || DEFAULT_RETRIES;

      newTaskRef = tasksRef
                            .orderByChild('_state')
                            .equalTo(self.startState)
                            .limitToFirst(1);
      logger.debug(self._getLogEntry('listening'));
      newTaskListener = newTaskRef.on(
        'child_added',
        function() {
          self._tryToProcess();
        }, /* istanbul ignore next */ function(error) {
          logger.debug(self._getLogEntry('errored listening to Firebase'), error);
        });
    } else {
      logger.debug(self._getLogEntry('invalid task spec, not listening for new ' +
        'tasks'));
      self.startState = null;
      self.inProgressState = null;
      self.finishedState = null;
      errorState = DEFAULT_ERROR_STATE;
      self.taskTimeout = null;
      self.taskRetries = DEFAULT_RETRIES;

      newTaskRef = null;
      newTaskListener = null;
    }

    self._setUpTimeouts();
  };

  function shutdown() {
    var self = this;

    if (shutdownDeferred) return shutdownDeferred.promise

    logger.debug(self._getLogEntry('shutting down'));

    // Set the global shutdown deferred promise, which signals we're shutting down
    shutdownDeferred = createDeferred()

    // We can report success immediately if we're not busy
    if (!busy) {
      self.setTaskSpec(null);
      logger.debug(self._getLogEntry('finished shutdown'));
      shutdownDeferred.resolve()
    }

    return shutdownDeferred.promise;
  };
}
