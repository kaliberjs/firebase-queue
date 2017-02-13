const _ = require('lodash')

module.exports = TaskWorker

const SERVER_TIMESTAMP = {'.sv': 'timestamp'}

function TaskWorker({ serverOffset, owner, spec: { startState, inProgressState, finishedState, errorState, timeout, retries } }) {

  this.reset = reset
  this.resetIfTimedOut = resetIfTimedOut
  this.resolveWith = resolveWith
  this.rejectWith = rejectWith
  this.updateProgressWith = updateProgressWith
  this.claimFor = claimFor

  function reset(task) {
    if (task === null) return null
    if (_isOwner(task) && _isInProgress(task)) return _reset(task)
  }

  function resetIfTimedOut(task) {
    if (task === null) return null

    const timeSinceUpdate = Date.now() - serverOffset - (task._state_changed || 0)
    const timedOut = (timeout && timeSinceUpdate >= timeout)

    if (_isInProgress(task) && timedOut) return _reset(task)
  }

  function resolveWith(newTask) {
    return task => {
      if (task === null) return null

      if (_isOwner(task) && _isInProgress(task)) {
        const replacement = _.isPlainObject(newTask) ? _.clone(newTask) : {}
        const newState = replacement._new_state
        delete replacement._new_state

        const validNewState = newState === null || typeof newState === 'string'
        const shouldRemove = newState === false || !(validNewState || finishedState)

        if (shouldRemove) return null

        replacement._state = validNewState ? newState : finishedState
        replacement._state_changed = SERVER_TIMESTAMP
        replacement._owner = null
        replacement._progress = 100
        replacement._error_details = null

        return replacement
      }
    }
  }

  function rejectWith(errorString, errorStack) {
    return task => {
      if (task === null) return null

      if (_isOwner(task) && _isInProgress(task)) {
        const {
          attempts: previousAttempts = 0,
          previous_state: previousState
        } = task._error_details || {}

        const attempts = previousState === inProgressState ? previousAttempts + 1 : 1

        task._state = attempts > retries ? errorState : startState
        task._state_changed = SERVER_TIMESTAMP
        task._owner = null
        task._error_details = {
          previous_state: inProgressState,
          error: errorString,
          error_stack: errorStack,
          attempts
        }
        return task
      }
    }
  }

  function updateProgressWith(progress) {
    return task => {
      if (task === null) return null

      if (_isOwner(task) && _isInProgress(task)) {
        task._progress = progress
        return task
      }
    }
  }

  function claimFor(getOwner) {
    return task => {
      if (task === null) return null

      if (!_.isPlainObject(task)) {
        return {
          _state: errorState,
          _state_changed: SERVER_TIMESTAMP,
          _error_details: {
            error: 'Task was malformed',
            original_task: task
          }
        }
      }

      if ((task._state || null) === startState) {

        task._state = inProgressState
        task._state_changed = SERVER_TIMESTAMP
        task._owner = getOwner()
        task._progress = 0
        return task
      }
    }
  }

  function _isOwner({ _owner }) { return _owner === owner }
  function _isInProgress({ _state }) { return _state === inProgressState } 

  function _reset(task) {
    task._state = startState
    task._state_changed = SERVER_TIMESTAMP
    task._owner = null
    task._progress = null
    task._error_details = null
    return task
  }
}