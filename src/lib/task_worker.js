module.exports = TaskWorker

const SERVER_TIMESTAMP = {'.sv': 'timestamp'}

function TaskWorker({ serverOffset, owner, spec: { startState, inProgressState, timeout } }) {

  this.reset = reset
  this.resetIfTimedOut = resetIfTimedOut

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