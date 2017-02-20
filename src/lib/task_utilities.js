module.exports = TaskUtilities

function TaskUtilities({ serverOffset, spec: { errorState, timeout } }) {

  const fields = ['_state', '_state_changed', '_owner', '_progress', '_error_details']

  this.isInErrorState = isInErrorState
  this.expiresIn = expiresIn
  this.getOwner = getOwner
  this.sanitize = sanitize

  function isInErrorState(snapshot) {
    return snapshot.child('_state').val() === errorState
  }

  function expiresIn(snapshot) {
    const now = Date.now() + serverOffset
    const startTime = (snapshot.child('_state_changed').val() || now)
    return Math.max(0, startTime - now + timeout)
  }

  function getOwner(snapshot) {
    return snapshot.child('_owner').val()
  }

  function sanitize(task) {
    fields.forEach(field => { delete task[field] })
    return task
  }
}