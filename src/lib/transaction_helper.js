module.exports = TransactionHelper

const SERVER_TIMESTAMP = {'.sv': 'timestamp'}
const MAX_TRANSACTION_ATTEMPTS = 10

function TransactionHelper({ serverOffset, processId, spec, taskNumber = 0 }) {

  const { startState, inProgressState, finishedState, errorState, timeout } = spec

  if (!processId) throw new Error('no processId')

  const owner = processId + ':' + taskNumber

  this.cloneForNextTask = cloneForNextTask

  this.resetIfTimedOut    = async ref => withRetries(ref, resetIfTimedOut)
  this.claim              = async ref => withRetries(ref, claim)

  this.updateProgressWith = async (ref, progress) => withRetries(ref, updateProgressWith(progress))
  this.resolveWith        = async (ref, newTask)  => withRetries(ref, resolveWith(newTask))
  this.rejectWith         = async (ref, error)    => withRetries(ref, rejectWith(error))

  function cloneForNextTask() {
    return new TransactionHelper({ serverOffset, taskNumber: taskNumber + 1, processId, spec })
  }

  function resetIfTimedOut(task) {
    if (task === null) return null

    const timeSinceUpdate = Date.now() + serverOffset - (task._state_changed || 0)
    const timedOut = (timeout && timeSinceUpdate >= timeout)

    if (isInProgress(task) && timedOut) {
      task._state = startState
      task._state_changed = SERVER_TIMESTAMP
      task._owner = null
      task._progress = null
      task._error_details = null
      return task
    }
  }

  function claim(task) {
    if (task === null) return null
    if ((task._state || null) === startState) {
      task._state = inProgressState
      task._state_changed = SERVER_TIMESTAMP
      task._owner = owner
      task._progress = 0
      return task
    }
  }

  function resolveWith(newTask) {
    return task => {
      if (task === null) return null

      if (isProcessing(task)) {
        if (newTask) return newTask
        else if (finishedState) {
          task._state = finishedState
          task._state_changed = SERVER_TIMESTAMP
          task._owner = null
          task._progress = 100
          task._error_details = null
        }
        else return null // remove
      }
    }
  }

  function rejectWith(error) {
    const errorString =
      (error instanceof Error && error.message) ||
      (typeof error === 'string' && error) ||
      (error !== undefined && error !== null && error.toString()) ||
      null

    const errorStack = (error && error.stack) || null

    return task => {
      if (task === null) return null

      if (isProcessing(task)) {
        task._state = errorState
        task._state_changed = SERVER_TIMESTAMP
        task._owner = null
        task._error_details = {
          error: errorString,
          error_stack: errorStack,
        }
        return task
      }
    }
  }

  function updateProgressWith(progress) {
    return task => {
      if (task === null) return null

      if (isProcessing(task)) {
        task._progress = progress
        return task
      }
    }
  }

  function isProcessing(x) { return isOwner(x) && isInProgress(x) }
  function isOwner(x) { return x._owner === owner }
  function isInProgress(x) { return x._state === inProgressState }

  async function withRetries(ref, transaction, attempts = 0) {
    try {
      const result = await ref.transaction(transaction, undefined, false)
      return result
    } catch (e) {
      if (attempts < MAX_TRANSACTION_ATTEMPTS) return withRetries(ref, transaction, attempts + 1)
      throw new Error(`transaction failed ${MAX_TRANSACTION_ATTEMPTS} times, error: ${e.message}`)
    }
  }
}
