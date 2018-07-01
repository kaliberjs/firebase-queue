'use strict'

module.exports = TransactionHelper

const SERVER_TIMESTAMP = {'.sv': 'timestamp'}
const MAX_TRANSACTION_ATTEMPTS = 10

function TransactionHelper({ processId, spec, taskNumber = 0 }) {

  const { startState, inProgressState, finishedState, errorState } = spec

  const owner = processId + ':' + taskNumber

  this.cloneForNextTask = cloneForNextTask

  this.claim              = async ref => withRetries(ref, claim)

  this.updateProgressWith = async (ref, progress) => withRetries(ref, updateProgressWith(progress))
  this.resolveWith        = async (ref, newTask)  => withRetries(ref, resolveWith(newTask))
  this.rejectWith         = async (ref, error)    => withRetries(ref, rejectWith(error))

  function cloneForNextTask() {
    return new TransactionHelper({ processId, spec, taskNumber: taskNumber + 1 })
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
          return task
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

  async function withRetries(ref, transaction, attempts = 0) {
    try {
      const result = await ref.transaction(transaction, undefined, false)
      return result
    } catch (e) {
      if (attempts < MAX_TRANSACTION_ATTEMPTS) return withRetries(ref, transaction, attempts + 1)
      throw new Error(`transaction failed ${MAX_TRANSACTION_ATTEMPTS} times, error: ${e.message}`)
    }
  }

  function isProcessing(x) { return x._owner === owner && x._state === inProgressState }
}
