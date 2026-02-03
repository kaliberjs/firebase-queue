
module.exports = TransactionHelper

const SERVER_TIMESTAMP = {'.sv': 'timestamp'}
const MAX_TRANSACTION_ATTEMPTS = 10

function TransactionHelper({ processId, spec, errorToErrorDetails, onTransactionRetry, taskNumber = 0 }) {
  const { startState, inProgressState, finishedState, errorState } = spec

  const owner = `${processId}:${taskNumber}`
  let claimTime = null

  this.cloneForNextTask = cloneForNextTask
  this.getClaimTime = () => claimTime

  this.claim              = async ref => withRetries(ref, claim)
  this.updateHeartbeat    = async ref => withRetries(ref, updateHeartbeat)
  this.updateProgressWith = async (ref, progress) => withRetries(ref, updateProgressWith(progress))
  this.resolveWith        = async (ref, newTask)  => withRetries(ref, resolveWith(newTask))
  this.rejectWith         = async (ref, error)    => withRetries(ref, rejectWith(error))

  function cloneForNextTask() {
    return new TransactionHelper({ processId, spec, errorToErrorDetails, onTransactionRetry, taskNumber: taskNumber + 1 })
  }

  function claim(task) {
    if (task === null) return null
    // Skip tasks that are scheduled for later retry
    if (task._retry_at && task._retry_at > Date.now()) return undefined
    if ((task._state || null) === startState) {
      claimTime = Date.now()
      task._state = inProgressState
      task._state_changed = SERVER_TIMESTAMP
      task._owner = owner
      task._progress = 0
      task._heartbeat = SERVER_TIMESTAMP
      task._started_at = SERVER_TIMESTAMP
      return task
    }
  }

  function updateHeartbeat(task) {
    if (task === null) return null
    if (isProcessing(task)) {
      task._heartbeat = SERVER_TIMESTAMP
      return task
    }
  }

  function resolveWith(newTask) {
    return task => {
      if (task === null) return null

      if (isProcessing(task)) {
        const durationMs = claimTime ? Date.now() - claimTime : null
        if (finishedState) {
          return {
            ...(newTask || task),
            _state: finishedState,
            _state_changed: SERVER_TIMESTAMP,
            _owner: null,
            _progress: 100,
            _heartbeat: null,
            _started_at: task._started_at,
            _duration_ms: durationMs,
            _error_details: null,
          }
        }
        if (newTask) return newTask
        return null // remove
      }
    }
  }

  function rejectWith(error) {
    const errorString =
      (error instanceof Error && error.message) ||
      (typeof error === 'string' && error) ||
      error?.toString?.() ||
      null

    const errorStack = error?.stack || null
    return task => {
      if (task === null) return null

      if (isProcessing(task)) {
        const durationMs = claimTime ? Date.now() - claimTime : null
        task._state = errorState
        task._state_changed = SERVER_TIMESTAMP
        task._owner = null
        task._heartbeat = null
        task._duration_ms = durationMs
        task._error_details = {
          error: errorString,
          error_stack: errorStack,
          ...errorToErrorDetails?.(error)
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
      if (attempts < MAX_TRANSACTION_ATTEMPTS) {
        if (onTransactionRetry) {
          try { onTransactionRetry(ref.key, attempts + 1, e) } catch (_) {}
        }
        return withRetries(ref, transaction, attempts + 1)
      }
      throw new Error(`transaction failed ${MAX_TRANSACTION_ATTEMPTS} times, error: ${e.message}`)
    }
  }

  function isProcessing(x) { return x._owner === owner && x._state === inProgressState }
}
