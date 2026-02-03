/** @import { ErrorToErrorDetails, Spec, Task, ReservedFields } from './types.ts' */
/** @import { database } from 'firebase-admin' */

module.exports = { createTransactionHelper }

/** @typedef {{ '.sv': 'timestamp' }} ServerTimestamp */
const SERVER_TIMESTAMP = /** @type {ServerTimestamp} */({'.sv': 'timestamp'})
const MAX_TRANSACTION_ATTEMPTS = 10

/**
 * @typedef {Object} TransactionHelper
 * @property {() => TransactionHelper} cloneForNextTask
 * @property {() => number | null} getClaimTime
 * @property {(ref: database.Reference) => Promise<database.TransactionResult>} claim
 * @property {(ref: database.Reference) => Promise<database.TransactionResult>} updateHeartbeat
 * @property {(ref: database.Reference, progress: number) => Promise<database.TransactionResult>} updateProgressWith
 * @property {(ref: database.Reference, newTask: Task | null) => Promise<database.TransactionResult>} resolveWith
 * @property {(ref: database.Reference, error: Error) => Promise<database.TransactionResult>} rejectWith
 */

/**
 * Creates a transaction helper for managing task state transitions.
 * 
 * @param {Object} props
 * @param {string} props.processId - Unique identifier for the worker process
 * @param {Spec} props.spec - Queue spec with state names
 * @param {ErrorToErrorDetails | null} [props.errorToErrorDetails] - Optional function to extract error details
 * @param {((taskId: string, attempt: number, error: Error) => void) | null} [props.onTransactionRetry] - Optional callback for transaction retries
 * @param {number} [props.taskNumber] - Task number for owner tracking
 * @returns {TransactionHelper}
 */
function createTransactionHelper({ processId, spec, errorToErrorDetails = null, onTransactionRetry = null, taskNumber = 0 }) {
  const { startState, inProgressState, finishedState, errorState } = spec
  const owner = `${processId}:${taskNumber}`
  /** @type {number | null} */
  let claimTime = null

  return {
    cloneForNextTask: () => createTransactionHelper({ processId, spec, errorToErrorDetails, onTransactionRetry, taskNumber: taskNumber + 1 }),
    getClaimTime: () => claimTime,
    claim: async ref => withRetries(ref, claim),
    updateHeartbeat: async ref => withRetries(ref, updateHeartbeat),
    updateProgressWith: async (ref, progress) => withRetries(ref, updateProgressWith(progress)),
    resolveWith: async (ref, newTask) => withRetries(ref, resolveWith(newTask)),
    rejectWith: async (ref, error) => withRetries(ref, rejectWith(error))
  }

  /**
   * Claims a task for processing.
   * @param {Task | null} task
   * @returns {Task | null | undefined}
   */
  function claim(task) {
    if (task === null) return null
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

  /**
   * Updates the heartbeat timestamp.
   * @param {Task | null} task
   * @returns {Task | null | undefined}
   */
  function updateHeartbeat(task) {
    if (task === null) return null
    if (isProcessing(task)) {
      task._heartbeat = SERVER_TIMESTAMP
      return task
    }
  }

  /**
   * Creates a resolver function for completing a task.
   * @param {Task | null} newTask
   * @returns {(task: Task | null) => Task | null | undefined}
   */
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
        return null
      }
    }
  }

  /**
   * Creates a rejecter function for failing a task.
   * @param {Error | string | any} error
   * @returns {(task: Task | null) => Task | null | undefined}
   */
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

  /**
   * Creates a progress updater function.
   * @param {number} progress
   * @returns {(task: Task | null) => Task | null | undefined}
   */
  function updateProgressWith(progress) {
    return task => {
      if (task === null) return null

      if (isProcessing(task)) {
        task._progress = progress
        return task
      }
    }
  }

  /**
   * Retries a transaction up to MAX_TRANSACTION_ATTEMPTS times.
   * @param {database.Reference} ref
   * @param {(task: Task | null) => Task | null | undefined} transaction
   * @param {number} [attempts]
   * @returns {Promise<database.TransactionResult>}
   */
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

  /**
   * @param {Task} x
   * @returns {boolean}
   */
  function isProcessing(x) { return x._owner === owner && x._state === inProgressState }
}
