'use strict'

/** @import { ErrorToErrorDetails, Spec, Task } from './types.ts' */
/** @import { database } from 'firebase-admin' */

module.exports = TransactionHelper

const SERVER_TIMESTAMP = {'.sv': 'timestamp'}
const MAX_TRANSACTION_ATTEMPTS = 10

/**
 * @arg {{
 *   processId: string,
 *   spec: Spec,
 *   errorToErrorDetails?: ErrorToErrorDetails | null,
 *   taskNumber?: number,
 * }} props
 */
function TransactionHelper({ processId, spec, errorToErrorDetails, taskNumber = 0 }) {
  const { startState, inProgressState, finishedState, errorState } = spec

  const owner = processId + ':' + taskNumber

  this.cloneForNextTask = cloneForNextTask

  /** @arg {database.Reference} ref */
  this.claim              = async ref => withRetries(ref, claim)

  /**
   * @arg {database.Reference} ref
   * @arg {number} progress
   */
  this.updateProgressWith = async (ref, progress) => withRetries(ref, updateProgressWith(progress))
  /**
   * @arg {database.Reference} ref
   * @arg {Record<string, any>} newTask
   */
  this.resolveWith        = async (ref, newTask)  => withRetries(ref, resolveWith(newTask))
  /**
   * @arg {database.Reference} ref
   * @arg {Error} error
   */
  this.rejectWith         = async (ref, error)    => withRetries(ref, rejectWith(error))

  function cloneForNextTask() {
    return new TransactionHelper({ processId, spec, errorToErrorDetails, taskNumber: taskNumber + 1 })
  }

  /** @arg {Task | null} task */
  function claim(task) {
    if (task === null) return null
    if ((task._state || null) === startState) {
      task._state = inProgressState
      // @ts-expect-error
      task._state_changed = SERVER_TIMESTAMP
      task._owner = owner
      task._progress = 0
      return task
    }
  }

  /** @arg {Record<string, any>} newTask */
  function resolveWith(newTask) {
    /** @arg {Task | null} task */
    return task => {
      if (task === null) return null

      if (isProcessing(task)) {
        if (finishedState) {
          return {
            ...(newTask || task),
            _state: finishedState,
            _state_changed: SERVER_TIMESTAMP,
            _owner: null,
            _progress: 100,
            _error_details: null,
          }
        }
        else if (newTask) return newTask
        else return null // remove
      }
    }
  }

  /** @arg {any} error */
  function rejectWith(error) {
    const errorString =
      (error instanceof Error && error.message) ||
      (typeof error === 'string' && error) ||
      (error !== undefined && error !== null && error.toString()) ||
      null

    const errorStack = (error && error.stack) || null
    /** @arg {Task} task */
    return task => {
      if (task === null) return null

      if (isProcessing(task)) {
        task._state = errorState
        // @ts-expect-error
        task._state_changed = SERVER_TIMESTAMP
        // @ts-expect-error
        task._owner = null
        task._error_details = {
          error: errorString,
          error_stack: errorStack,
          ...(errorToErrorDetails && errorToErrorDetails(error))
        }
        return task
      }
    }
  }

  /** @arg {number} progress */
  function updateProgressWith(progress) {
    /** @arg {Task | null} task */
    return task => {
      if (task === null) return null

      if (isProcessing(task)) {
        task._progress = progress
        return task
      }
    }
  }

  /**
   * @arg {database.Reference} ref
   * @arg {(x: any) => any} transaction
   * @arg {number} [attempts]
   */
  async function withRetries(ref, transaction, attempts = 0) {
    try {
      const result = await ref.transaction(transaction, undefined, false)
      return result
    } catch (e) {
      if (attempts < MAX_TRANSACTION_ATTEMPTS) return withRetries(ref, transaction, attempts + 1)
      throw new Error(`transaction failed ${MAX_TRANSACTION_ATTEMPTS} times, error: ${e instanceof Error ? e.message : e}`)
    }
  }

  /** @arg {Task} x */
  function isProcessing(x) { return x._owner === owner && x._state === inProgressState }
}
