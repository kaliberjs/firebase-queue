const QueueWorker = require('./queue_worker.js')
/** @import { Config } from './types.ts' */

module.exports = Queue

/**
 * @constructor
 * @arg {Config} config
 */
function Queue({
  tasksRef,
  processTask,
  reportError,
  options: {
    spec: {
      startState = null,
      inProgressState = 'in_progress',
      finishedState = null,
      errorState = 'error'
    } = {},
    errorToErrorDetails = null,
    numWorkers = 1
  } = {}
}) {
  if (!(this instanceof Queue)) throw new Error('You forgot the `new` keyword: `new Queue(...)`')

  const spec = { startState, inProgressState, finishedState, errorState }
  check(tasksRef, isFirebaseRef,
    'tasksRef must be a Firebase reference')

  check(processTask, isFunction,
    'processTask must be a function')

  check(reportError, isFunction,
    'reportError must be a function')

  check(inProgressState, isString,
    'options.spec.inProgressState must be a string')

  check(startState, isNull, [isString, not(inProgressState)],
    'options.spec.startState must be null or a string that !== inProgressState')

  check(finishedState, isNull, [isString, not(inProgressState), not(startState)],
    'options.spec.finishedState must be null or a string that !== inProgressState and !== startState')

  check(errorState, [isString, not(inProgressState), not(startState), not(finishedState)],
    'options.spec.errorState must be a string that !== inProgressState and !== startState and !== finishedState')

  check(numWorkers, isPositiveInteger,
    'options.numWorkers must be a positive integer')

  const queueId = tasksRef.push().key
  /** @type {null | Promise<void>} */
  let shutdownStarted = null
  let removeWorkers  = createWorkers()

  this.shutdown = shutdown

  async function shutdown() {
    if (shutdownStarted) return shutdownStarted
    shutdownStarted = removeWorkers()
    // @ts-expect-error
    removeWorkers = null // make sure no references to workers are being kept and allow garbage collection
    return shutdownStarted
  }

  function createWorkers() {
    const workers = [...Array(numWorkers).keys()].map(createWorker)

    return async () => {
      await Promise.all(workers.map(worker => worker.shutdown()))
    }

    /** @arg {number} index */
    function createWorker(index) {
      return new QueueWorker({
        processId: `${queueId}:${index}`,
        tasksRef,
        spec,
        errorToErrorDetails,
        processTask,
        reportError
      })
    }
  }

  /** @arg {any} x */
  function isFunction(x) { return typeof x === 'function' }
  /** @arg {any} x */
  function isFirebaseRef(x) { return x && [x.on, x.off, x.transaction, x.orderByChild, x.push].every(isFunction) }
  /** @arg {any} x */
  function isString(x) { return typeof x === 'string' }
  /** @arg {any} x */
  function isNull(x) { return x === null }
  /** @arg {any} y @returns {(x: any) => boolean} */
  function not(y) { return x => x !== y }
  /** @arg {any} x */
  function isPositiveInteger(x) { return typeof x === 'number' && x >= 1 && x % 1 === 0 }

  /**
   * @arg {any} val
   * @arg  {...any} rest
   */
  function check(val, ...rest) {
    const message = rest[rest.length - 1]
    const or = rest.slice(0, rest.length - 1)
    const valid = or.reduce(
      (result, and) => result || /** @type {((x: any) => boolean)[]} */ ([]).concat(and).reduce(
        (result, isValid) => result && isValid(val),
        true
      ),
      false
    )
    if (!valid) throw new Error(message)
  }
}
