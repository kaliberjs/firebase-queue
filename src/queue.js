'use strict'

const uuid = require('uuid')
const QueueWorker = require('./lib/queue_worker.js')

module.exports = Queue

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
    numWorkers = 1
  } = {}
}) {
  if (!(this instanceof Queue)) throw new Error('You forgot the `new` keyword: `new Queue(...)`')

  const spec = { startState, inProgressState, finishedState, errorState }
  const queueId = uuid.v4()

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

  let shutdownStarted = null
  const removeWorkers  = createWorkers()

  this.shutdown = shutdown

  async function shutdown() {
    if (shutdownStarted) return shutdownStarted
    shutdownStarted = removeWorkers()
    return shutdownStarted
  }

  function createWorkers() {
    const workers = [...Array(numWorkers).keys()].map(createWorker)

    return async () => {
      const workersToRemove = workers.slice()
      workers.splice(0)
      await Promise.all(workersToRemove.map(worker => worker.shutdown()))
    }

    function createWorker(index) {
      return new QueueWorker({
        processId: `${queueId}:${index}`,
        tasksRef,
        spec,
        processTask,
        reportError
      })
    }
  }

  function isFunction(x) { return typeof x === 'function' }
  function isFirebaseRef(x) { return [x.on, x.off, x.transaction, x.orderByChild].every(isFunction) }
  function isString(x) { return typeof x === 'string' }
  function isNull(x) { return x === null }
  function not(y) { return x => x !== y }
  function isPositiveInteger(x) { return typeof x === 'number' && x >= 1 && x % 1 === 0 }

  function check(val, ...rest) {
    const message = rest[rest.length - 1]
    const or = rest.slice(0, rest.length -1)
    const valid = or.reduce(
      (result, and) => result || [].concat(and).reduce(
        (result, isValid) => result && isValid(val),
        true
      ),
      false
    )
    if (!valid) throw new Error(message)
  }
}
