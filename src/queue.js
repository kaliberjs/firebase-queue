'use strict'

const QueueWorker = require('./lib/queue_worker.js')

const DEFAULT_ERROR_STATE = 'error'

module.exports = Queue

function Queue({
  tasksRef,
  processingFunction,
  options: {
    spec: rawSpec = {
      inProgressState: 'in_progress',
      timeout: 300000 // 5 minutes
    },
    sanitize = true
  } = {}
}) {
  const spec = getSanitizedTaskSpec(rawSpec)

  if (typeof processingFunction !== 'function')
    throwError('No processing function provided.')

  if (Boolean(sanitize) !== sanitize)
    throwError('options.sanitize must be a boolean.')


  const workers = createWorkers()

  this.shutdown = shutdown

  function createWorkers() {
    return [createWorker(spec)]
  }

  function createWorker(spec, id) {
    const worker = new QueueWorker({
      tasksRef,
      spec,
      sanitize,
      processingFunction
    })
    return worker
  }

  async function shutdown() {
    const removedWorkers = workers.slice()
    workers.splice(0)
    return Promise.all(removedWorkers.map(worker => worker.shutdown()))
  }

  function getSanitizedTaskSpec({
    startState = null,
    inProgressState = null,
    finishedState = null,
    errorState = DEFAULT_ERROR_STATE,
    timeout = null
  }) { return { startState, inProgressState, finishedState, errorState, timeout } }

  // allows us to use throw both as statement and expression
  function throwError(message) { throw new Error(message) }
}
