'use strict'

const DefaultQueueWorker = require('./lib/queue_worker.js')

module.exports = function Queue({
  tasksRef,
  processingFunction,
  options: {
    spec = {
      inProgressState: 'in_progress',
      timeout: 300000 // 5 minutes
    },
    numWorkers = 1,
    sanitize = true,
    suppressStack = false,
    QueueWorker = DefaultQueueWorker
  } = {}
}) {

  if (typeof processingFunction !== 'function')
    throwError('No processing function provided.')

  if (typeof numWorkers !== 'number' || numWorkers <= 0 || numWorkers % 1 !== 0)
    throwError('options.numWorkers must be a positive integer.')

  if (Boolean(sanitize) !== sanitize)
    throwError('options.sanitize must be a boolean.')
      
  if (Boolean(suppressStack) !== suppressStack)
    throwError('options.suppressStack must be a boolean.')

  const workers = createWorkers()
  
  this.shutdown = shutdown

  function createWorkers() {
    return Array(numWorkers).fill().map((_, i) => createWorker(spec, i))
  }

  function createWorker(spec, id) {
    const processIdBase = '' + id
    const worker = new QueueWorker({
      tasksRef,
      processIdBase,
      spec,
      sanitize,
      suppressStack,
      processingFunction
    })
    return worker
  }

  function shutdown() {
    const removedWorkers = workers.slice()
    workers.splice(0)
    return Promise.all(removedWorkers.map(worker => worker.shutdown()))
  }

  // allows us to use throw both as statement and expression
  function throwError(message) { throw new Error(message) }
}
