'use strict'

const QueueWorker = require('./lib/queue_worker.js')

const DEFAULT_NUM_WORKERS = 1
const DEFAULT_SANITIZE = true
const DEFAULT_SUPPRESS_STACK = false
const DEFAULT_TASK_SPEC = {
  inProgressState: 'in_progress',
  timeout: 300000 // 5 minutes
}


/**
 * @constructor
 * @param {firebase.database.Reference|Object} ref A Firebase Realtime Database
 *  reference to the queue or an object containing both keys:
 *     - tasksRef: {firebase.database.Reference} A Firebase Realtime Database
 *         reference to the queue tasks location.
 *     - specsRef: {firebase.database.Reference} A Firebase Realtime Database
 *         reference to the queue specs location.
 * @param {Object} options (optional) Object containing possible keys:
 *     - specId: {String} the task specification ID for the workers.
 *     - numWorkers: {Number} The number of workers to create for this task.
 *     - sanitize: {Boolean} Whether to sanitize the 'data' passed to the
 *         processing function of internal queue keys.
 * @param {Function} processingFunction A function that is called each time to
 *   process a task. This function is passed four parameters:
 *     - data {Object} The current data at the location.
 *     - progress {Function} A function to update the progress percent of the
 *         task for informational purposes. Pass it a number between 0 and 100.
 *         Returns a promise of whether the operation was completed
 *         successfully.
 *     - resolve {Function} An asychronous callback function - call this
 *         function when the processingFunction completes successfully. This
 *         takes an optional Object parameter that, if passed, will overwrite
 *         the data at the task location, and returns a promise of whether the
 *         operation was successful.
 *     - reject {Function} An asynchronous callback function - call this
 *         function if the processingFunction encounters an error. This takes
 *         an optional String or Object parameter that will be stored in the
 *         '_error_details/error' location in the task and returns a promise
 *         of whether the operation was successful.
 * @returns {Object} The new Queue object.
 */
module.exports = function Queue() {

  var constructorArguments = arguments

  let currentTaskSpec = undefined
  let initialized = false
  let shuttingDown = false

  let specChangeListener = null

  const options = constructorArguments.length === 3
    ? (isObject(constructorArguments[1]) && constructorArguments[1]) || throwError('Options parameter must be a plain object.')
    : {}

  const processingFunction = constructorArguments.length < 2
    ? throwError('Queue must at least have the queueRef and processingFunction arguments.')
    : constructorArguments.length === 2
    ? constructorArguments[1]
    : constructorArguments.length === 3
    ? constructorArguments[2]
    : throwError('Queue can only take at most three arguments - queueRef, options (optional), and processingFunction.')

  const numWorkers = options.numWorkers === undefined
    ? DEFAULT_NUM_WORKERS
    : ((typeof options.numWorkers == 'number' && options.numWorkers > 0 && options.numWorkers % 1 === 0) || throwError('options.numWorkers must be a positive integer.')) &&
      options.numWorkers

  const specId = options.specId === undefined
    ? undefined
    : (typeof options.specId === 'string' && options.specId) || 
      throwError('options.specId must be a String.')

  const sanitize = options.sanitize === undefined
    ? DEFAULT_SANITIZE
    : ((Boolean(options.sanitize) === options.sanitize) || throwError('options.sanitize must be a boolean.')) &&
      options.sanitize
      
  const suppressStack = options.suppressStack === undefined
    ? DEFAULT_SUPPRESS_STACK
    : ((options.suppressStack === true || options.suppressStack === false) || throwError('options.suppressStack must be a boolean.')) &&
      options.suppressStack

  const [tasksRef, specsRef] = constructorArguments[0].tasksRef && (!specId || constructorArguments[0].specsRef)
    ? [constructorArguments[0].tasksRef, constructorArguments[0].specsRef]
    : isObject(constructorArguments[0])
    ? throwError('When ref is an object it must contain both keys \'tasksRef\' and \'specsRef\'')
    : [constructorArguments[0].child('tasks'), constructorArguments[0].child('specs')]

  const workers = Array(numWorkers).fill().map(createWorker)

  if (!specId) {
    workers.forEach(worker => worker.setTaskSpec(DEFAULT_TASK_SPEC))
    initialized = true
  } else {
    specChangeListener = specsRef.child(specId).on(
      'value', 
      taskSpecSnap => {
        const taskSpec = {
          startState: val('start_state'),
          inProgressState: val('in_progress_state'),
          finishedState: val('finished_state'),
          errorState: val('error_state'),
          timeout: val('timeout'),
          retries: val('retries')
        }

        workers.forEach(worker => worker.setTaskSpec(taskSpec))
        currentTaskSpec = taskSpec
        initialized = true

        function val(key) { return taskSpecSnap.child(key).val() }
      },
      /* istanbul ignore next */ throwError
    )
  }

  this.addWorker = addWorker
  this.getWorkerCount = getWorkerCount
  this.shutdownWorker = shutdownWorker
  this.shutdown = shutdown

  // used in tests
  this._workers = workers
  this._specId = specId
  this._sanitize = sanitize
  this._suppressStack = suppressStack
  this._initialized = () => initialized
  this._specChangeListener = () => specChangeListener

  return this

  /**
   * Gracefully shuts down a queue.
   * @returns {Promise} A promise fulfilled when all the worker processes
   *   have finished their current tasks and are no longer listening for new ones.
   */
  function shutdown() {
    shuttingDown = true

    if (specChangeListener) {
      specsRef.child(specId).off('value', specChangeListener)
      specChangeListener = null
    }

    return Promise.all(workers.map(worker => worker.shutdown()))
  }

  /**
   * Adds a queue worker.
   * @returns {QueueWorker} the worker created.
   */
  function addWorker() {
    if (shuttingDown) throwError('Cannot add worker while queue is shutting down')

    const worker = createWorker()
    workers.push(worker)

    if (!specId) worker.setTaskSpec(DEFAULT_TASK_SPEC)
    else if (currentTaskSpec) worker.setTaskSpec(currentTaskSpec)
    // if the currentTaskSpec is not yet set it will be called once it's fetched

    return worker
  }

  /**
   * Gets queue worker count.
   * @returns {Number} Total number of workers for this queue.
   */
  function getWorkerCount() {
    return workers.length
  }

  /**
   * Shutdowns a queue worker if one exists.
   * @returns {RSVP.Promise} A promise fulfilled once the worker is shutdown
   *   or rejected if there are no workers left to shutdown.
   */
  function shutdownWorker() {
    const worker = workers.pop()

    return worker
      ? worker.shutdown()
      : Promise.reject(new Error('No workers to shutdown'))
  }

  function createWorker(_, i = workers.length) {
    const processId = (specId ? specId + ':' : '') + i
    return new QueueWorker(
      tasksRef,
      processId,
      sanitize,
      suppressStack,
      processingFunction
    )
  }

  function isObject(value) {
    return value && !Object.getPrototypeOf(Object.getPrototypeOf(value))
  }

  // allows us to use throw both as statement and expression
  function throwError(message) { throw new Error(message) }
}
