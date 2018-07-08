const { sequence, wait, waitFor, TIMEOUT } = require('./promise_utils')
const ops = require('./ops')
const Queue = require('../../src/queue')

module.exports = runSpecs

async function runSpecs({ rootRef, report, specs }) {
  const result = await runSpecs()

  const executedSync = result.some(x => x.info.sync)
  const executedAsync = result.some(x => x.info.async)

  if (!executedSync) report({ title: `processed a task synchronous`, success: false, error: `failed` })
  if (!executedAsync) report({ title: `processed a task asynchronous`, success: false, error: `failed` })

  return executedSync && executedAsync && result.every(x => x.success)

  async function runSpecs() {
    return sequence(specs, async ([title, specOrFunction]) => {
      const spec = getSpecFrom(specOrFunction)
      const result = await Promise.race([runSpec(rootRef, title, spec), wait(1000)])
      const actualResult = result === TIMEOUT
        ? { title, success: false, info: {}, error: `timed out` }
        : result
      report(actualResult)
      return actualResult
    })
  }
}

async function runSpec(rootRef, title, spec) {
  const {
    numTasks = 1,
    createTask = index => ({ index }),
    queue: { tasksRef = rootRef.push().ref, count = 1, options = undefined } = {},
    expectedNumProcessed = numTasks,
    process = undefined,
    test,
    expectReportedErrors = undefined,
  } = spec

  const reportError = createReportError()
  const processTask = createProcessTask(process)
  const tasks = createTasks(numTasks, createTask)
  const queues = createQueues(count, { tasksRef, processTask, reportError, options })
  try {
    await storeTasks(tasks, tasksRef)
    await processTask.waitFor(expectedNumProcessed)
    await queues.shutdown()
    const remaining = await fetchRemaining(tasksRef)
    const reportedErrorFailure = executeReportedErrorTests(reportError.reported, expectReportedErrors)
    const testFailure = await executeTests(test, { tasks, processed: processTask.processed, remaining })

    const success = !reportedErrorFailure && !testFailure
    const error = [reportedErrorFailure, testFailure].filter(Boolean).join(`\n\n`)
    return { title, success, info: processTask.info, error }
  } catch (e) {
    const error = reportError.reported.join(`\n\n`) + (e === TIMEOUT ? `timed out` : `${e}\n${e.stack}`)
    return { title, success: false, info: processTask.info, error }
  } finally {
    await queues.shutdown()
  }
}

function getSpecFrom(specOrFunction) {
  return typeof specOrFunction === 'function' ? specOrFunction() : specOrFunction
}

function createReportError() {
  const reported = []
  function reportError(e) { reported.push(e) }
  reportError.reported = reported

  return reportError
}

function createProcessTask(process) {
  const processed = []
  const info = { sync: false, async: false }

  function processTask(task, meta) {
    try {
      const result = process && process(task, meta)
      if (result && result.then) {
        info.async = true
        result.then(_ => addProcessed(task), _ => addProcessed(task))
      } else {
        info.sync = true
        addProcessed(task)
      }
      return result
    } catch(e) {
      addProcessed(task)
      throw e
    }
    function addProcessed(x) { processed[x.index] = x }
  }

  processTask.processed = processed
  processTask.info = info
  processTask.waitFor = async expectedNumProcessed =>
    waitFor(() => processed.filter(Boolean).length === expectedNumProcessed, { timeout: 500 })

  return processTask
}

function createTasks(numTasks, createTask) {
  return [...Array(numTasks).keys()].map(createTask)
}

function createQueues(count, config) {
  const queues = [...Array(count)].map(_ => new Queue(config))
  return { shutdown: async () => Promise.all(queues.map(x => x.shutdown())) }
}

async function storeTasks(tasks, tasksRef) {
  return Promise.all(tasks.map(x => tasksRef.push(x)))
}

async function fetchRemaining(tasksRef) {
  return Object.values((await tasksRef.once(`value`)).val() || {})
}

async function executeTests(test, data) {
  return ops.execute(await test(data))
}

function executeReportedErrorTests(reported, expectReportedErrors) {
  return !reported.length
    ? expectReportedErrors && 'Expected an error to be reported'
    : expectReportedErrors
        ? expectReportedErrors(reported)
        : reported.join(`\n\n`)
}
