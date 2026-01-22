const { sequence, wait, waitFor, TIMEOUT } = require('./promise_utils')
const ops = require('./ops')
const Queue = require('../../')
/** @import { database } from 'firebase-admin' */
/** @import { Config, Falsy, Meta, Options, Task } from '../../src/types.ts' */

module.exports = {
  runSpecs,
  checkExecutionResults,
}

/**
 * @template {Record<string, any>} [T={}]
 * @typedef {{
 *   numTasks?: number,
 *   createTask?(index: number): Record<string, any>,
 *   queue?: {
 *     tasksRef?: database.Reference,
 *     count?: number,
 *     options?: Options
 *   },
 *   expectedNumProcessed?: number,
 *   process?: (task: Task, meta: Meta) => Promise<Record<string, any> | Falsy> | Record<string, any> | Falsy,
 *   test: (data: { tasks: Task[], processed: Task[], remaining: Task[] }) => ops.Operation | Promise<ops.Operation>,
 *   expectReportedErrors?: (reported: Array<Error>) => false | string,
 * } & T} Spec
 */

/**
 * @typedef {Success | Failure} Result
 * @typedef {{
 *   success: true,
 *   error?: Falsy
 * }} Success
 * @typedef {{
 *   success: false,
 *   error: string
 * }} Failure
 * @typedef {Result & { info: { sync: boolean, async: boolean } }} SpecResult
 */

/**
 * @template {Record<string, any>} [T=Record<string, any>]
 * @arg {{
 *   rootRef: database.Reference,
 *   report(props: { title: string, spec: Spec<T>, result: Result }): void,
 *   specs: [string, (() => Spec<T>) | Spec<T>][],
 *   timeout: number,
 * }} props
 */
async function runSpecs({ rootRef, report, specs, timeout }) {
  const results = await runSpecs()

  return { success: results.every(x => x.result.success), results }

  async function runSpecs() {
    return sequence(specs, async ([title, specOrFunction]) => {
      const spec = getSpecFrom(specOrFunction)
      const runResult = await Promise.race([runSpec(rootRef, title, spec, timeout), wait(timeout * 2)])
      const result = runResult === TIMEOUT
        ? /** @type const */ ({ success: false, info: { sync: false, async: false }, error: `timed out` })
        : runResult
      report({ title, spec, result })
      return { title, spec, result }
    })
  }
}

/**
 * @arg {{
 *   results: Array<{ result: Pick<SpecResult, 'info'> }>,
 *   report(result: { title: string, result: Result }):void
 * }} props
 */
function checkExecutionResults({ results, report }) {
  const executionResults = /** @type {{ title: String, result: Result }[]}*/ ([
    { title: `processed a task synchronous`, result: {
      success: results.some(x => x.result.info.sync), error: `failed`
    } },
    { title: `processed a task asynchronous`, result: {
      success: results.some(x => x.result.info.async), error: `failed`
    } },
  ])
  executionResults.forEach(report)

  return { success: executionResults.every(x => x.result.success), results: executionResults }
}

/**
 * @template  T
 * @arg {T | ((...args: any[]) => T)} specOrFunction
 */
function getSpecFrom(specOrFunction) {
  return isFunction(specOrFunction) ? specOrFunction() : specOrFunction
}

/**
 * @template T
 * @arg {T | ((...args: any[]) => any)} x
 * @returns {x is ((...args: any[]) => any)}
 */
function isFunction(x) {
  return typeof x === 'function'
}

/**
 * @arg {database.Reference} rootRef
 * @arg {string} title
 * @arg {Spec} spec
 * @arg {number} timeout
 * @returns {Promise<SpecResult>}
 */
async function runSpec(rootRef, title, spec, timeout) {
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
  const processTask = createProcessTask(process, timeout)
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
    return success ? { success, info: processTask.info } : { success, info: processTask.info, error }
  } catch (e) {
    if (!e)
      throw new Error(`No error was thrown`)

    const error = reportError.reported.join(`\n\n`) + (e === TIMEOUT
      ? `timed out`
      // @ts-expect-error
      : `${e}\n${e.stack}`
    )
    return { success: false, info: processTask.info, error }
  } finally {
    await tasksRef.remove()
    await queues.shutdown()
  }
}

function createReportError() {
  const reported = /** @type {Error[]} */ ([])
  /** @arg {Error} e */
  function reportError(e) { reported.push(e) }
  reportError.reported = reported

  return reportError
}

/**
 * @arg {((task: Task, meta: Meta) => Promise<any> | any) | undefined} process
 * @arg {number} timeout
 */
function createProcessTask(process, timeout) {
  const processed = /** @type {Array<Task>} */ ([])
  const info = { sync: false, async: false }

  /**
   * @arg {Task} task
   * @arg {Meta} meta
   */
  function processTask(task, meta) {
    try {
      const result = process && process(task, meta)
      if (isPromise(result)) {
        info.async = true
        result.then(_ => addProcessed(task), _ => addProcessed(task))
      } else {
        info.sync = true
        addProcessed(task)
      }
      return result
    } catch (e) {
      addProcessed(task)
      throw e
    }
    /** @arg {Task} x */
    function addProcessed(x) { processed[x.index] = x }
  }

  processTask.processed = processed
  processTask.info = info
  /** @arg {number} expectedNumProcessed */
  processTask.waitFor = async expectedNumProcessed =>
    waitFor(() => processed.filter(Boolean).length === expectedNumProcessed, { timeout })

  return processTask
}

/** @arg {any} x @returns {x is Promise<any>} */
function isPromise(x) {
  return Boolean(x && x.then)
}

/**
 * @arg {number} numTasks
 * @arg {(index: number) => Record<string, any>} createTask
 */
function createTasks(numTasks, createTask) {
  return [...Array(numTasks).keys()].map(createTask)
}

/**
 * @arg {number} count
 * @arg {Config} config
 */
function createQueues(count, config) {
  const queues = [...Array(count)].map(_ => new Queue(config))
  return { shutdown: async () => Promise.all(queues.map(x => x.shutdown())) }
}

/**
 * @arg {Array<Task>} tasks
 * @arg {database.Reference} tasksRef
 */
async function storeTasks(tasks, tasksRef) {
  return Promise.all(tasks.map(x => tasksRef.push(x)))
}

/** @arg {database.Reference} tasksRef @returns {Promise<Task[]>} */
async function fetchRemaining(tasksRef) {
  return Object.values((await tasksRef.once(`value`)).val() || {})
}

/**
 * @template {ops.Operation} O
 * @template T
 * @arg {(data: T) => O | Promise<O>} test
 * @arg {T} data
 */
async function executeTests(test, data) {
  try { return ops.execute(await test(data)) } catch (e) { return `Failed to execute test:\n${e}` }
}

/**
 * @arg {Array<Error>} reported
 * @arg {(reported: Array<Error>) => false | string} [expectReportedErrors]
 */
function executeReportedErrorTests(reported, expectReportedErrors) {
  return !reported.length
    ? expectReportedErrors && 'Expected an error to be reported'
    : expectReportedErrors
      ? expectReportedErrors(reported)
      : reported.join(`\n\n`)
}
