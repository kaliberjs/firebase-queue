const { createQueue } = require('../src/queue')
const { createTransactionHelper } = require('../src/transaction_helper')
const { createObservability, noopObservability } = require('../src/observability')
/** @import { database } from 'firebase-admin' */
/** @import { Test } from './machinery/run_unit_tests' */
/** @import { Config, Spec, Task } from '../src/types' */

const { waitFor, TIMEOUT, wait } = require('./machinery/promise_utils')
const { expectError } = require('./machinery/test_utils')

/**
 * @arg {{ rootRef: database.Reference, timeout: number }} props
 * @returns {[string, Test][]}
 */
module.exports = ({ rootRef, timeout }) => {
  const tasksRef = rootRef.push().ref
  /** @arg {...any} args @returns {any} */
  function dontCallMe(...args) {
    throw new Error(`unexpected call of function with arguments:\b${JSON.stringify(args, null, 2)}`)
  }
  const validConfig = { tasksRef, processTask: dontCallMe, reportError: dontCallMe }
  // @ts-expect-error
  const validTaskRef = /** @type {database.Reference} */ ({
    on: dontCallMe,
    off: dontCallMe,
    push: () => tasksRef.push(),
    transaction: dontCallMe,
    orderByChild() { return this },
    equalTo() { return this },
    limitToFirst() { return this },
  })
  /** @arg {Partial<Config>} config */
  function newQueue(config) { return createQueue({ ...validConfig, ...config }) }
  /** @arg {Spec} spec */
  function newQueueWithSpec(spec) { return newQueue({ options: { spec } }) }

  return /** @type {[String, Test][]} */ ([
    [`Queue - require a valid 'tasksRef'`, () => expectError({
      // @ts-expect-error
      code: [() => newQueue({ tasksRef: `invalid` }), () => newQueue({ tasksRef: undefined })],
      test: [e => e.message.includes(`tasksRef`), `Error did not mention 'tasksRef'`],
    })],
    [`Queue - require a valid 'processTask'`, () => expectError({
      // @ts-expect-error
      code: [() => newQueue({ processTask: `invalid` }), () => newQueue({ processTask: undefined })],
      test: [e => e.message.includes(`processTask`), `Error did not mention 'processTask'`],
    })],
    [`Queue - require a valid 'reportError'`, () => expectError({
      // @ts-expect-error
      code: [() => newQueue({ reportError: `invalid` }), () => newQueue({ reportError: undefined })],
      test: [e => e.message.includes(`reportError`), `Error did not mention 'reportError'`],
    })],
    [`Queue - require a valid 'spec.inProgressState'`, () => expectError({
      code: [
        // @ts-expect-error
        () => newQueueWithSpec({ inProgressState: { invalid: true } }),
        // @ts-expect-error
        () => newQueueWithSpec({ inProgressState: null }),
      ],
      test: [e => e.message.includes(`spec.inProgressState`), `Error did not mention 'spec.inProgressState'`],
    })],
    [`Queue - require a valid 'spec.startState'`, () => expectError({
      code: [
        // @ts-expect-error
        () => newQueueWithSpec({ startState: { invalid: true } }),
        () => newQueueWithSpec({ startState: `in_progress` }),
      ],
      test: [e => e.message.includes(`spec.startState`), `Error did not mention 'spec.startState'`],
    })],
    [`Queue - require a valid 'spec.finishedState'`, () => expectError({
      code: [
        // @ts-expect-error
        () => newQueueWithSpec({ finishedState: { invalid: true } }),
        () => newQueueWithSpec({ finishedState: `in_progress` }),
        () => newQueueWithSpec({ startState: 'start', finishedState: `start` }),
      ],
      test: [e => e.message.includes(`spec.finishedState`), `Error did not mention 'spec.finishedState'`],
    })],
    [`Queue - require a valid 'spec.errorState'`, () => expectError({
      code: [
        // @ts-expect-error
        () => newQueueWithSpec({ errorState: { invalid: true } }),
        () => newQueueWithSpec({ errorState: `in_progress` }),
        () => newQueueWithSpec({ startState: 'start', errorState: `start` }),
        () => newQueueWithSpec({ finishedState: 'finished', errorState: `finished` }),
        // @ts-expect-error
        () => newQueueWithSpec({ errorState: null }),
      ],
      test: [e => e.message.includes(`spec.errorState`), `Error did not mention 'spec.errorState'`],
    })],
    [`Queue - require a valid 'options.numWorkers'`, () => expectError({
      code: [
        () => newQueue({ options: { numWorkers: 0 } }),
        () => newQueue({ options: { numWorkers: -1 } }),
        () => newQueue({ options: { numWorkers: NaN } }),
        // @ts-expect-error
        () => newQueue({ options: { numWorkers: 'nope' } }),
        () => newQueue({ options: { numWorkers: 1.1 } }),
        // @ts-expect-error
        () => newQueue({ options: { numWorkers: "1" } }),
      ],
      test: [e => e.message.includes(`numWorkers`), `Error did not mention 'numWorkers'`],
    })],
    [`Queue - should not continue processing after shutdown`, async () => {
      const processed = []
      const queue = createQueue({ tasksRef, processTask, reportError: dontCallMe })
      await queue.shutdown()
      await tasksRef.push({ index: 0 })
      try {
        await waitFor(() => processed.length === 1, { timeout })
        return `Expected timeout because no tasks should be processed`
      } catch (e) {
        if (e !== TIMEOUT) throw e
      } finally {
        await tasksRef.remove()
      }

      /** @arg {Task} x */
      function processTask(x) { processed.push(x) }
    }],
    [`Queue - should correctly report errors`, async () => {
      let reported = null
      function reportError(e) { reported = e }

      const tasksRef = { ...validTaskRef, on: (x_, y, onError) => onError(new Error('custom error')), off: () => {} }
      // @ts-expect-error
      const queue = createQueue({ tasksRef, processTask: dontCallMe, reportError })
      await queue.shutdown()

      return reported
        ? reported.message !== `custom error` && `The wrong error was reported`
        : `Expected an error to be reported`
    }],
    [`Queue - pause and resume`, async () => {
      const testTasksRef = rootRef.push().ref
      const processed = []
      let resolveProcessing
      const processingPromise = new Promise(r => { resolveProcessing = r })
      
      function processTask(x) { 
        processed.push(x) 
        if (processed.length === 1) resolveProcessing()
      }
      function reportError(e) { console.error(e) }

      const queue = createQueue({ tasksRef: testTasksRef, processTask, reportError })

      if (queue.isPaused()) return `Queue should not start paused`

      await testTasksRef.push({ index: 0 })
      await processingPromise
      
      queue.pause()
      if (!queue.isPaused()) return `Queue should be paused after pause()`
      
      await wait(50)

      await testTasksRef.push({ index: 1 })
      await wait(timeout * 0.3)

      if (processed.length !== 1) return `Only 1 task should be processed while paused, got ${processed.length}`

      queue.resume()
      if (queue.isPaused()) return `Queue should not be paused after resume()`

      await waitFor(() => processed.length === 2, { timeout })

      await queue.shutdown()
      await testTasksRef.remove()
    }],
    [`Queue - getStats includes pause and retry info`, async () => {
      const testTasksRef = rootRef.push().ref
      const queue = createQueue({
        tasksRef: testTasksRef,
        processTask: dontCallMe,
        reportError: dontCallMe,
        options: { retry: { maxAttempts: 3 } }
      })

      const stats = queue.getStats()
      await queue.shutdown()

      if (!('isPaused' in stats)) return `getStats should include isPaused`
      if (!('totalRetried' in stats)) return `getStats should include totalRetried`
      if (stats.isPaused !== false) return `isPaused should be false initially`
      if (stats.totalRetried !== 0) return `totalRetried should be 0 initially`
    }],
    [`Queue - lifecycle hooks for pause/resume`, async () => {
      const events = []
      const testTasksRef = rootRef.push().ref
      const queue = createQueue({
        tasksRef: testTasksRef,
        processTask: dontCallMe,
        reportError: dontCallMe,
        options: {
          lifecycle: {
            onQueuePaused: () => events.push('paused'),
            onQueueResumed: () => events.push('resumed'),
          }
        }
      })

      queue.pause()
      queue.resume()
      await queue.shutdown()

      if (events[0] !== 'paused') return `Expected onQueuePaused to be called`
      if (events[1] !== 'resumed') return `Expected onQueueResumed to be called`
    }],
    [`TransactionHelper - should retry transactions`, async () => {
      const t = createTransactionHelper({ processId: '', spec: {} })
      let tried = 0
      // @ts-expect-error
      await t.claim({ transaction })

      /** @arg {any} x */
      async function transaction(x) {
        if (!tried) {
          tried += 1
          throw new Error('try again')
        }
      }
    }],
    [`TransactionHelper - should give up after a certain amount of transactions`, async () => {
      const t = createTransactionHelper({ processId: '', spec: {} })
      try {
        // @ts-expect-error
        await t.claim({ transaction: async () => { throw new Error(`try again`) } })
        return `Expected transaction to give up after a certain amount of retries`
      } catch (e) {}
    }],

    // Observability tests
    ['Observability - noopObservability works without errors', () => {
      noopObservability.log('info', 'test.event', { key: 'value' })
      noopObservability.increment('test.counter', { label: 'foo' })
      noopObservability.histogram('test.duration', 123, { status: 'ok' })
      noopObservability.gauge('test.gauge', 5, { worker: 'a' })
    }],

    ['Observability - createObservability calls logger methods', () => {
      const calls = []
      const logger = {
        debug: (msg, meta) => calls.push({ level: 'debug', msg, meta }),
        info: (msg, meta) => calls.push({ level: 'info', msg, meta }),
        warn: (msg, meta) => calls.push({ level: 'warn', msg, meta }),
        error: (msg, meta) => calls.push({ level: 'error', msg, meta }),
      }
      
      const obs = createObservability({ logger, queueId: 'test-queue' })
      
      obs.log('info', 'task.claimed', { taskId: '123' })
      obs.log('warn', 'task.failed', { error: 'oops' })
      
      if (calls.length !== 2) return `Expected 2 log calls, got ${calls.length}`
      if (calls[0].level !== 'info') return `Expected info level, got ${calls[0].level}`
      if (calls[0].meta.queue !== 'test-queue') return `Expected queue label in meta`
      if (calls[0].meta.taskId !== '123') return `Expected taskId in meta`
    }],

    ['Observability - createObservability calls metrics methods', () => {
      const calls = []
      const metrics = {
        increment: (name, labels) => calls.push({ type: 'increment', name, labels }),
        histogram: (name, value, labels) => calls.push({ type: 'histogram', name, value, labels }),
        gauge: (name, value, labels) => calls.push({ type: 'gauge', name, value, labels }),
      }
      
      const obs = createObservability({ metrics, queueId: 'q1' })
      
      obs.increment('queue.tasks.completed', { worker: 'w1' })
      obs.histogram('queue.task.duration_ms', 500, { status: 'completed' })
      obs.gauge('queue.workers.busy', 3)
      
      if (calls.length !== 3) return `Expected 3 metric calls, got ${calls.length}`
      if (calls[0].type !== 'increment') return `Expected increment`
      if (calls[0].labels.queue !== 'q1') return `Expected queue label`
      if (calls[1].value !== 500) return `Expected histogram value 500`
      if (calls[2].value !== 3) return `Expected gauge value 3`
    }],

    ['Observability - handles errors in logger/metrics gracefully', () => {
      const logger = {
        info: () => { throw new Error('logger broke') },
      }
      const metrics = {
        increment: () => { throw new Error('metrics broke') },
      }
      
      const obs = createObservability({ logger, metrics })
      
      obs.log('info', 'test', {})
      obs.increment('test', {})
    }],
  ])
}
