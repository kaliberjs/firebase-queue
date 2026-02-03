const Queue = require('../src/queue')
const TransactionHelper = require('../src/transaction_helper')
const { createObservability, noopObservability } = require('../src/observability')

const { waitFor, TIMEOUT, wait } = require('./machinery/promise_utils')
const { expectError } = require('./machinery/test_utils')

module.exports = ({ rootRef, timeout }) => {
  const tasksRef = rootRef.push().ref
  /* istanbul ignore next */function dontCallMe(...args) {
    throw new Error(`unexpected call of function with arguments:\b${JSON.stringify(args, null, 2)}`)
  }
  const validConfig = { tasksRef, processTask: dontCallMe, reportError: dontCallMe }
  const validTaskRef = {
    on: dontCallMe,
    off: dontCallMe,
    push: () => tasksRef.push(),
    transaction: dontCallMe,
    orderByChild: function () { return this },
    equalTo: function() { return this },
    limitToFirst: function() { return this },
  }
  function newQueue(config) { return new Queue({ ...validConfig, ...config }) }
  function newQueueWithSpec(spec) { return newQueue({ options: { spec }}) }

  return [
    [`Queue - require the 'new' keyword`, () => expectError({
      code: () => Queue(validConfig),
      test: [e => e.message.includes(`new`), `Error did not mention 'new'`],
    })],
    [`Queue - require a valid 'tasksRef'`, () => expectError({
      code: [() => newQueue({ tasksRef: `invalid` }), () => newQueue({ tasksRef: undefined })],
      test: [e => e.message.includes(`tasksRef`), `Error did not mention 'tasksRef'`],
    })],
    [`Queue - require a valid 'processTask'`, () => expectError({
      code: [() => newQueue({ processTask: `invalid` }), () => newQueue({ processTask: undefined })],
      test: [e => e.message.includes(`processTask`), `Error did not mention 'processTask'`],
    })],
    [`Queue - require a valid 'reportError'`, () => expectError({
      code: [() => newQueue({ reportError: `invalid` }), () => newQueue({ reportError: undefined })],
      test: [e => e.message.includes(`reportError`), `Error did not mention 'reportError'`],
    })],
    [`Queue - require a valid 'spec.inProgressState'`, () => expectError({
      code: [
        () => newQueueWithSpec({ inProgressState: { invalid: true } }),
        () => newQueueWithSpec({ inProgressState: null }),
      ],
      test: [e => e.message.includes(`spec.inProgressState`), `Error did not mention 'spec.inProgressState'`],
    })],
    [`Queue - require a valid 'spec.startState'`, () => expectError({
      code: [
        () => newQueueWithSpec({ startState: { invalid: true } }),
        () => newQueueWithSpec({ startState: `in_progress` }),
      ],
      test: [e => e.message.includes(`spec.startState`), `Error did not mention 'spec.startState'`],
    })],
    [`Queue - require a valid 'spec.finishedState'`, () => expectError({
      code: [
        () => newQueueWithSpec({ finishedState: { invalid: true } }),
        () => newQueueWithSpec({ finishedState: `in_progress` }),
        () => newQueueWithSpec({ startState: 'start', finishedState: `start` }),
      ],
      test: [e => e.message.includes(`spec.finishedState`), `Error did not mention 'spec.finishedState'`],
    })],
    [`Queue - require a valid 'spec.errorState'`, () => expectError({
      code: [
        () => newQueueWithSpec({ errorState: { invalid: true } }),
        () => newQueueWithSpec({ errorState: `in_progress` }),
        () => newQueueWithSpec({ startState: 'start', errorState: `start` }),
        () => newQueueWithSpec({ finishedState: 'finished', errorState: `finished` }),
        () => newQueueWithSpec({ errorState: null }),
      ],
      test: [e => e.message.includes(`spec.errorState`), `Error did not mention 'spec.errorState'`],
    })],
    [`Queue - require a valid 'options.numWorkers'`, () => expectError({
      code: [
        () => newQueue({ options: { numWorkers: 0 } }),
        () => newQueue({ options: { numWorkers: -1 } }),
        () => newQueue({ options: { numWorkers: NaN } }),
        () => newQueue({ options: { numWorkers: 'nope' } }),
        () => newQueue({ options: { numWorkers: 1.1 } }),
        () => newQueue({ options: { numWorkers: "1" } }),
      ],
      test: [e => e.message.includes(`numWorkers`), `Error did not mention 'numWorkers'`],
    })],
    [`Queue - should not continue processing after shutdown`, async () => {
      const processed = []
      const queue = new Queue({ tasksRef, processTask, reportError: dontCallMe })
      await queue.shutdown()
      await tasksRef.push({ index: 0 })
      try {
        await waitFor(() => processed.length === 1, { timeout })
        /* istanbul ignore next */ return `Expected timeout because no tasks should be processed`
      } catch (e) {
        /* istanbul ignore if */
        if (e !== TIMEOUT) throw e
      } finally {
        await tasksRef.remove()
      }

      /* istanbul ignore next */ function processTask(x) { processed.push(x) }
    }],
    [`Queue - should correctly report errors`, async () => {
      let reported = null
      function reportError(e) { reported = e }

      const tasksRef = { ...validTaskRef, on: (x_, y, onError) => onError(new Error('custom error')), off: () => {} }
      const queue = new Queue({ tasksRef, processTask: dontCallMe, reportError })
      await queue.shutdown()

      return reported
        ? reported.message !== `custom error` && /* istanbul ignore next */ `The wrong error was reported`
        : /* istanbul ignore next */ `Expected an error to be reported`

    }],
    [`Queue - pause and resume`, async () => {
      const testTasksRef = rootRef.push().ref
      const processed = []
      let resolveProcessing
      const processingPromise = new Promise(r => { resolveProcessing = r })
      
      function processTask(x) { 
        processed.push(x) 
        // Signal that first task was processed
        if (processed.length === 1) resolveProcessing()
      }
      function reportError(e) { console.error(e) }

      const queue = new Queue({ tasksRef: testTasksRef, processTask, reportError })

      // Verify initial state
      if (queue.isPaused()) return `Queue should not start paused`

      // Process one task first
      await testTasksRef.push({ index: 0 })
      await processingPromise
      
      // Now pause
      queue.pause()
      if (!queue.isPaused()) return `Queue should be paused after pause()`
      
      // Wait for worker to reach its next waitForNextTask cycle
      await wait(50)

      // Add second task while paused
      await testTasksRef.push({ index: 1 })
      await wait(timeout * 0.3)

      // Only first task should be processed (second task waiting)
      if (processed.length !== 1) return `Only 1 task should be processed while paused, got ${processed.length}`

      // Resume and verify second task is processed
      queue.resume()
      if (queue.isPaused()) return `Queue should not be paused after resume()`

      await waitFor(() => processed.length === 2, { timeout })

      await queue.shutdown()
      await testTasksRef.remove()
    }],
    [`Queue - getStats includes pause and retry info`, async () => {
      const testTasksRef = rootRef.push().ref
      const queue = new Queue({
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
      const queue = new Queue({
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
      const t = new TransactionHelper({ spec: {} })
      let tried = 0
      await t.claim({ transaction })

      async function transaction() {
        if (!tried) {
          tried += 1
          throw new Error('try again')
        }
      }
    }],
    [`TransactionHelper - should give up after a certain amount of transactions`, async () => {
      const t = new TransactionHelper({ spec: {} })
      try {
        await t.claim({ transaction: async () => { throw new Error(`try again`) } })
        /* istanbul ignore next */
        return `Expected transaction to give up after a certain amount of retries`
      } catch (e) {}
    }],

    // Observability tests
    ['Observability - noopObservability works without errors', () => {
      // All methods should be callable without throwing
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
      
      // Should not throw
      obs.log('info', 'test', {})
      obs.increment('test', {})
    }],
  ]
}
