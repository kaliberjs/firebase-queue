const Queue = require(`../src/queue`)
const TransactionHelper = require(`../src/transaction_helper`)
/** @import { database } from 'firebase-admin' */
/** @import { Test } from './machinery/run_unit_tests' */
/** @import { Config, Spec, Task } from '../src/types' */

const { waitFor, TIMEOUT } = require('./machinery/promise_utils')
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
    orderByChild: function () { return this },
    equalTo: function() { return this },
    limitToFirst: function() { return this },
  })
  /** @arg {Partial<Config>} config */
  function newQueue(config) { return new Queue({ ...validConfig, ...config }) }
  /** @arg {Spec} spec */
  function newQueueWithSpec(spec) { return newQueue({ options: { spec }}) }

  return /** @type {[String, Test][]} */ ([
    [`Queue - require the 'new' keyword`, () => expectError({
      // @ts-expect-error
      code: () => Queue(validConfig),
      test: [e => e.message.includes(`new`), `Error did not mention 'new'`],
    })],
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
      const processed = /** @type {Task[]} */ ([])
      const queue = new Queue({ tasksRef, processTask, reportError: dontCallMe })
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
      let reported = /** @type {Error | null} */ (null)
      /** @arg {Error} e */
      function reportError(e) { reported = e }

      /** @type {database.Reference} */
      const tasksRef = {
        ...validTaskRef,
        /** @returns {any} */
        on: (x, y, onError) => {
          if (!(typeof onError === 'function'))
            throw new Error('Passed in `onError` is missing or not a function')

          onError(new Error('custom error'))
        },
        off: () => {}
      }
      const queue = new Queue({ tasksRef, processTask: dontCallMe, reportError })
      await queue.shutdown()

      return reported
        ? reported.message !== `custom error` && `The wrong error was reported`
        : `Expected an error to be reported`

    }],
    [`TransactionHelper - should retry transactions`, async () => {
      const t = new TransactionHelper({ processId: '', spec: {} })
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
      const t = new TransactionHelper({ processId: '', spec: {} })
      try {
        // @ts-expect-error
        await t.claim({ transaction: async () => { throw new Error(`try again`) } })
        return `Expected transaction to give up after a certain amount of retries`
      } catch (e) {}
    }],
  ])
}
