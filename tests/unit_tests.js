const Queue = require(`../src/queue`)
const TransactionHelper = require(`../src/transaction_helper`)

const { waitFor, TIMEOUT } = require('./machinery/promise_utils')
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
  ]
}