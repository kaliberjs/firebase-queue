const firebase = require(`firebase`)
const TIMEOUT = Symbol(`timeout`)
const Queue = require(`../src/queue`)
const TransactionHelper = require(`../src/lib/transaction_helper`)

const app = firebase.initializeApp({
  // apiKey: `api key not needed`,
  databaseURL: `ws://localhost:5000`,
})
const db = app.database()
const root = db.ref()

const specs = [
  [`default options - process a task and remove it from the queue`, () => ({
    numTasks: 1,
    test: (data, processed, remaining) => [
      [processed, `equal`, data], `and`, [remaining, `equal`, []]
    ]
  })],

  [`default options - processing multiple tasks expecting them to be handled by the same worker`, () => {
    const owners = []

    return {
      numTasks: 4,
      process: async (x, { snapshot }) => {
        await wait(20) // simulate long running processes to give other workers a chance
        const [queueId, workerIndex] = snapshot.child(`_owner`).val().split(`:`)
        owners.push(queueId + workerIndex)
      },
      test: (data, processed, remaining) => [
        [processed, `equal`, data],
        `and`,
        [remaining, `equal`, []],
        `and`,
        [owners, `equal`, Array(owners.length).fill(owners[0])]
      ]
    }
  }],

  [`default options - failed to process a task`, () => ({
    numTasks: 5,
    process: ({ index }) => {
      switch (index) {
        case 0: throw new Error(`custom error`)
        case 1: throw `custom error`
        case 2: throw { toString: () => `custom error` }
        case 3: throw null
        case 4: throw undefined
      }
    },
    test: (data, processed, remaining) => {
      const normalizedRemaining = remaining.map((x, i) =>
        setFieldPresence(i > 2 ? `_error_details` : [`_error_details`, [`error_stack`]], `_state_changed`)(x)
      )
      const normalizedData = data.map((x, i) =>
        addFields({
          _error_details: i > 2 ? false : { error: `custom error`, error_stack: i === 0 },
          _progress: 0,
          _state: `error`,
          _state_changed: true,
        })(x)
      )
      return [
       [processed, `equal`, data], `and`, [normalizedRemaining, `equal`, normalizedData]
      ]
    }
  })],

  [`multiple queues with multiple workers - processing the same set of tasks`, () => ({
    numTasks: 8,
    queue: { count: 2, options: { numWorkers: 2 } },
    test: async (data, processed, remaining) => [
      [processed, `equal`, data], `and`, [remaining, `equal`, []]
    ]
  })],

  [`multiple queues with multiple workers - distribute the work`, () => {
    const owners = []

    return {
      numTasks: 4,
      queue: { count: 2, options: { numWorkers: 2 } },
      process: async (x, { snapshot }) => {
        await wait(20) // simulate long running processes to give other workers a chance
        const [queueId, workerIndex] = snapshot.child(`_owner`).val().split(`:`)
        owners.push(queueId + workerIndex)
      },
      test: (data, processed, remaining) => [
        [processed, `equal`, data], `and`, [remaining, `equal`, []], `and`, [owners, `noDuplicates`]
      ]
    }
  }],

  [`spec with finished state - leave tasks in queue with correct state`, () => ({
    numTasks: 1,
    queue: { options: { spec: { finishedState: `finished` } } },
    test: (data, processed, remaining) => {
      const normalizedRemaining = remaining.map(removeField(`_state_changed`))
      const normalizedData = data.map(x => ({ _progress: 100, _state: `finished`, ...x }))
      return [
        [processed, `equal`, data],
        `and`,
        [normalizedRemaining, `equal`, normalizedData]
      ]
    }
  })],

  [`processing - allow process function to access queue properties`, () => {
    const tasks = []

    return {
      numTasks: 1,
      process: (_, { snapshot }) => { tasks.push(snapshot.val()) },
      test: (data, processed, remaining) => {
        const normalizedTasks = tasks.map(
          setFieldPresence(`_state`, `_state_changed`, `_progress`, `_owner`)
        )
        const normalizedData = data.map(
          setTrue(`_owner`, `_progress`, `_state`, `_state_changed`)
        )
        return [
          [normalizedTasks, `equal`, normalizedData], `and`, [remaining, `equal`, []]
        ]
      }
    }
  }],

  [`complex processing - setProgress, access reference and replace task`, () => ({
    numTasks: 1,
    process: async (x, { snapshot, setProgress }) => {
      await setProgress(88)
      const { _progress } = (await snapshot.ref.once(`value`)).val()
      return { progress: _progress, _state: `do not process again` }
    },
    test: (data, processed, remaining) => {
      const dataWithProgressAndState = data.map(x => ({ _state: `do not process again`, progress: 88 }))
      return [
        [processed, `equal`, data],
        `and`,
        [remaining, `equal`, dataWithProgressAndState]
      ]
    }
  })],

  [`complex processing - setProgress after task was removed`, () => ({
    numTasks: 1,
    process: async (x, { snapshot, setProgress }) => {
      const owner = snapshot.child('_owner')
      await owner.ref.set(null)
      try {
        await setProgress(88)
      } finally {
        await owner.ref.set(owner.val())
      }
      /* istanbul ignore next */
      return { _error_details: { failure: 'expected setProgress to throw an error' } }
    },
    test: (data, processed, remaining) => {
      const normalizedRemaining = remaining.map(
        setFieldPresence([`_error_details`, [`error`, `error_stack`]], `_state_changed`)
      )
      const normalizedData = data.map(
        addFields({
          _error_details: { error: true, error_stack: true },
          _progress: 0,
          _state: `error`,
          _state_changed: true,
        })
      )
      return [
        [processed, `equal`, data],
        `and`,
        [normalizedRemaining, `equal`, normalizedData]
      ]
    }
  })],

  [`weird stuff - changes in the matrix #1 (changing the owner)`, () => ({
    numTasks: 2,
    process: async ({ index }, { snapshot }) => {
      await snapshot.ref.child(`_owner`).set('the owner got changed')
      if (index) throw new Error('oops')
    },
    test: (data, processed, remaining) => {
      const normalizedRemaining = remaining.map(setFieldPresence(`_state_changed`))
      const normalizedData = data.map(addFields({
        _owner: `the owner got changed`,
        _progress: 0,
        _state: `in_progress`,
        _state_changed: true,
      }))
      return [
        [processed, `equal`, data],
        `and`,
        [normalizedRemaining, `equal`, normalizedData]
      ]
    },
    expectReportedErrors: ([e1, e2]) => [
      e1 && e1.message.includes(`resolve`) && e2 && e2.message.includes(`reject`),
      /* istanbul ignore next */() => `Expected problems with resolving and rejecting to be reported`
    ],
  })],

  [`weird stuff - changes in the matrix #2 (changing the state)`, () => ({
    numTasks: 2,
    process: async ({ index }, { snapshot }) => {
      await snapshot.ref.child(`_state`).set('the state got changed')
      if (index) throw new Error('oops')
    },
    test: (data, processed, remaining) => {
      const normalizedRemaining = remaining.map(setFieldPresence(`_owner`, `_state_changed`))
      const normalizedData = data.map(addFields({
        _owner: true,
        _progress: 0,
        _state: `the state got changed`,
        _state_changed: true,
      }))
      return [
        [processed, `equal`, data],
        `and`,
        [normalizedRemaining, `equal`, normalizedData]
      ]
    },
    expectReportedErrors: ([e1, e2]) => [
      e1 && e1.message.includes(`resolve`) && e2 && e2.message.includes(`reject`),
      /* istanbul ignore next */() => `Expected problems with resolving and rejecting to be reported`
    ],
  })],

  // `Custom states, 'inProgressState', 'startState', 'finishedState' and 'errorState'`
]

/* istanbul ignore next */ function dontCallMe() { throw new Error(`please do not call me`) }
const validConfig = { tasksRef: root, processTask: dontCallMe, reportError: dontCallMe }
function newQueue(config) { return new Queue({ ...validConfig, ...config }) }
function newQueueWithSpec(spec) { return newQueue({ options: { spec }}) }
const unitTests = [
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

function expectError({ code, test: [test, error] }) {
  if (Array.isArray(code)) {
    return code.map(code => run(code, test, error))
      .map((result, i) => result && `[${i}] - ${result}`)
      .filter(Boolean)
      .join(`\n`)
  } else return run(code, test, error)

  function run(code, test, error) {
    try { code(); return `No error thrown` }
    catch (e) { return !test(e) && `${error}\n${e}` }
  }
}

const ops = {
  execute: ([a, op, ...b]) => {
    const f = ops[op]
    return f ? f(a, ...b) : [false, () => `Could not find operation with name '${op}'`]
  },
  equal: (a, b) => {
    const preparedA = prepare(a)
    const preparedB = prepare(b)
    return [
      JSON.stringify(preparedA) === JSON.stringify(preparedB),
      () => `Expected 'a' to equal 'b'\n` +
            `'a': ${JSON.stringify(preparedA, null, 2)}\n` +
            `'b': ${JSON.stringify(preparedB, null, 2)}`,
    ]

    function prepare(x) {
      if (!x) return x
      if (Array.isArray(x)) return x.map(prepare)
      if (typeof x === 'object') return new Map(Object.entries(x).map(([k, v]) => [k, prepare(v)]).sort())
      return x
    }
  },
  and: (aa, ...bb) => bb.filter(x => x !== `and`).reduce(
    ([success, error], bb) => success ? ops.execute(bb) : [success, error],
    ops.execute(aa)
  ),
  noDuplicates: a => [
    new Set(a).size === a.length,
    () => `Expected no duplicates in ${JSON.stringify(a, null, 2)}`,
  ],
}

db.goOnline()
performSelfCheck()
  .then(success => success && executeUnitTests(unitTests))
  .then(success => success && executeSpecs(specs))
  .then(success => {
    /* istanbul ignore if */
    if (!success) process.exitCode = 1
  })
  .catch(/* istanbul ignore next */ e => {
    console.error(e)
    process.exitCode = 1
  })
  .then(_ => { db.goOffline() })

async function performSelfCheck() {
  const selfCheckSpecs = [
    [`specs ops 'equal' - report failure when not equal`, () => ({
      numTasks: 1,
      test: _ => [0, `equal`, 1]
    })],
    [`specs ops 'and' - report failure when first fails`, () => ({
      numTasks: 1,
      test: _ => [[0, `equal`, 1], `and`, []]
    })],
    [`specs ops 'non existing op' - report failure when an op does not exist`, () => ({
      numTasks: 1,
      test: _ => [0, `non existing op`]
    })],
    [`specs ops 'noDuplicates' - report failure when there are duplicates`, () => ({
      numTasks: 1,
      test: _ => [[0, 0], `noDuplicates`]
    })],
    [`specs - report timeout for long processes`, () => ({
      numTasks: 1,
      process: async _ => { await wait(550) },
    })],
    [`specs - report errors if they occur in test`, () => ({
      numTasks: 1,
      test: _ => { throw new Error(`custom error`) }
    })],
    [`specs - report errors if they are reported`, () => ({
      numTasks: 1,
      process: async (_, { snapshot }) => {
        await snapshot.ref.child(`_state`).set('the state got changed')
      },
      test: () => [0, `equal`, 0],
    })],
    [`specs - report errors if they were expected`, () => ({
      numTasks: 1,
      test: () => [0, `equal`, 0],
      expectReportedErrors: true,
    })],
  ]

  const selfCheckUnitTests = [
    ['specs expect error - fail when no error is thrown', () => expectError({
      code: () => {},
      test: [undefined, undefined]
    })],
    ['specs expect error - fail the incorrect error is thrown', () => expectError({
      code: [() => { throw null }],
      test: [e => e !== null, `incorrect error`]
    })],
    ['specs colors - there is a difference between success and failure color', () =>
      (failureColor(``) !== successColor(``)) && `Colors were the same`
    ],
  ]

  const specResults = await sequence(selfCheckSpecs, reportWith(report, executeSpec))
  const unitTestResults = await sequence(selfCheckUnitTests, reportWith(report, executeUnitTest))

  const success = await executeSpecs([], report)
  report({ success, title: `specs - report if there are no specs that execute synchronously` })

  return !success && specResults.every(x => !x.success) && unitTestResults.every(x => !x.success)

  function report({ title, success }) {
    /* istanbul ignore if */
    if (success) console.error(`${failureColor(`x`)} ${title}\nExpected failure, but got success`)
  }
}

async function executeUnitTests(unitTests) {
  const result = await sequence(unitTests, reportWith(defaultReport, executeUnitTest))
  return result.every(x => x.success)
}

async function executeUnitTest([title, test]) {
  const error = await test()
  return { title, success: !error, error }
}

async function executeSpecs(specs, report = defaultReport) {
  const result = await sequence(specs, reportWith(report, executeSpec))

  if (!result.some(x => x.processedSynchronously)) {
    report({ title: `executed a synchonous test`, success: false, error: 'failed' })
    return false
  }

  return result.every(x => x.success)
}

function defaultReport({ title, success, error }) {
  /* istanbul ignore else */
  if (success) console.log(`${successColor(`✓`)} ${title}`)
  else console.error(`${failureColor(`x`)} ${title}\n\n    ${error.replace(/\n/g, `\n    `)}\n`)
}

function successColor(s) { return color(s, 10) }
function failureColor(s) { return color(s, 9) }
function color(s, color) {
  return `\x1B[38;5;${color}m${s}\x1B[0m`
}

function reportWith(report, f) {
  return async (...args) => {
    const result = await f(...args)
    report(result)
    return result
  }
}

async function executeSpec([title, f]) {
  const {
    numTasks,
    queue: { count = 1, options = undefined } = {},
    process,
    test,
    expectReportedErrors
  } = f()

  const tasksRef = root.push().ref
  const reportedErrors = []
  const reportError = e => { reportedErrors.push(e) }
  const data = [...Array(numTasks).keys()].map(index => ({ index }))
  const processed = []
  function addProcessed(x) { processed[x.index] = x } // this one should not be async
  let processedSynchronously = false
  const processTask = (task, meta) => {
    try {
      const result = process && process(task, meta)
      if (result && result.then) result.then(_ => addProcessed(task), _ => addProcessed(task))
      else {
        processedSynchronously = true
        addProcessed(task)
      }
      return result
    } catch(e) {
      addProcessed(task)
      throw e
    }
  }
  const queues = [...Array(count)].map(_ => new Queue({ tasksRef, processTask, reportError, options }))
  const shutdown = async () => Promise.all(queues.map(x => x.shutdown()))
  try {
    await Promise.all(data.map(x => tasksRef.push(x)))
    await waitFor(() => processed.filter(Boolean).length === data.length, { timeout: 500 })
    await shutdown()
    const remaining = Object.values((await tasksRef.once(`value`)).val() || {})
    const [testSuccess, testFailureMessage] = ops.execute(await test(data, processed, remaining))
    const [reportedErrorSuccess, reportedErrorFailureMessage = ''] =
      reportedErrors.length
      ? (
        expectReportedErrors
          ? expectReportedErrors(reportedErrors)
          : [false, () => reportedErrors.join(`\n\n`)]
      )
      : (
        expectReportedErrors
          ? [false, () => 'Expected an error to be reported']
          : [true]
      )

    const success = testSuccess && reportedErrorSuccess
    const error = (!reportedErrorSuccess && reportedErrorFailureMessage()) ||
                  (!testSuccess && testFailureMessage())
    return { title, success, processedSynchronously, error }
  } catch (e) {
    const error = reportedErrors.join(`\n\n`) + (e === TIMEOUT ? `timed out` : e)
    return { title, success: false, processedSynchronously, error }
  } finally {
    await shutdown()
  }
}

function addFields(o) {
  return x => ({ ...o, ...x })
}

function removeField(field) {
  return ({ [field]: removed, ...x }) => x
}

function setFieldPresence(...fields) {
  return x => {
    const changes = fields.map(y => {
      if (Array.isArray(y)) {
        const [key, rest] = y
        return { [key]: setFieldPresence(...rest)(x[key]) }
      } else return { [y]: notUndefined(x[y]) }
    })

    return Object.assign({}, x, ...changes)
  }
}

function setTrue(...fields) {
  return x => ({
    ...Object.assign({}, ...fields.map(y => ({ [y]: true }))),
    ...x,
  })
}

function notUndefined(x) { return x !== undefined }

async function sequence(a, f) {
  return a.reduce(
    async (result, x) => {
      const a = await result
      a.push(await f(x))
      return a
    },
    []
  )
}

function wait(x) {
  return new Promise(resolve => { setTimeout(resolve, x) })
}

function waitFor(f, { timeout }) {
  return new Promise((resolve, reject) => {
    const start = Date.now()
    check()

    function check() {
      setTimeout(
        async () => {
          const result = await Promise.race([f(), wait(timeout)])
          if (result) resolve()
          else if (Date.now() - start > timeout) reject(TIMEOUT)
          else check()
        },
        10
      )
    }

  })
}
