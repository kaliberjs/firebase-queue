const firebase = require(`firebase`)
const TIMEOUT = Symbol(`timeout`)
const Queue = require(`../src/queue`)

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

  [`default options - failed to process a task`, () => ({
    numTasks: 1,
    process: _ => { throw new Error(`custom error`) },
    test: (data, processed, remaining) => {
      const normalizedRemaining = remaining.map(
        setFieldPresence([`_error_details`, [`error_stack`]], `_state_changed`)
      )
      const normalizedData = data.map(
        addFields({
          _error_details: { error: `custom error`, error_stack: true },
          _progress: 0,
          _state: `error`,
          _state_changed: true,
        })
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
      queue: { count: 2, options: { numWorkers: 2, sanitize: false } },
      process: async ({ index, _owner }) => {
        await wait(20) // simulate long running processes to give other workers a chance
        const [queueId, workerIndex] = _owner.split(`:`)
        owners.push(queueId + workerIndex)
        return { processed: { index } }
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

  [`unsanitized tasks - allow process function to access queue properties`, () => ({
    numTasks: 1,
    queue: { options: { sanitize: false } },
    test: (data, processed, remaining) => {
      const normalizedProcessed = processed.map(
        setFieldPresence(`_state`, `_state_changed`, `_progress`, `_owner`)
      )
      const normalizedData = data.map(
        setTrue(`_owner`, `_progress`, `_state`, `_state_changed`)
      )
      return [
        [normalizedProcessed, `equal`, normalizedData], `and`, [remaining, `equal`, []]
      ]
    }
  })],

  [`complex processing - setProgress, access reference and replace task`, () => ({
    numTasks: 1,
    process: async (x, { ref, setProgress }) => {
      await setProgress(88)
      const { _progress } = (await ref.once(`value`)).val()
      const result = { progress: _progress, _state: `do not process again` }
      return { processed: x, result }
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
    process: async (x, { ref, setProgress }) => {
      const ownerRef = ref.child('_owner')
      const owner = (await ownerRef.once('value')).val()
      await ownerRef.set(null)
      try {
        await setProgress(88)
      } finally {
        await ownerRef.set(owner)
      }
      /* istanbul ignore next */
      return { processed: x, result: { _error_details: { failure: 'expected setProgress to throw an error' } } }
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
]

const ops = {
  execute: ([a, op, ...b]) => {
    const f = ops[op]
    return f ? f(a, ...b) : [false, () => `Could not find operation with name '${op}'`]
  },
  equal: (a, b) => [
    JSON.stringify(a) === JSON.stringify(b),
    () => `Expected 'a' to equal 'b'\n'a': ${JSON.stringify(a, null, 2)}\n'b': ${JSON.stringify(b, null, 2)}`,
  ],
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
checkSpecs()
  .then(success => success && execute(specs))
  .then(success => {
    /* istanbul ignore if */
    if (!success) process.exitCode = 1
  })
  .catch(/* istanbul ignore next */ e => {
    console.error(e)
    process.exitCode = 1
  })
  .then(_ => { db.goOffline() })

async function checkSpecs() {
  const selfChecks = [
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
      process: async _ => { await wait(550) }
    })],
    [`specs - report errors if they occur in test`, () => ({
      numTasks: 1,
      test: _ => { throw new Error(`custom error`) }
    })],
  ]

  const result = await sequence(selfChecks, reportWith(report, executeSpec))

  const success = await execute([], report)
  report({ success, title: `specs - report if there are no specs that execute synchronously` })

  return !success && result.every(x => !x.success)

  function report({ title, success }) {
    /* istanbul ignore if */
    if (success) console.error(`${title} - no failure`)
  }
}

async function execute(specs, report = defaultReport) {
  const result = await sequence(specs, reportWith(report, executeSpec))

  if (!result.some(x => x.processedSynchronously)) {
    report({ title: `executed an asynchonous test`, success: false, error: 'failed' })
    return false
  }

  return result.every(x => x.success)
}

function defaultReport({ title, success, error }) {
  /* istanbul ignore else */
  if (success) console.log(`${title} - ✓`)
  else console.error(`${title} - ${error}`)
}

function reportWith(report, f) {
  return async (...args) => {
    const result = await f(...args)
    report(result)
    return result
  }
}

async function executeSpec([title, f, report]) {
  const { numTasks, queue: { count = 1, options = undefined } = {}, process, test } = f()
  const tasksRef = root.push().ref
  let reportedError = false
  const reportError = e => { reportedError = e }
  const data = [...Array(numTasks).keys()].map(index => ({ index }))
  const processed = []
  function addProcessed(x) { processed[x.index] = x } // this one should not be async
  let processedSynchronously = false
  const processTask = (x, ...args) => {
    try {
      const y = (process && process(x, ...args)) || {}

      if (y.then && y.catch) {
        return y
          .then(({ processed, result }) => {
            addProcessed(processed)
            return result
          })
          .catch(e => {
            addProcessed(x)
            throw e
          })
      } else {
        processedSynchronously = true
        const { processed = x, result } = y
        addProcessed(processed)
        return result
      }
    } catch (e) {
      addProcessed(x)
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
    const [testSuccess, testError] = ops.execute(await test(data, processed, remaining))
    const success = testSuccess && !reportedError
    const error = reportedError || (!testSuccess && testError())
    return { title, success, processedSynchronously, error }
  } catch (e) {
    const error = reportedError || (e === TIMEOUT ? `timed out` : e)
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
          if (await f()) resolve()
          else if (Date.now() - start > timeout) reject(TIMEOUT)
          else check()
        },
        10
      )
    }

  })
}
