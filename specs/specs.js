const firebase = require(`firebase`)
const TIMEOUT = Symbol(`timeout`)
const Queue = require('../src/queue')

const app = firebase.initializeApp({
  // apiKey: `api key not needed`,
  databaseURL: `ws://localhost:5000`,
})
const db = app.database()

const specs = [
  [`default options - process a task and remove it from the queue`, () => ({
    numTasks: 1,
    test: (data, processed, remaining) => [
      [processed, `equal`, data], `and`, [remaining, `equal`, []]
    ]
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
        const [queueId, workerIndex] = _owner.split(':')
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
    queue: { options: { spec: { finishedState: 'finished' } } },
    test: (data, processed, remaining) => {
      const remainingWithoutStateChanged = remaining.map(({ _state_changed, ...x }) => x)
      const dataWithFinishedState = data.map(x => ({ _progress: 100, _state: 'finished', ...x }))
      return [
        [processed, `equal`, data],
        `and`,
        [remainingWithoutStateChanged, `equal`, dataWithFinishedState]
      ]
    }
  })],

  ['unsanitized tasks - allow process function to access queue properties', () => ({
    numTasks: 1,
    queue: { options: { sanitize: false } },
    test: (data, processed, remaining) => {
      const normalizedProcessed = processed.map(x => ({
        ...x,
        _state: notUndefined(x._state),
        _state_changed: notUndefined(x._state_changed),
        _progress: notUndefined(x._progress),
        _owner: notUndefined(x._owner),
      }))
      const normalizedData = data.map(x => ({
        _owner: true, _progress: true, _state: true, _state_changed: true, ...x
      }))
      return [
        [normalizedProcessed, `equal`, normalizedData], `and`, [remaining, `equal`, []]
      ]
    }
  })],

  ['complex processing - setProgress, access reference and replace task', () => ({
    numTasks: 1,
    process: async (x, { ref, setProgress }) => {
      await setProgress(88)
      const { _progress } = (await ref.once('value')).val()
      const result = { ...x, progress: _progress, _state: 'do not process again' }
      return { processed: x, result }
    },
    test: (data, processed, remaining) => {
      const dataWithProgressAndState = data.map(x => ({ _state: 'do not process again', ...x, progress: 88 }))
      return [
        [processed, `equal`, data],
        `and`,
        [remaining, `equal`, dataWithProgressAndState]
      ]
    }
  })]
]

const ops = {
  execute: ([a, op, ...b]) => {
    const f = ops[op]
    if (!f) console.error(`Could not find operation with name ${op}`)
    return f(a, ...b)
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

execute(specs)
  .then(x => {
    if (!x) process.exitCode = 1
  })
  .catch(e => {
    console.error(e)
    process.exitCode = 1
  })

async function execute(specs) {
  db.goOnline()
  const root = db.ref()

  let processedSynchronously = false

  const result = await sequence(specs,
    async ([title, f]) => {
      const { numTasks, queue: { count = 1, options = undefined } = {}, process, test } = f()
      const tasksRef = root.push().ref
      const reportError = e => {
        console.error(e)
        throw e
      }
      const data = [...Array(numTasks).keys()].map(index => ({ index }))
      const processed = []
      function addProcessed(x) { processed[x.index] = x } // this one should not be async
      processedSynchronously = processedSynchronously || !process
      const processTask = process
        ? async (x, ...args) => {
            const { processed, result } = process ? await process(x, ...args) : x
            addProcessed(processed)
            return result
          }
        : addProcessed
      const queues = [...Array(count)].map(_ => new Queue({ tasksRef, processTask, reportError, options }))
      const shutdown = async () => Promise.all(queues.map(x => x.shutdown()))
      try {
        await Promise.all(data.map(x => tasksRef.push(x)))
        await waitFor(() => processed.filter(Boolean).length === data.length, { timeout: 500 })
        await shutdown()
        const remaining = Object.values((await tasksRef.once('value')).val() || {})
        const [success, error] = ops.execute(await test(data, processed, remaining))
        if (success) console.log(title, `-`, `✓`)
        else console.error(title, `-`, error())
        return success
      } catch (e) {
        if (e === TIMEOUT) {
          console.error(title, `-`, `timed out`)
          console.error(`test result:`)
          console.error(JSON.stringify(test(), null, 2))
        }
        else console.error(title, `-`, e)
        return false
      } finally {
        await shutdown()
      }
    })

  db.goOffline()

  if (!processedSynchronously) {
    console.error('Did not test synchronous `processTask` functions')
    return false
  }

  return result.every(x => x)
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
