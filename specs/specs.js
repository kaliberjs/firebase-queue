const firebase = require(`firebase`)
const TIMEOUT = Symbol(`timeout`)
const Queue = require('../src/queue')

const app = firebase.initializeApp({
  // apiKey: `api key not needed`,
  databaseURL: `ws://localhost:5000`,
})
const db = app.database()

const specs = [
  [`default settings - process a task and remove it from the queue`, () => ({
    numTasks: 1,
    test: (data, processed, remaining) => [
      [processed, `equal`, data], `and`, [remaining, `equal`, []]
    ]
  })],

  [`multiple queues with multiple workers - processing the same set of tasks`, () => {
    return {
      numTasks: 4,
      queue: { count: 2, options: { numWorkers: 2 } },
      test: async (data, processed, remaining) => [
        [processed, `equal`, data], `and`, [remaining, `equal`, []]
      ]
    }
  }],

  [`multiple queues with multiple workers - distribute the work`, () => {
    const owners = []

    return {
      numTasks: 4,
      queue: { count: 2, options: { numWorkers: 2, sanitize: false } },
      process: async ({ index, _owner }) => {
        await wait(20) // simulate long running processes to give other workers a chance
        owners.push(_owner.slice(0, _owner.length - 2))
        return { index }
      },
      test: (data, processed, remaining) => [
        [processed, `equal`, data],
        `and`,
        [remaining, `equal`, []],
        `and`,
        [owners, `noDuplicates`],
      ]
    }
  }]
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
      const processTask = async x => {
        const y = process ? await process(x) : x
        processed[y.index] = y
      }
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

  return result.every(x => x)
}

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
