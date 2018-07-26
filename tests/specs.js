const { wait, waitFor } = require('./machinery/promise_utils')

module.exports = ({ rootRef, timeout }) => [
  [`default options - process a task and remove it from the queue`, {
    test: test(processedAll, noRemaining)
  }],

  [`default options - processing multiple tasks expecting them to be handled by the same worker`, () => {
    const workers = []
    return {
      numTasks: 4,
      process: async (_, { snapshot }) => {
        await wait(timeout * 0.1) // simulate long running processes to give other workers a chance
        const [queueId, workerIndex] = snapshot.child(`_owner`).val().split(`:`)
        workers.push(queueId + workerIndex)
      },
      test: test(processedAll, noRemaining, [workers, `sameValues`])
    }
  }],

  [`default options - processing multiple tasks expecting them to have a different owner`, () => {
    const owners = []
    return {
      numTasks: 4,
      process: (_, { snapshot }) => { owners.push(snapshot.child(`_owner`).val()) },
      test: test(processedAll, noRemaining, [owners, `noDuplicates`])
    }
  }],

  [`default options - failed to process a task - custom error`, {
    process: _ => { throw new Error(`custom error`) },
    test: test(processedAll, remainingErrors({
      _error_details: { error: `custom error`, error_stack: true } }
    ))
  }],

  [`default options - failed to process a task - string error`, {
    process: _ => { throw `custom error` },
    test: test(processedAll, remainingErrors({
      _error_details: { error: `custom error`, error_stack: false }
    }))
  }],

  [`default options - failed to process a task - object error`, {
    process: _ => { throw { toString: () => `custom error` } },
    test: test(processedAll, remainingErrors({
      _error_details: { error: `custom error`, error_stack: false }
    }))
  }],

  [`default options - failed to process a task - null error`, {
    process: _ => { throw null },
    test: test(processedAll, remainingErrors({ _error_details: false }))
  }],

  [`default options - failed to process a task - undefined error`, {
    process: _ => { throw undefined },
    test: test(processedAll, remainingErrors({ _error_details: false }))
  }],

  [`multiple queues with multiple workers - processing the same set of tasks, no intermediate states`, () => {
    const changes = [...Array(8).keys()].map(_ => [])
    const tasksRef = rootRef.push().ref
    tasksRef.on('child_added',   x => { changes[x.val().index].push(`add`) })
    tasksRef.on('child_changed', x => { changes[x.val().index].push(`change`) })
    tasksRef.on('child_removed', x => { changes[x.val().index].push(`remove`) })
    return {
      numTasks: 8,
      queue: { tasksRef, count: 2, options: { numWorkers: 2 } },
      test: test(processedAll, noRemaining, () => {
        tasksRef.off()
        return [changes, `equal`, Array(8).fill([`add`, `change`, `remove`])]
      })
    }
  }],

  [`multiple queues with multiple workers - distribute the work`, () => {
    const workers = []
    return {
      numTasks: 4,
      queue: { count: 2, options: { numWorkers: 2 } },
      process: async (_, { snapshot }) => {
        await wait(timeout * 0.1) // simulate long running processes to give other workers a chance
        const [queueId, workerIndex] = snapshot.child(`_owner`).val().split(`:`)
        workers.push(queueId + workerIndex)
      },
      test: test(processedAll, noRemaining, [workers, `noDuplicates`])
    }
  }],

  [`spec with finished state - leave tasks in queue with correct state`, {
    queue: { options: { spec: { finishedState: `finished` } } },
    test: test(processedAll, ({ tasks, remaining }) => {
      const normalizedRemaining = remaining.map(setFieldPresence(`_state_changed`))
      const normalizedData = tasks.map(addFields({ _progress: 100, _state: `finished`, _state_changed: true }))
      return [normalizedRemaining, `equal`, normalizedData]
    })
  }],

  [`complex processing - allow process function to access queue properties`, () => {
    const snapshots = []
    return {
      process: (_, { snapshot }) => { snapshots.push(snapshot.val()) },
      test: test(processedAll, noRemaining,
        [snapshots, `haveFields`, [`index`, `_state`, `_state_changed`, `_progress`, `_owner`]]
      )
    }
  }],

  [`complex processing - setProgress`, () => {
    const progresses = []
    return {
      process: async (_, { snapshot, setProgress }) => {
        await setProgress(88)
        progresses.push((await snapshot.ref.child(`_progress`).once(`value`)).val())
      },
      test: test(processedAll, noRemaining, [progresses, `equal`, [88]])
    }
  }],

  [`complex processing - replace task`, {
    process: _ => ({ _state: `do not process again` }),
    test: test(processedAll, ({ remaining }) =>
      [remaining, `equal`, [{ _state: `do not process again` }]]
    )
  }],

  [`weird stuff - setProgress when the state or owner is changed`, () => {
    const errors = []
    const targetProps = [`_state`, `_owner`]
    return {
      numTasks: 2,
      process: async ({ index }, { snapshot, setProgress }) => {
        const target = snapshot.child(targetProps[index])
        await target.ref.set(`this got changed`)
        try { await setProgress(88) }
        catch (e) { errors.push(e) }
        await target.ref.set(target.val())
      },
      test: test(processedAll, noRemaining, () => [errors.length, `equal`, 2])
    }
  }],

  [`weird stuff - resolving and rejecting when owner is changed`, {
    numTasks: 2,
    process: async ({ index }, { snapshot }) => {
      await snapshot.ref.child(`_owner`).set(`this got changed`)
      if (index) throw new Error('oops')
    },
    test: test(processedAll, ({ tasks, remaining }) => {
      const normalizedRemaining = remaining.map(setFieldPresence(`_state_changed`))
      const normalizedData = tasks.map(addFields({
        _owner: `this got changed`,
        _progress: 0,
        _state: `in_progress`,
        _state_changed: true,
      }))
      return [normalizedRemaining, `equal`, normalizedData]
    }),
    expectReportedErrors: expectResolveAndRejectErrors,
  }],

  [`weird stuff - resolving and rejecting when state is changed`, {
    numTasks: 2,
    process: async ({ index }, { snapshot }) => {
      await snapshot.ref.child(`_state`).set(`this got changed`)
      if (index) throw new Error('oops')
    },
    test: test(processedAll, ({ tasks, remaining }) => {
      const normalizedRemaining = remaining.map(setFieldPresence(`_owner`, `_state_changed`))
      const normalizedData = tasks.map(addFields({
        _owner: true,
        _progress: 0,
        _state: `this got changed`,
        _state_changed: true,
      }))
      return [normalizedRemaining, `equal`, normalizedData]
    }),
    expectReportedErrors: expectResolveAndRejectErrors,
  }],

  [`custom spec - custom 'errorState'`, {
    queue: { options: { spec: { errorState: `i have failed` } } },
    process: async _ => { throw new Error(`custom error`) },
    test: test(processedAll, ({ tasks, remaining }) => {
      const normalizedRemaining = remaining.map(setFieldPresence(`_error_details`, `_state_changed`))
      const normalizedData = tasks.map(addFields({
        _state: `i have failed`,
        _progress: 0,
        _state_changed: true,
        _error_details: true,
      }))

      return [normalizedRemaining, `equal`, normalizedData]
    })
  }],

  [`custom spec - custom 'finishedState'`, {
    queue: { options: { spec: { finishedState: `i am finished` } } },
    test: test(processedAll, ({ tasks, remaining }) => {
      const normalizedRemaining = remaining.map(setFieldPresence(`_state_changed`))
      const normalizedData = tasks.map(addFields({
        _state: `i am finished`,
        _progress: 100,
        _state_changed: true,
      }))
      return [normalizedRemaining, `equal`, normalizedData]
    })
  }],

  [`custom spec - custom 'finishedState' set when returning data without a '_state'`, {
    queue: { options: { spec: { finishedState: `i am finished` } } },
    process: x => ({ key: 'value' }),
    test: test(processedAll, ({ remaining }) => {
      const normalizedRemaining = remaining.map(setFieldPresence(`_state_changed`))
      const expectedRemaining = [{
         key: 'value',
        _state: `i am finished`,
        _progress: 100,
        _state_changed: true,
      }]
      return [normalizedRemaining, `equal`, expectedRemaining]
    })
  }],

  [`custom spec - custom 'startState'`, {
    numTasks: 2,
    createTask: index => index ? { index } : { index, _state: `i should start` },
    queue: { options: { spec: { startState: `i should start` } } },
    expectedNumProcessed: 1,
    test: ({ tasks, processed, remaining }) => [
      [processed.slice(1), `equal`, []], `and`, [remaining, `equal`, tasks.slice(1)],
      `and`,
      [processed.slice(0, 1).map(addFields({ _state: `i should start` })), `equal`, tasks.slice(0, 1)]
    ]
  }],

  [`custom spec - custom 'startState' and replacing the task`, {
    numTasks: 1,
    createTask: index => ({ index, _state: `i should start` }),
    queue: { options: { spec: { startState: `i should start` } } },
    process: x => x,
    test: ({ tasks, processed, remaining }) => [
      [remaining, `equal`, tasks.map(({ _state, ...x }) => x)],
      `and`,
      [processed, `equal`, remaining]
    ]
  }],

  [`custom spec - custom 'inProgressState'`, () => {
    const inProgressStates = []
    return {
      queue: { options: { spec: { inProgressState: `i am in progress` } } },
      process: async (_, { snapshot }) => { inProgressStates.push(snapshot.child(`_state`).val()) },
      test: test(processedAll, noRemaining, [inProgressStates, `equal`, [`i am in progress`]])
    }
  }],
]

function addFields(o) { return x => ({ ...o, ...x }) }
function setFieldPresence(...fields) {
  return x => {
    const changes = fields.map(y => {
      if (Array.isArray(y)) {
        const [key, rest] = y
        const value = x[key]
        return { [key]: value !== undefined && setFieldPresence(...rest)(value) }
      } else return { [y]: x[y] !== undefined }
    })

    return Object.assign({}, x, ...changes)
  }
}

function test(check, ...checks) {
  return data => {
    const result = checks.reduce(
      (result, check) => [...result, `and`, asTest(check)],
      [asTest(check)]
    )
    return result

    function asTest(x) { return typeof x === 'function' ? x(data) : x }
  }
}
function processedAll({ tasks, processed }) { return [processed, `equal`, tasks] }
function noRemaining({ remaining }) { return [remaining, `equal`, []] }
function remainingErrors({ _error_details }) {
  const fieldPresence = setFieldPresence([`_error_details`, [`error_stack`]], `_state_changed`)

  return ({ tasks, remaining }) => {
    const normalizedRemaining = remaining.map(fieldPresence)
    const normalizedData = tasks.map(addFields({
      _error_details,
      _progress: 0,
      _state: `error`,
      _state_changed: true,
    }))
    return [normalizedRemaining, `equal`, normalizedData]
  }
}

function expectResolveAndRejectErrors([e1, e2]) {
  const expectedErrors = e1 && e1.message.includes(`resolve`) && e2 && e2.message.includes(`reject`)
  return !expectedErrors &&
    /* istanbul ignore next */`Expected problems with resolving and rejecting to be reported`
}
