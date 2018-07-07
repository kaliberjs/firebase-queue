const { wait } = require('./machinery/promise_utils')

module.exports = rootRef => [
  [`default options - process a task and remove it from the queue`, {
    numTasks: 1,
    test: (data, processed, remaining) => [
      [processed, `equal`, data], `and`, [remaining, `equal`, []]
    ]
  }],

  [`default options - processing multiple tasks expecting them to be handled by the same worker`, () => {
    const workers = []
    const owners = []

    return {
      numTasks: 4,
      process: async (x, { snapshot }) => {
        await wait(20) // simulate long running processes to give other workers a chance
        const owner = snapshot.child(`_owner`).val()
        const [queueId, workerIndex] = owner.split(`:`)
        workers.push(queueId + workerIndex)
        owners.push(owner)
      },
      test: (data, processed, remaining) => [
        [processed, `equal`, data],
        `and`,
        [remaining, `equal`, []],
        `and`,
        [workers, `equal`, Array(workers.length).fill(workers[0])],
        `and`,
        [owners, `noDuplicates`]
      ]
    }
  }],

  [`default options - failed to process a task`, {
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
  }],

  [`multiple queues with multiple workers - processing the same set of tasks`, () => {
    const changes = [...Array(8).keys()].map(_ => [])
    const tasksRef = rootRef.push().ref
    tasksRef.on('child_added', x => { changes[x.val().index].push(`add`) })
    tasksRef.on('child_changed', x => { changes[x.val().index].push(`change`) })
    tasksRef.on('child_removed', x => { changes[x.val().index].push(`remove`) })
    return {
      numTasks: 8,
      queue: { tasksRef, count: 2, options: { numWorkers: 2 } },
      test: async (data, processed, remaining) => {
        tasksRef.off()
        const expectedChanges = changes.map(_ => [`add`, `change`, `remove`])
        return [
          [processed, `equal`, data],
          `and`,
          [remaining, `equal`, []],
          `and`,
          [changes, `equal`, expectedChanges],
        ]
      }
    }
  }],

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

  [`spec with finished state - leave tasks in queue with correct state`, {
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
  }],

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

  [`complex processing - setProgress, access reference and replace task`, {
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
  }],

  [`complex processing - setProgress after task was removed`, {
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
  }],

  [`weird stuff - changes in the matrix #1 (changing the owner)`, {
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
    expectReportedErrors: ([e1, e2]) =>
      !(e1 && e1.message.includes(`resolve`) && e2 && e2.message.includes(`reject`)) &&
      /* istanbul ignore next */`Expected problems with resolving and rejecting to be reported`,
  }],

  [`weird stuff - changes in the matrix #2 (changing the state)`, {
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
    expectReportedErrors: ([e1, e2]) =>
      !(e1 && e1.message.includes(`resolve`) && e2 && e2.message.includes(`reject`)) &&
      /* istanbul ignore next */`Expected problems with resolving and rejecting to be reported`,
  }],

  [`custom spec - custom 'inProgressState', 'startState', 'finishedState' and 'errorState'`, () => {
    const inProgressStates = []

    return {
      numTasks: 3,
      createTask: index => ({ index, _state: index < 2 ? `i should start` : null }),
      queue: { options: {
        spec: {
          startState: `i should start`,
          inProgressState: `i am in progress`,
          finishedState: `i am finished`,
          errorState: `i have failed`,
        }
      }},
      expectedNumProcessed: 2,
      process: async ({ index }, { snapshot, setProgress }) => {
        inProgressStates.push(snapshot.child(`_state`).val())
        await setProgress(50)
        if (index === 1) throw new Error(`custom error`)
      },
      test: (data, processed, remaining) => {
        const normalizedRemaining = remaining.map(setFieldPresence(`_error_details`, `_state_changed`))
        const normalizedData = data.map(x => ({
          ...x,
          _state: (x.index === 0 && `i am finished`) ||
                  (x.index === 1 && `i have failed`) ||
                  (x.index === 2 && undefined),
          _progress: (x.index === 0 && 100) ||
                     (x.index === 1 && 50) ||
                     (x.index === 2 && undefined),
          _state_changed: x.index < 2,
          _error_details: x.index === 1,
        }))

        return [
          [processed.filter(Boolean).map(addFields({ _state: `i should start` })), `equal`, data.slice(0, 2)],
          `and`,
          [normalizedRemaining, `equal`, normalizedData],
          `and`,
          [[...new Set(inProgressStates)], `equal`, [`i am in progress`]]
        ]
      }
    }
  }],
]

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
      } else return { [y]: x[y] !== undefined }
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