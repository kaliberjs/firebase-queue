'use strict';

const _ = require('lodash')
const Helpers = require('../helpers')
const chai = require('chai')
const sinon = require('sinon')

chai
  .use(require('sinon-chai'))
  .use(require('chai-as-promised'))
  .should()

const { expect } = chai

const th = new Helpers()
const { now, serverNow, allways, sideEffect, withTasksRef, withQueueWorkerFor, validBasicTaskSpec, validTaskSpecWithFinishedState, chain, pushTasks, waitForState, waitForStates, echo, nonPlainObjects } = th
const tasksRef = th.tasksRef
const _tasksRef = tasksRef

const nonBooleans             = ['', 'foo', NaN, Infinity,              0, 1, ['foo', 'bar'], { foo: 'bar' }, null,            { foo: { bar: { baz: true } } }, _.noop                ]
const nonStrings              = [           NaN, Infinity, true, false, 0, 1, ['foo', 'bar'], { foo: 'bar' }, null,            { foo: { bar: { baz: true } } }, _.noop                ]
const nonFunctions            = ['', 'foo', NaN, Infinity, true, false, 0, 1, ['foo', 'bar'], { foo: 'bar' }, null,            { foo: { bar: { baz: true } } }                        ]
const nonPositiveIntegers     = ['', 'foo', NaN, Infinity, true, false, 0,    ['foo', 'bar'], { foo: 'bar' },                  { foo: { bar: { baz: true } } }, _.noop, -1, 1.1       ]
const invalidPercentageValues = ['', 'foo', NaN, Infinity, true, false,       ['foo', 'bar'], { foo: 'bar' },                  { foo: { bar: { baz: true } } }, _.noop, -1,      100.1]
const invalidTaskSpecs        = ['', 'foo', NaN, Infinity, true, false, 0, 1, ['foo', 'bar'], { foo: 'bar' }, null, undefined, { foo: { bar: { baz: true } } }, _.noop, -1            ]

const nonStringsWithoutNull = nonStrings.filter(x => x !== null)
const nonPositiveIntegersWithout0 = nonPositiveIntegers.filter(x => x !== 0)

describe('QueueWorker', () => {
  
  describe('initialize', () => {

    function initialize({ tasksRef = _tasksRef, processId = '0', sanitize = true, suppressStack = false, processingFunction = _.noop } = {}) {
      return new th.QueueWorker(tasksRef, processId, sanitize, suppressStack, processingFunction)
    }

    // we should probably replace the `throws(strings)` with `throws(CONSTANT)`

    it('should not create a QueueWorker with no parameters', () => 
      expect(() => new th.QueueWorker()).to.throw('No tasks reference provided.')
    )

    it('should not create a QueueWorker with only a tasksRef', () =>
      expect(() => new th.QueueWorker(tasksRef)).to.throw('Invalid process ID provided.')
    )

    it('should not create a QueueWorker with only a tasksRef, process ID, sanitize and suppressStack option', () =>
      expect(() => new th.QueueWorker(tasksRef, '0', true, false)).to.throw('No processing function provided.')
    )

    it('should not create a QueueWorker with a tasksRef, processId, sanitize option and an invalid processing function', () => 
      nonFunctions.forEach(nonFunctionObject =>
        expect(() => initialize({ processingFunction: nonFunctionObject })).to.throw('No processing function provided.')
      )
    )

    it('should create a QueueWorker with a tasksRef, processId, sanitize option and a processing function', () =>
      expect(() => initialize()).to.not.throw(Error)
    )

    it('should not create a QueueWorker with a non-string processId specified', () => 
      nonStrings.forEach(nonStringObject =>
        expect(() => initialize({ processId: nonStringObject })).to.throw('Invalid process ID provided.')
      )
    )

    it('should not create a QueueWorker with a non-boolean sanitize option specified', () =>
      nonBooleans.forEach(nonBooleanObject =>
        expect(() => initialize({ sanitize: nonBooleanObject })).to.throw('Invalid sanitize option.')
      )
    )

    it('should not create a QueueWorker with a non-boolean suppressStack option specified', () =>
      nonBooleans.forEach(nonBooleanObject =>
        expect(() => initialize({ suppressStack: nonBooleanObject })).to.throw('Invalid suppressStack option.')
      )
    )
  })

  describe('# Resetting tasks', () => {

    it('should reset a task when another task is currently being processed', () =>
      withTasksRef(tasksRef => 
        withQueueWorkerFor(tasksRef, echo, qw => {
          qw.setTaskSpec(validTaskSpecWithFinishedState)
          const { startState, inProgressState, finishedState } = validTaskSpecWithFinishedState
          return chain(
            pushTasks(tasksRef, { task: 1 }, { task: 2 }),
            ([task1, task2]) => waitForState([task1, task2], inProgressState),
            ([_, task2]) => waitForState(task2, startState),
            task2 => {
              const task = task2.val()
              expect(task).to.not.have.any.keys('_owner', '_progress', '_state')
              expect(task).to.have.property('task').that.equals(2)
              expect(task).to.have.property('_state_changed').that.is.closeTo(serverNow(), 250)
            }
          )
        })
      )
    )
  })

  describe('#_resetTask', () => {
    let qw
    let testRef

    beforeEach(() => {
      qw = new th.QueueWorker(tasksRef, '0', true, false, _.noop)
    })

    afterEach(done => {
      testRef.off()
      qw.shutdown()
        .then(_ => tasksRef.set(null))
        .then(done)
    })

    function resetTask({ task, spec = th.validBasicTaskSpec }) {
      qw._setTaskSpec(spec)
      testRef = tasksRef.push()
      return testRef.set(task)
        .then(_ => qw._resetTask(testRef))
        .then(_ => testRef.once('value'))
    }

    it('should reset a task that is currently in progress', () => {
      return resetTask({
        task: {
          '_state': th.validBasicTaskSpec.inProgressState,
          '_state_changed': now(),
          '_owner': qw._currentId()
        }
      }).then(snapshot => {
          const task = snapshot.val()
          expect(task).to.not.have.any.keys('_owner', '_progress', '_state')
          expect(task).to.have.property('_state_changed').that.is.closeTo(serverNow(), 250)
        })
    })

    it('should not reset a task if it is no longer owned by current worker', () => {
      const task = {
        '_state': th.validBasicTaskSpec.inProgressState,
        '_state_changed': now(),
        '_owner': 'someone-else'
      }
      return resetTask({ task })
        .then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
    })

    it('should not reset a task that no longer exists', () => {
      return resetTask({ task: null })
        .then(snapshot => {
          expect(snapshot.val()).to.be.null
        })
    })  

    it('should not reset a task if it is has already changed state', () => {
      const task = {
        '_state': th.validTaskSpecWithFinishedState.finishedState,
        '_state_changed': now(),
        '_owner': qw._currentId()
      }
      return resetTask({ task, spec: th.validTaskSpecWithFinishedState })
        .then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
    })

    it('should not reset a task if it is has no state', () => {
      const task = {
        '_state_changed': now(),
        '_owner': qw._currentId()
      }
      return resetTask({ task, spec: th.validTaskSpecWithFinishedState })
        .then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
    })
  })

  describe('#_resetTaskIfTimedOut', () => {
    let qw
    let testRef

    beforeEach(() => {
      qw = new th.QueueWorker(tasksRef, '0', true, false, _.noop)
    })

    afterEach(done => {
      testRef.off()
      qw.shutdown()
        .then(_ => tasksRef.set(null))
        .then(done)
    })

    function resetTaskIfTimedOut({ task, spec = th.validBasicTaskSpec }) {
      qw._setTaskSpec(spec)
      testRef = tasksRef.push()
      return testRef.set(task)
        .then(_ => qw._resetTaskIfTimedOut(testRef))
        .then(_ => testRef.once('value'))
    }

    it('should not reset a task if it is has changed state recently', () => {
      const task = {
        '_state': th.validBasicTaskSpec.inProgressState,
        '_state_changed': now(),
        '_owner': 'someone'
      }
      return resetTaskIfTimedOut({ task })
        .then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
    })

    it('should reset a task that is currently in progress that has timed out', () => {
      return resetTaskIfTimedOut({
        task: {
          '_state': th.validBasicTaskSpec.inProgressState,
          '_state_changed': now() - th.validTaskSpecWithTimeout.timeout,
          '_owner': 'someone'
        },
        spec: th.validTaskSpecWithTimeout
      }).then(snapshot => {
          var task = snapshot.val()
          // we should probably check _state, _owner and _progress
          expect(task).to.have.all.keys(['_state_changed'])
          expect(task._state_changed).to.be.closeTo(serverNow(), 250)
        })
    })
  })

  describe('#_resolve', () => {
    let qw
    let testRef

    beforeEach(() => {
      qw = new th.QueueWorkerWithoutProcessing(tasksRef, '0', true, false, _.noop)
    })

    afterEach(done => {
      testRef.off()
      qw.shutdown()
        .then(_ => tasksRef.set(null))
        .then(done)
    })

    function resolve({ task, taskNumber = qw._taskNumber(), newTask, spec = th.validTaskSpecWithFinishedState }) {
      qw._setTaskSpec(spec)
      testRef = tasksRef.push()
      return testRef.set(task)
        .then(_ => qw._resolve(testRef, taskNumber)[0](newTask))
        .then(_ => testRef.once('value'))
    }

    it('should resolve a task owned by the current worker and remove it when no finishedState is specified', () => {
      return resolve({
        task: {
          '_state': th.validBasicTaskSpec.inProgressState,
          '_owner': qw._currentId()
        },
        spec: th.validBasicTaskSpec
      }).then(snapshot => {
          expect(snapshot.val()).to.be.null
        })
    })

    it('should resolve a task owned by the current worker and change the state when a finishedState is specified and no object passed', () => {
      return resolve({
        task: {
          '_state': th.validTaskSpecWithFinishedState.inProgressState,
          '_owner': qw._currentId()
        }
      }).then(snapshot => {
          var task = snapshot.val()
          expect(task).to.have.all.keys(['_state', '_state_changed', '_progress'])
          expect(task._progress).to.equal(100)
          expect(task._state).to.equal(th.validTaskSpecWithFinishedState.finishedState)
          expect(task._state_changed).to.be.closeTo(serverNow(), 250)
        })
    })

    nonPlainObjects.forEach(nonPlainObject => 
      it('should resolve an task owned by the current worker and change the state when a finishedState is specified and an invalid object ' + nonPlainObject + ' passed', () => {
        return resolve({
          task: {
            '_state': th.validTaskSpecWithFinishedState.inProgressState,
            '_owner': qw._currentId()
          },
          newTask: nonPlainObject
        }).then(snapshot => {
            var task = snapshot.val()
            expect(task).to.have.all.keys(['_state', '_state_changed', '_progress'])
            expect(task._progress).to.equal(100)
            expect(task._state).to.equal(th.validTaskSpecWithFinishedState.finishedState)
            expect(task._state_changed).to.be.closeTo(serverNow(), 250)
          })
      })
    )

    it('should resolve a task owned by the current worker and change the state when a finishedState is specified and a plain object passed', () => {
      return resolve({
        task: {
          '_state': th.validTaskSpecWithFinishedState.inProgressState,
          '_owner': qw._currentId()
        },
        newTask: { foo: 'bar' }
      }).then(snapshot => {
          var task = snapshot.val()
          expect(task).to.have.all.keys(['_state', '_state_changed', '_progress', 'foo'])
          expect(task._progress).to.equal(100)
          expect(task._state).to.equal(th.validTaskSpecWithFinishedState.finishedState)
          expect(task._state_changed).to.be.closeTo(serverNow(), 250)
          expect(task.foo).to.equal('bar')
        })
    })

    it('should resolve a task owned by the current worker and change the state to a provided valid string _new_state', () => {
      return resolve({
        task: {
          '_state': th.validTaskSpecWithFinishedState.inProgressState,
          '_owner': qw._currentId()
        },
        newTask: {
          foo: 'bar',
          _new_state: 'valid_new_state'
        }
      }).then(snapshot => {
          var task = snapshot.val();
          expect(task).to.have.all.keys(['_state', '_state_changed', '_progress', 'foo'])
          expect(task._progress).to.equal(100)
          expect(task._state).to.equal('valid_new_state')
          expect(task._state_changed).to.be.closeTo(serverNow(), 250)
          expect(task.foo).to.equal('bar')
          // _new_state should be gone right?
        })
    })

    it('should resolve a task owned by the current worker and change the state to a provided valid null _new_state', () => {
      return resolve({
        task: {
          '_state': th.validTaskSpecWithFinishedState.inProgressState,
          '_owner': qw._currentId()
        },
        newTask: {
          foo: 'bar',
          _new_state: null
        }
      }).then(snapshot => {
          var task = snapshot.val()
          expect(task).to.have.all.keys(['_state_changed', '_progress', 'foo'])
          expect(task._progress).to.equal(100)
          expect(task._state_changed).to.be.closeTo(serverNow(), 250)
          expect(task.foo).to.equal('bar')
        })
    })

    it('should resolve a task owned by the current worker and remove the task when provided _new_state = false', () => {
      return resolve({
        task: {
          '_state': th.validTaskSpecWithFinishedState.inProgressState,
          '_owner': qw._currentId()
        },
        newTask: {
          foo: 'bar',
          _new_state: false
        }
      }).then(snapshot => {
          expect(snapshot.val()).to.be.null
        })
    })

    it('should resolve a task owned by the current worker and change the state to finishedState when provided an invalid _new_state', () => {
      return resolve({
        task: {
          '_state': th.validTaskSpecWithFinishedState.inProgressState,
          '_owner': qw._currentId()
        },
        newTask: {
          foo: 'bar',
          _new_state: {
            state: 'object_is_an_invalid_new_state'
          }
        }
      }).then(snapshot => {
          var task = snapshot.val()
          expect(task).to.have.all.keys(['_state', '_state_changed', '_progress', 'foo'])
          expect(task._progress).to.equal(100)
          expect(task._state).to.equal(th.validTaskSpecWithFinishedState.finishedState)
          expect(task._state_changed).to.be.closeTo(serverNow(), 250)
          expect(task.foo).to.equal('bar')
        })
    })

    it('should not resolve a task that no longer exists', () => {
      return resolve({
        task: null
      }).then(snapshot => {
          expect(snapshot.val()).to.be.null
        })
    })

    it('should not resolve a task if it is no longer owned by the current worker', () => {
      const task = {
        '_state': th.validTaskSpecWithFinishedState.inProgressState,
        '_owner': 'other_worker'
      };
      return resolve({
        task
      }).then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
    })

    it('should not resolve a task if it is has already changed state', () => {
      const task = {
        '_state': th.validTaskSpecWithFinishedState.finishedState,
        '_owner': qw._currentId()
      };
      return resolve({
        task
      }).then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
    })

    it('should not resolve a task if it is has no state', () => {
      const task = {
        '_owner': qw._processId + ':' + qw._taskNumber()
      }
      return resolve({
        task
      }).then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
    })

    it('should not resolve a task if a new task is being processed', () => {
      const task = {
        '_state': th.validTaskSpecWithFinishedState.inProgressState,
        '_owner': qw._currentId()
      }
      return resolve({
        taskNumber: qw._taskNumber() + 1,
        task
      }).then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
    })
  })

  describe('#_reject', () => {
    let qw
    let testRef

    beforeEach(() => {
      qw = new th.QueueWorker(tasksRef, '0', true, false, _.noop)
    })

    afterEach(done => {
      testRef.off()
      qw.shutdown()
        .then(_ => tasksRef.set(null))
        .then(done)
    })

    function reject({ task, taskNumber = qw._taskNumber(), error, spec = th.validBasicTaskSpec }) {
      qw._setTaskSpec(spec)
      testRef = tasksRef.push()
      return testRef.set(task)
        .then(_ => qw._reject(testRef, taskNumber)[0](error))
        .then(_ => testRef.once('value'))
    }

    it('should reject a task owned by the current worker', () => {
      return reject({
        task: {
          '_state': th.validBasicTaskSpec.inProgressState,
          '_owner': qw._currentId(),
          '_progress': 0
        }
      }).then(snapshot => {
          var task = snapshot.val()
          expect(task).to.have.all.keys(['_state', '_progress', '_state_changed', '_error_details'])
          expect(task._state).to.equal('error')
          expect(task._state_changed).to.be.closeTo(serverNow(), 250)
          expect(task._progress).to.equal(0)
          expect(task._error_details).to.have.all.keys(['previous_state', 'attempts'])
          expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState)
          expect(task._error_details.attempts).to.equal(1)
        })
    })

    it('should reject a task owned by the current worker and reset if more retries are specified', () => {
      return reject({
        task: {
          '_state': th.validTaskSpecWithRetries.inProgressState,
          '_owner': qw._currentId(),
          '_progress': 0,
          '_error_details': {
            'previous_state': th.validTaskSpecWithRetries.inProgressState,
            'attempts': 1
          }
        },
        spec: th.validTaskSpecWithRetries
      }).then(snapshot => {
          var task = snapshot.val()
          expect(task).to.have.all.keys(['_progress', '_state_changed', '_error_details'])
          expect(task._state_changed).to.be.closeTo(serverNow(), 250)
          expect(task._progress).to.equal(0)
          expect(task._error_details).to.have.all.keys(['previous_state', 'attempts'])
          expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState)
          expect(task._error_details.attempts).to.equal(2)
        })
    })

    it('should reject a task owned by the current worker and reset the attempts count if chaning error handlers', () => {
      return reject({
        task: {
          '_state': th.validTaskSpecWithRetries.inProgressState,
          '_owner': qw._currentId(),
          '_progress': 0,
          '_error_details': {
            'previous_state': 'other_in_progress_state',
            'attempts': 1
          }
        },
        spec: th.validTaskSpecWithRetries
      }).then(snapshot => {
          var task = snapshot.val()
          expect(task).to.have.all.keys(['_progress', '_state_changed', '_error_details'])
          expect(task._state_changed).to.be.closeTo(serverNow(), 250)
          expect(task._progress).to.equal(0)
          expect(task._error_details).to.have.all.keys(['previous_state', 'attempts'])
          expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState)
          expect(task._error_details.attempts).to.equal(1)
        })
    })

    it('should reject a task owned by the current worker and a non-standard error state', () => {
      return reject({
        task: {
          '_state': th.validBasicTaskSpec.inProgressState,
          '_owner': qw._currentId(),
          '_progress': 0
        },
        spec: th.validTaskSpecWithErrorState
      }).then(snapshot => {
          var task = snapshot.val()
          expect(task).to.have.all.keys(['_state', '_progress', '_state_changed', '_error_details'])
          expect(task._state).to.equal(th.validTaskSpecWithErrorState.errorState)
          expect(task._state_changed).to.be.closeTo(serverNow(), 250)
          expect(task._progress).to.equal(0)
          expect(task._error_details).to.have.all.keys(['previous_state', 'attempts'])
          expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState)
          expect(task._error_details.attempts).to.equal(1)
        })
    })

    nonStringsWithoutNull.forEach(nonStringObject =>
      it('should reject a task owned by the current worker and convert the error to a string if not a string: ' + nonStringObject, () => {
        return reject({
          task: {
            '_state': th.validBasicTaskSpec.inProgressState,
            '_owner': qw._currentId(),
            '_progress': 0
          },
          error: nonStringObject
        }).then(snapshot => {
            var task = snapshot.val()
            expect(task).to.have.all.keys(['_state', '_progress', '_state_changed', '_error_details'])
            expect(task._state).to.equal('error')
            expect(task._state_changed).to.be.closeTo(serverNow(), 250)
            expect(task._progress).to.equal(0)
            expect(task._error_details).to.have.all.keys(['previous_state', 'error', 'attempts'])
            expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState)
            expect(task._error_details.error).to.equal(nonStringObject.toString())
            expect(task._error_details.attempts).to.equal(1)
          })
      })
    )

    it('should reject a task owned by the current worker and append the error string to the _error_details', () => {
      const error = 'My error message'
      return reject({
        task: {
          '_state': th.validBasicTaskSpec.inProgressState,
          '_owner': qw._currentId(),
          '_progress': 0
        },
        error
      }).then(snapshot => {
          var task = snapshot.val()
          expect(task).to.have.all.keys(['_state', '_progress', '_state_changed', '_error_details'])
          expect(task._state).to.equal('error')
          expect(task._state_changed).to.be.closeTo(serverNow(), 250)
          expect(task._progress).to.equal(0)
          expect(task._error_details).to.have.all.keys(['previous_state', 'error', 'attempts'])
          expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState)
          expect(task._error_details.attempts).to.equal(1)
          expect(task._error_details.error).to.equal(error)
        })
    })

    it('should reject a task owned by the current worker and append the error string and stack to the _error_details', () => {
      const error = new Error('My error message')
      return reject({
        task: {
          '_state': th.validBasicTaskSpec.inProgressState,
          '_owner': qw._currentId(),
          '_progress': 0
        },
        error
      }).then(snapshot => {
          var task = snapshot.val()
          expect(task).to.have.all.keys(['_state', '_progress', '_state_changed', '_error_details'])
          expect(task._state).to.equal('error')
          expect(task._state_changed).to.be.closeTo(serverNow(), 250)
          expect(task._progress).to.equal(0)
          expect(task._error_details).to.have.all.keys(['previous_state', 'error', 'attempts', 'error_stack'])
          expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState)
          expect(task._error_details.attempts).to.equal(1)
          expect(task._error_details.error).to.equal(error.message)
          expect(task._error_details.error_stack).to.be.a.string
        })
    })

    it('should reject a task owned by the current worker and append the error string to the _error_details', () => {
      qw = new th.QueueWorker(tasksRef, '0', true, true, _.noop)
      qw._setTaskSpec(th.validBasicTaskSpec)
      const error = new Error('My error message')
      return reject({
        task: {
          '_state': th.validBasicTaskSpec.inProgressState,
          '_owner': qw._currentId(),
          '_progress': 0
        },
        error
      }).then(snapshot => {
          var task = snapshot.val()
          expect(task).to.have.all.keys(['_state', '_progress', '_state_changed', '_error_details'])
          expect(task._state).to.equal('error')
          expect(task._state_changed).to.be.closeTo(serverNow(), 250)
          expect(task._progress).to.equal(0)
          expect(task._error_details).to.have.all.keys(['previous_state', 'error', 'attempts'])
          expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState)
          expect(task._error_details.attempts).to.equal(1)
          expect(task._error_details.error).to.equal(error.message)
        })
    })

    it('should not reject a task that no longer exists', () => {
      return reject({ task: null, spec: th.validTaskSpecWithFinishedState })
        .then(snapshot => {
          expect(snapshot.val()).to.be.null
        })
    })

    it('should not reject a task if it is no longer owned by the current worker', () => {
      const task = {
        '_state': th.validTaskSpecWithFinishedState.inProgressState,
        '_owner': 'other_worker'
      }
      return reject({ task, spec: th.validTaskSpecWithFinishedState })
        .then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
    })

    it('should not reject a task if it is has already changed state', () => {
      const task = {
        '_state': th.validTaskSpecWithFinishedState.finishedState,
        '_owner': qw._currentId(),
        '_progress': 0
      }
      return reject({ task, spec: th.validTaskSpecWithFinishedState })
        .then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
    })

    it('should not reject a task if it is has no state', () => {
      const task = {
        '_owner': qw._currentId()
      }
      return reject({ task, spec: th.validTaskSpecWithFinishedState })
        .then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
    })

    it('should not reject a task if a new task is being processed', () => {
      const task = {
        '_state': th.validTaskSpecWithFinishedState.inProgressState,
        '_owner': qw._currentId()
      }
      return reject({ task, taskNumber: qw._taskNumber() + 1, spec: th.validTaskSpecWithFinishedState })
        .then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
    })
  })

  describe('#_updateProgress', () => {
    let qw
    let testRef
    
    beforeEach(() => {
      qw = new th.QueueWorker(tasksRef, '0', true, false, _.noop);
    })

    afterEach(done => {
      testRef.off()
      qw.shutdown()
        .then(_ => tasksRef.set(null))
        .then(done)
    })

    function updateProgress({ task, taskNumber = qw._taskNumber(), progress, spec = th.validBasicTaskSpec }) {
      qw._setTaskSpec(spec)
      testRef = tasksRef.push()
      return testRef.set(task)
        .then(_ => qw._updateProgress(testRef, taskNumber)(progress))
    }

    invalidPercentageValues.forEach(invalidPercentageValue => 
      it('should ignore invalid input ' + invalidPercentageValue + ' to update the progress', () => 
        updateProgress({ task: null, progress: invalidPercentageValue })
          .should.eventually.be.rejectedWith('Invalid progress')
      )
    )

    it('should not update the progress of a task no longer owned by the current worker', () => {
      return updateProgress({
        task: { 
          '_state': th.validBasicTaskSpec.inProgressState, 
          '_owner': 'someone_else' 
        },
        progress: 10
      }).should.eventually.be.rejectedWith('Can\'t update progress - current task no longer owned by this process')
    })

    it('should not update the progress of a task if the task is no longer in progress', () => {
      return updateProgress({
        task: { 
          '_state': th.validTaskSpecWithFinishedState.finishedState, 
          '_owner': qw._currentId() 
        },
        progress: 10,
        spec: th.validTaskSpecWithFinishedState
      }).should.eventually.be.rejectedWith('Can\'t update progress - current task no longer owned by this process')
    })

    it('should not update the progress of a task if the task has no _state', () => {
      return updateProgress({
        task: { 
          '_owner': qw._currentId() 
        },
        progress: 10
      }).should.eventually.be.rejectedWith('Can\'t update progress - current task no longer owned by this process')
    })

    it('should update the progress of the current task', () => {
      return updateProgress({
        task: { 
          '_state': th.validBasicTaskSpec.inProgressState, 
          '_owner': qw._currentId() 
        },
        progress: 10
      }).should.eventually.be.fulfilled
    })

    it('should not update the progress of a task if a new task is being processed', () => {
      return updateProgress({
        task: { 
          '_owner': qw._currentId() 
        },
        taskNumber: qw._taskNumber() + 1,
        progress: 10
      }).should.eventually.be.rejectedWith('Can\'t update progress - no task currently being processed')
    })
  })

  describe('#_tryToProcess', () => {
    let qw

    beforeEach(() => {
      qw = new th.QueueWorker(tasksRef, '0', true, false, _.noop);
    })

    afterEach(done => {
      qw.setTaskSpec();
      tasksRef.set(null, done);
    })

    /*
      Most of the specs here have extensive knowledge of the inner 
      workings of tryToProcess. We have to eventually fix that
    */

    it('should not try and process a task if busy', () => {
      qw._startState(th.validTaskSpecWithStartState.startState)
      qw._inProgressState(th.validTaskSpecWithStartState.inProgressState)
      qw._busy(true)
      qw._newTaskRef(tasksRef)
      return tasksRef.push({
        '_state': th.validTaskSpecWithStartState.startState
      }).then(_ => qw._tryToProcess())
        .then(_ => {
          expect(qw._currentTaskRef()).to.be.null;
        })
    })

    it('should try and process a task if not busy', () => {
      qw._startState(th.validTaskSpecWithStartState.startState)
      qw._inProgressState(th.validTaskSpecWithStartState.inProgressState)
      qw._newTaskRef(tasksRef)
      return tasksRef.push({
        '_state': th.validTaskSpecWithStartState.startState
      }).then(_ => qw._tryToProcess())
        .then(_ => {
          expect(qw._currentTaskRef()).to.not.be.null;
          expect(qw._busy()).to.be.true;
        })
    })

    it('should try and process a task if not busy, rejecting it if it throws', () => {
      qw = new th.QueueWorker(tasksRef, '0', true, false, () => {
        throw new Error('Error thrown in processingFunction')
      })
      qw._startState(th.validTaskSpecWithStartState.startState)
      qw._inProgressState(th.validTaskSpecWithStartState.inProgressState)
      qw._finishedState(th.validTaskSpecWithFinishedState.finishedState)
      qw._taskRetries(0)
      qw._newTaskRef(tasksRef)
      const testRef = tasksRef.push()

      return testRef.set({
        '_state': th.validTaskSpecWithStartState.startState
      }).then(_ => qw._tryToProcess())
        .then(_ => {
          expect(qw._currentTaskRef()).to.not.be.null;
          expect(qw._busy()).to.be.true 
        })
        .then(_ => testRef.once('child_changed'))
        .then(_ => testRef.once('value'))
        .then(snapshot => {
          var task = snapshot.val();
          expect(task).to.have.all.keys(['_state', '_progress', '_state_changed', '_error_details']);
          expect(task._state).to.equal('error');
          expect(task._state_changed).to.be.closeTo(serverNow(), 250);
          expect(task._progress).to.equal(0);
          expect(task._error_details).to.have.all.keys(['previous_state', 'attempts', 'error', 'error_stack']);
          expect(task._error_details.previous_state).to.equal(th.validTaskSpecWithStartState.inProgressState);
          expect(task._error_details.attempts).to.equal(1);
          expect(task._error_details.error).to.equal('Error thrown in processingFunction');
          expect(task._error_details.error_stack).to.be.a.string;
        })
    })

    it('should try and process a task without a _state if not busy', () => {
      qw._startState(null)
      qw._inProgressState(th.validBasicTaskSpec.inProgressState)
      qw._newTaskRef(tasksRef)
      return tasksRef.push({
        foo: 'bar'
      }).then(_ => qw._tryToProcess())
        .then(_ => {
          expect(qw._currentTaskRef()).to.not.be.null;
          expect(qw._busy()).to.be.true;
        })
    })

    it('should not try and process a task if not a plain object [1]', () => {
      qw._inProgressState(th.validTaskSpecWithStartState.inProgressState)
      qw._suppressStack(true)
      qw._newTaskRef(tasksRef)
      const testRef = tasksRef.push('invalid')
      return testRef
        .then(_ => qw._tryToProcess())
        .then(_ => {
          expect(qw._currentTaskRef()).to.be.null;
          expect(qw._busy()).to.be.false;
        })
        .then(_ => testRef.once('value'))
        .then(snapshot => {
          var task = snapshot.val();
          expect(task).to.have.all.keys(['_error_details', '_state', '_state_changed']);
          expect(task._error_details).to.have.all.keys(['error', 'original_task']);
          expect(task._error_details.error).to.equal('Task was malformed');
          expect(task._error_details.original_task).to.equal('invalid');
          expect(task._state).to.equal('error');
          expect(task._state_changed).to.be.closeTo(serverNow(), 250);
        })
    })

    it('should not try and process a task if not a plain object [2]', () => {
      qw._inProgressState(th.validTaskSpecWithStartState.inProgressState)
      qw._newTaskRef(tasksRef)
      const testRef = tasksRef.push('invalid')
      return testRef
        .then(_ => qw._tryToProcess())
        .then(_ => {
          expect(qw._currentTaskRef()).to.be.null;
          expect(qw._busy()).to.be.false;
        })
        .then(_ => testRef.once('value'))
        .then(snapshot => {
          var task = snapshot.val()
          expect(task).to.have.all.keys(['_error_details', '_state', '_state_changed'])
          expect(task._error_details).to.have.all.keys(['error', 'original_task', 'error_stack'])
          expect(task._error_details.error).to.equal('Task was malformed')
          expect(task._error_details.original_task).to.equal('invalid')
          expect(task._error_details.error_stack).to.be.a.string
          expect(task._state).to.equal('error')
          expect(task._state_changed).to.be.closeTo(serverNow(), 250)
        })
    })

    it('should not try and process a task if no longer in correct startState', () => {
      qw._startState(th.validTaskSpecWithStartState.startState)
      qw._inProgressState(th.validTaskSpecWithStartState.inProgressState)
      qw._newTaskRef(tasksRef)
      return tasksRef.push({
        '_state': th.validTaskSpecWithStartState.inProgressState
      }).then(_ => qw._tryToProcess())
        .then(_ => {
          expect(qw._currentTaskRef()).to.be.null
        })
    })

    it('should not try and process a task if no task to process', () => {
      qw._startState(th.validTaskSpecWithStartState.startState)
      qw._inProgressState(th.validTaskSpecWithStartState.inProgressState)
      qw._newTaskRef(tasksRef)
      return qw._tryToProcess().then(_ => {
        expect(qw._currentTaskRef()).to.be.null
      })
    })

    it('should invalidate callbacks if another process times the task out', () => {
      qw._startState(th.validTaskSpecWithStartState.startState)
      qw._inProgressState(th.validTaskSpecWithStartState.inProgressState)
      qw._newTaskRef(tasksRef)
      const testRef = tasksRef.push({
        '_state': th.validTaskSpecWithStartState.startState
      })
      return testRef
        .then(_ => qw._tryToProcess())
        .then(_ => {
          expect(qw._currentTaskRef()).to.not.be.null;
          expect(qw._busy()).to.be.true;
        })
        .then(_ => testRef.update({ '_owner': null }))
        .then(_ => {
          expect(qw._currentTaskRef()).to.be.null;
        })
    })

    it('should sanitize data passed to the processing function when specified', done => {
      qw = new th.QueueWorker(tasksRef, '0', true, false, data => {
        try {
          expect(data).to.have.all.keys(['foo'])
          done()
        } catch (error) {
          done(error)
        }
      })
      qw.setTaskSpec(th.validBasicTaskSpec)
      tasksRef.push({ foo: 'bar' })
    });

    it('should not sanitize data passed to the processing function when specified', done => {
      qw = new th.QueueWorker(tasksRef, '0', false, false, data => {
        try {
          expect(data).to.have.all.keys(['foo', '_owner', '_progress', '_state', '_state_changed', '_id']);
          done()
        } catch (error) {
          done(error)
        }
      });
      qw.setTaskSpec(th.validBasicTaskSpec)
      tasksRef.push({ foo: 'bar' })
    })
  })

  describe('#_setUpTimeouts', () => {
    let qw
    let clock
    let setTimeoutSpy
    let clearTimeoutSpy

    before(done => {
      // ensure firebase is up and running before we stop the clock 
      // at individual specs
      tasksRef.push().set(null).then(done)
    })

    beforeEach(() => {
      qw = new th.QueueWorkerWithoutProcessing(tasksRef, '0', true, false, _.noop)
      clock = sinon.useFakeTimers(now())
      setTimeoutSpy = sinon.spy(global, 'setTimeout')
      clearTimeoutSpy = sinon.spy(global, 'clearTimeout')
    })

    afterEach(done => {
      setTimeoutSpy.restore()
      clearTimeoutSpy.restore()
      clock.restore()
      qw.shutdown()
        .then(_ => tasksRef.set(null))
        .then(done)
    })

    it('should not set up timeouts when no task timeout is set', () => {
      qw.setTaskSpec(th.validBasicTaskSpec)
      return tasksRef.push().set({
        '_state': th.validBasicTaskSpec.inProgressState,
        '_state_changed': now()
      }).then(_ => {
          expect(qw._expiryTimeouts).to.deep.equal({})
        })
    })

    it('should not set up timeouts when a task not in progress is added and a task timeout is set', () => {
      qw.setTaskSpec(th.validTaskSpecWithTimeout)
      return tasksRef.push({
        '_state': th.validTaskSpecWithFinishedState.finishedState,
        '_state_changed': now()
      }).then(_ => {
          expect(qw._expiryTimeouts).to.deep.equal({})
        })
    })

    it('should set up timeout listeners when a task timeout is set', () => {
      expect(qw._expiryTimeouts).to.deep.equal({})
      expect(qw._processingTasksRef()).to.be.null
      expect(qw._processingTaskAddedListener()).to.be.null
      expect(qw._processingTaskRemovedListener()).to.be.null

      qw.setTaskSpec(th.validTaskSpecWithTimeout)

      expect(qw._expiryTimeouts).to.deep.equal({})
      expect(qw._processingTasksRef()).to.not.be.null
      expect(qw._processingTaskAddedListener()).to.not.be.null
      expect(qw._processingTaskRemovedListener()).to.not.be.null
    })

    it('should remove timeout listeners when a task timeout is not specified after a previous task specified a timeout', () => {
      qw.setTaskSpec(th.validTaskSpecWithTimeout)

      expect(qw._expiryTimeouts).to.deep.equal({})
      expect(qw._processingTasksRef()).to.not.be.null
      expect(qw._processingTaskAddedListener()).to.not.be.null
      expect(qw._processingTaskRemovedListener()).to.not.be.null

      qw.setTaskSpec(th.validBasicTaskSpec)

      expect(qw._expiryTimeouts).to.deep.equal({})
      expect(qw._processingTasksRef()).to.be.null
      expect(qw._processingTaskAddedListener()).to.be.null
      expect(qw._processingTaskRemovedListener()).to.be.null
    })

    it('should set up a timeout when a task timeout is set and a task added', () => {
      qw.setTaskSpec(th.validTaskSpecWithTimeout);
      const testRef = tasksRef.push({
        '_state': th.validTaskSpecWithTimeout.inProgressState,
        '_state_changed': now() - 5
      })

      return testRef.then(_ => {
          expect(qw._expiryTimeouts).to.have.all.keys([testRef.key])
          expect(setTimeoutSpy.firstCall.args[1]).to.equal(th.validTaskSpecWithTimeout.timeout - 5)
        })
    })

    it('should set up a timeout when a task timeout is set and a task owner changed', () => {
      qw.setTaskSpec(th.validTaskSpecWithTimeout)
      const testRef = tasksRef.push({
        '_owner': qw._processId + ':0',
        '_state': th.validTaskSpecWithTimeout.inProgressState,
        '_state_changed': now() - 10
      })

      return testRef
        .then(_ => {
          expect(qw._expiryTimeouts).to.have.all.keys([testRef.key])
        })
        .then(_ => testRef.update({ '_owner': qw._processId + ':1', '_state_changed': now() - 5 }))
        .then(_ => {
          expect(qw._expiryTimeouts).to.have.all.keys([testRef.key])
          expect(setTimeoutSpy.lastCall.args[1]).to.equal(th.validTaskSpecWithTimeout.timeout - 5)
        })
    })

    it('should not set up a timeout when a task timeout is set and a task updated', () => {
      qw.setTaskSpec(th.validTaskSpecWithTimeout)

      const testRef = tasksRef.push({
        '_owner': qw._processId + ':0',
        '_progress': 0,
        '_state': th.validTaskSpecWithTimeout.inProgressState,
        '_state_changed': now() - 5
      })

      return testRef
        .then(_ => {
          expect(qw._expiryTimeouts).to.have.all.keys([testRef.key])
        })
        .then(_ => testRef.update({ '_progress': 1 }))
        .then(_ => {
          expect(qw._expiryTimeouts).to.have.all.keys([testRef.key])
          expect(setTimeoutSpy.firstCall.args[1]).to.equal(th.validTaskSpecWithTimeout.timeout - 5)
        })
    })

    it('should set up a timeout when a task timeout is set and a task added without a _state_changed time', () => {
      qw.setTaskSpec(th.validTaskSpecWithTimeout)

      const testRef = tasksRef.push({
        '_state': th.validTaskSpecWithTimeout.inProgressState
      })

      return testRef
        .then(_ => {
          expect(qw._expiryTimeouts).to.have.all.keys([testRef.key]);
          expect(setTimeoutSpy.firstCall.args[1]).to.equal(th.validTaskSpecWithTimeout.timeout);
        })
    })

    it('should clear timeouts when a task timeout is not set and a timeout exists', () => {
      qw.setTaskSpec(th.validTaskSpecWithTimeout)
      const testRef = tasksRef.push({
        '_state': th.validTaskSpecWithTimeout.inProgressState,
        '_state_changed': now()
      })

      return testRef
        .then(_ => {
          expect(qw._expiryTimeouts).to.have.all.keys([testRef.key])
          qw.setTaskSpec()
          expect(qw._expiryTimeouts).to.deep.equal({})
        })
    })

    it('should clear a timeout when a task is completed', () => {
      const taskSpec = _.clone(th.validTaskSpecWithTimeout)
      taskSpec.finishedState = th.validTaskSpecWithFinishedState.finishedState
      qw.setTaskSpec(taskSpec);

      const task = {
        '_state': taskSpec.inProgressState,
        '_state_changed': now()
      }

      const testRef = tasksRef.push()
      return testRef.set(task)
        .then(_ => testRef.update({ '_state': taskSpec.finishedState }))
        .then(_ => {
          expect(setTimeoutSpy.callCount).to.equal(1, 'setTimeout was not called')
          const clearId = setTimeoutSpy.firstCall.returnValue
          expect(clearTimeoutSpy.callCount).to.equal(1, 'clearTimeout was not called')
          expect(clearTimeoutSpy.firstCall.calledWith(clearId)).to.be.equal(true, 'clearTimeout was not called with correct Id')
        })
    })
  })

  describe('#QueueWorker.isValidTaskSpec', () => {

    let taskSpec = null

    const { isValidTaskSpec } = th.QueueWorker

    beforeEach(() => {
      taskSpec = _.clone(th.validBasicTaskSpec)
    })

    it('should not accept a non-plain object as a valid task spec', () => 
      nonPlainObjects.forEach(nonPlainObject => {
        expect(isValidTaskSpec(nonPlainObject)).to.be.false;
      })
    )

    it('should not accept an empty object as a valid task spec', () => {
      expect(isValidTaskSpec({})).to.be.false
    })

    it('should not accept a non-empty object without the required keys as a valid task spec', () => {
      expect(isValidTaskSpec({ foo: 'bar' })).to.be.false
    })

    it('should not accept a startState that is not a string as a valid task spec', () => 
      nonStringsWithoutNull.forEach(nonStringObject => {
        taskSpec.startState = nonStringObject
        expect(isValidTaskSpec(taskSpec)).to.be.false
      })
    )

    it('should not accept an inProgressState that is not a string as a valid task spec', () => 
      nonStrings.forEach(nonStringObject => {
        taskSpec.inProgressState = nonStringObject
        expect(isValidTaskSpec(taskSpec)).to.be.false
      })
    )

    it('should not accept a finishedState that is not a string as a valid task spec', () => 
      nonStringsWithoutNull.forEach(nonStringObject => {
        taskSpec.finishedState = nonStringObject
        expect(isValidTaskSpec(taskSpec)).to.be.false
      })
    )

    it('should not accept a finishedState that is not a string as a valid task spec', () => 
      nonStringsWithoutNull.forEach(nonStringObject => {
        taskSpec.errorState = nonStringObject
        expect(isValidTaskSpec(taskSpec)).to.be.false
      })
    )

    it('should not accept a timeout that is not a positive integer as a valid task spec', () => 
      nonPositiveIntegers.forEach(nonPositiveIntegerObject => {
        taskSpec.timeout = nonPositiveIntegerObject
        expect(isValidTaskSpec(taskSpec)).to.be.false
      })
    )

    it('should not accept a retries that is not a positive or 0 integer as a valid task spec', () => 
      nonPositiveIntegersWithout0.forEach(nonPositiveIntegerObject => {
        taskSpec.retries = nonPositiveIntegerObject
        expect(isValidTaskSpec(taskSpec)).to.be.false
      })
    )

    it('should accept a valid task spec without a timeout', () => {
      expect(isValidTaskSpec(th.validBasicTaskSpec)).to.be.true
    })

    it('should accept a valid task spec with a startState', () => {
      expect(isValidTaskSpec(th.validTaskSpecWithStartState)).to.be.true
    })

    it('should not accept a taskSpec with the same startState and inProgressState', () => {
      taskSpec.startState = taskSpec.inProgressState
      expect(isValidTaskSpec(taskSpec)).to.be.false
    })

    it('should accept a valid task spec with a finishedState', () => {
      expect(isValidTaskSpec(th.validTaskSpecWithFinishedState)).to.be.true
    })

    it('should not accept a taskSpec with the same finishedState and inProgressState', () => {
      taskSpec.finishedState = taskSpec.inProgressState
      expect(isValidTaskSpec(taskSpec)).to.be.false
    })

    it('should accept a valid task spec with a errorState', () => {
      expect(isValidTaskSpec(th.validTaskSpecWithErrorState)).to.be.true
    })

    it('should not accept a taskSpec with the same errorState and inProgressState', () => {
      taskSpec.errorState = taskSpec.inProgressState
      expect(isValidTaskSpec(taskSpec)).to.be.false
    })

    it('should accept a valid task spec with a timeout', () => {
      expect(isValidTaskSpec(th.validTaskSpecWithTimeout)).to.be.true
    })

    it('should accept a valid task spec with retries', () => {
      expect(isValidTaskSpec(th.validTaskSpecWithRetries)).to.be.true
    })

    it('should accept a valid task spec with 0 retries', () => {
      taskSpec.retries = 0
      expect(isValidTaskSpec(taskSpec)).to.be.true
    })

    it('should not accept a taskSpec with the same startState and finishedState', () => {
      taskSpec = _.clone(th.validTaskSpecWithFinishedState)
      taskSpec.startState = taskSpec.finishedState
      expect(isValidTaskSpec(taskSpec)).to.be.false
    })

    it('should accept a taskSpec with the same errorState and startState', () => {
      taskSpec = _.clone(th.validTaskSpecWithStartState)
      taskSpec.errorState = taskSpec.startState
      expect(isValidTaskSpec(taskSpec)).to.be.true
    })

    it('should accept a taskSpec with the same errorState and finishedState', () => {
      taskSpec = _.clone(th.validTaskSpecWithFinishedState)
      taskSpec.errorState = taskSpec.finishedState
      expect(isValidTaskSpec(taskSpec)).to.be.true
    })

    it('should accept a valid task spec with a startState, a finishedState, an errorState, a timeout, and retries', () => {
      expect(isValidTaskSpec(th.validTaskSpecWithEverything)).to.be.true
    })

    it('should accept a valid basic task spec with null parameters for everything else', () => {
      taskSpec = _.assign(taskSpec, {
        startState: null,
        finishedState: null,
        errorState: null,
        timeout: null,
        retries: null
      })
      expect(isValidTaskSpec(taskSpec)).to.be.true
    })
  })

  describe('#setTaskSpec', () => {
    let qw

    before(done => {
      tasksRef.set(null).then(done)
    })

    beforeEach(() => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop)
    })

    afterEach(done => {
      qw.shutdown()
        .then(_ => tasksRef.set(null))
        .then(done)
    })

    /*
      These are quite problematic, the code inhere it very much tied to 
      the implementation.

      My guess is that these were created after the fact and have been copied
      and pasted. It will be rough to dig through them and turn them into specs
      that test the behaviour or effect of a certain call.
    */

    it('should reset the worker when called with an invalid task spec', () => 
      invalidTaskSpecs.forEach(invalidTaskSpec => {
        const oldTaskNumber = qw._taskNumber()
        qw.setTaskSpec(invalidTaskSpec)
        expect(qw._taskNumber()).to.not.equal(oldTaskNumber)
        expect(qw._startState()).to.be.null
        expect(qw._inProgressState()).to.be.null
        expect(qw._finishedState()).to.be.null
        expect(qw._taskTimeout()).to.be.null
        expect(qw._newTaskRef()).to.be.null
        expect(qw._newTaskListener()).to.be.null
        expect(qw._expiryTimeouts).to.deep.equal({})
      })
    )

    it('should reset the worker when called with an invalid task spec after a valid task spec', () =>
      invalidTaskSpecs.forEach(invalidTaskSpec => {
        qw.setTaskSpec(th.validBasicTaskSpec)
        const oldTaskNumber = qw._taskNumber()
        qw.setTaskSpec(invalidTaskSpec)
        expect(qw._taskNumber()).to.not.equal(oldTaskNumber)
        expect(qw._startState()).to.be.null
        expect(qw._inProgressState()).to.be.null
        expect(qw._finishedState()).to.be.null
        expect(qw._taskTimeout()).to.be.null
        expect(qw._newTaskRef()).to.be.null
        expect(qw._newTaskListener()).to.be.null
        expect(qw._expiryTimeouts).to.deep.equal({})
      })
    )

    it('should reset the worker when called with an invalid task spec after a valid task spec with everythin', () => 
      invalidTaskSpecs.forEach(invalidTaskSpec => {
        qw.setTaskSpec(th.validTaskSpecWithEverything)
        const oldTaskNumber = qw._taskNumber()
        qw.setTaskSpec(invalidTaskSpec)
        expect(qw._taskNumber()).to.not.equal(oldTaskNumber)
        expect(qw._startState()).to.be.null
        expect(qw._inProgressState()).to.be.null
        expect(qw._finishedState()).to.be.null
        expect(qw._taskTimeout()).to.be.null
        expect(qw._newTaskRef()).to.be.null
        expect(qw._newTaskListener()).to.be.null
        expect(qw._expiryTimeouts).to.deep.equal({})
      })
    )

    it('should reset a worker when called with a basic valid task spec', () => {
      const oldTaskNumber = qw._taskNumber()
      qw.setTaskSpec(th.validBasicTaskSpec)
      expect(qw._taskNumber()).to.not.equal(oldTaskNumber)
      expect(qw._startState()).to.be.null
      expect(qw._inProgressState()).to.equal(th.validBasicTaskSpec.inProgressState)
      expect(qw._finishedState()).to.be.null
      expect(qw._taskTimeout()).to.be.null
      expect(qw._newTaskRef()).to.have.property('on').and.be.a('function')
      expect(qw._newTaskListener()).to.be.a('function')
      expect(qw._expiryTimeouts).to.deep.equal({})
    })

    it('should reset a worker when called with a valid task spec with a startState', () => {
      const oldTaskNumber = qw._taskNumber()
      qw.setTaskSpec(th.validTaskSpecWithStartState)
      expect(qw._taskNumber()).to.not.equal(oldTaskNumber)
      expect(qw._startState()).to.equal(th.validTaskSpecWithStartState.startState)
      expect(qw._inProgressState()).to.equal(th.validTaskSpecWithStartState.inProgressState)
      expect(qw._finishedState()).to.be.null
      expect(qw._taskTimeout()).to.be.null
      expect(qw._newTaskRef()).to.have.property('on').and.be.a('function')
      expect(qw._newTaskListener()).to.be.a('function')
      expect(qw._expiryTimeouts).to.deep.equal({})
    })

    it('should reset a worker when called with a valid task spec with a finishedState', () => {
      const oldTaskNumber = qw._taskNumber()
      qw.setTaskSpec(th.validTaskSpecWithFinishedState)
      expect(qw._taskNumber()).to.not.equal(oldTaskNumber)
      expect(qw._startState()).to.be.null
      expect(qw._inProgressState()).to.equal(th.validTaskSpecWithFinishedState.inProgressState)
      expect(qw._finishedState()).to.equal(th.validTaskSpecWithFinishedState.finishedState)
      expect(qw._taskTimeout()).to.be.null
      expect(qw._newTaskRef()).to.have.property('on').and.be.a('function')
      expect(qw._newTaskListener()).to.be.a('function')
      expect(qw._expiryTimeouts).to.deep.equal({})
    })

    it('should reset a worker when called with a valid task spec with a timeout', () => {
      const oldTaskNumber = qw._taskNumber()
      qw.setTaskSpec(th.validTaskSpecWithTimeout)
      expect(qw._taskNumber()).to.not.equal(oldTaskNumber)
      expect(qw._startState()).to.be.null
      expect(qw._inProgressState()).to.equal(th.validTaskSpecWithTimeout.inProgressState)
      expect(qw._finishedState()).to.be.null
      expect(qw._taskTimeout()).to.equal(th.validTaskSpecWithTimeout.timeout)
      expect(qw._newTaskRef()).to.have.property('on').and.be.a('function')
      expect(qw._newTaskListener()).to.be.a('function')
      expect(qw._expiryTimeouts).to.deep.equal({})
    })

    it('should reset a worker when called with a valid task spec with everything', () => {
      const oldTaskNumber = qw._taskNumber()
      qw.setTaskSpec(th.validTaskSpecWithEverything)
      expect(qw._taskNumber()).to.not.equal(oldTaskNumber)
      expect(qw._startState()).to.equal(th.validTaskSpecWithEverything.startState)
      expect(qw._inProgressState()).to.equal(th.validTaskSpecWithEverything.inProgressState)
      expect(qw._finishedState()).to.equal(th.validTaskSpecWithEverything.finishedState)
      expect(qw._taskTimeout()).to.equal(th.validTaskSpecWithEverything.timeout)
      expect(qw._newTaskRef()).to.have.property('on').and.be.a('function')
      expect(qw._newTaskListener()).to.be.a('function')
      expect(qw._expiryTimeouts).to.deep.equal({})
    })

    it('should not pick up tasks on the queue not for the current worker', () => {
      // This current version of the spec shows that it causes the worker to stall.
      // A different implementation of the worker might make this spec succeed,
      // in that case this spec should be written differently:
      //
      // Make 2 queues where one contains [b1, b2] and the other [c, c, c, b3]
      // Both queues respond to `b` tasks. If the implementation is correct, b3
      // will be faster, if incorrect b2 will be faster

      qw = new th.QueueWorker(tasksRef, '0', true, false, th.echo)
      return Promise.all([
        tasksRef.push({ '_state': '1.start' }),
        tasksRef.push({ '_state': '2.start', test: 'check' })
      ]).then(([_, ref]) => {
        qw.setTaskSpec({ startState: '2.start', inProgressState: 'in_progress', finishedState: 'done'})
        return Promise.race([
          th.waitForState(ref, 'done').then(snapshot => snapshot.val()),
          th.timeout(1000)
        ])
      }).then(val => {
        expect(val).to.have.a.property('test').that.equals('check')
      })
    })

    it('should pick up tasks on the queue with no "_state" when a task is specified without a startState', () => {
      let result = null
      qw = new th.QueueWorker(tasksRef, '0', true, false, th.withData(data => { result = data }))
      qw.setTaskSpec(th.validBasicTaskSpec)
      const task = { foo: 'bar' }
      return tasksRef.push(task)
        .then(_ => th.waitFor(() => !!result, 500))
        .then(_ => {
          expect(result).to.deep.equal(task)
        })
    })

    it('should pick up tasks on the queue with the corresponding "_state" when a task is specifies a startState', () => {
      let result = null
      qw = new th.QueueWorker(tasksRef, '0', true, false, th.withData(data => { result = data }))
      qw.setTaskSpec(th.validTaskSpecWithStartState)
      const task = { foo: 'bar', '_state': th.validTaskSpecWithStartState.startState }
      return tasksRef.push(task)
        .then(_ => th.waitFor(() => !!result, 500))
        .then(_ => {
          delete task._state
          expect(result).to.deep.equal(task)
        })
    })
  })

  describe('#shutdown', () => {
    let qw
    let callbackStarted
    let callbackComplete

    beforeEach((done) => {
      callbackStarted = false
      callbackComplete = false
      qw = new th.QueueWorker(tasksRef, '0', true, false, function(data, progress, resolve) {
        callbackStarted = true
        setTimeout(() => {
          callbackComplete = true
          resolve()
        }, 500)
      })

      tasksRef.push().set(null).then(done)
    })

    it('should shutdown a worker not processing any tasks', () => {
      return qw.shutdown().should.eventually.be.fulfilled;
    })

    it('should shutdown a worker after the current task has finished', () => {
      expect(callbackStarted).to.be.false
      qw.setTaskSpec(th.validBasicTaskSpec)
      return tasksRef.push({
        foo: 'bar'
      }).then(_ => new Promise(r => setTimeout(r, 400)))
        .then(_ => {
          expect(callbackStarted).to.be.true
          expect(callbackComplete).to.be.false
          return qw.shutdown()
        })
        .then(_ => {
          expect(callbackComplete).to.be.true;
        })
    })

    it('should return the same shutdown promise if shutdown is called twice', () => {
      qw.setTaskSpec(th.validBasicTaskSpec)
      return tasksRef.push({
        foo: 'bar'
      }).then(_ => {
          const firstPromise = qw.shutdown()
          const secondPromise = qw.shutdown()
          expect(firstPromise).to.deep.equal(secondPromise)
          return firstPromise
      })
    })
  })
})
