'use strict';

const _ = require('lodash')
const Helpers = require('../helpers.js')
const chai = require('chai')
const sinon = require('sinon')

chai
  .use(require('sinon-chai'))
  .use(require('chai-as-promised'))
  .should()

const { expect } = chai

const th = new Helpers()
const tasksRef = th.testRef.child('tasks')

const nonBooleans             = ['', 'foo', NaN, Infinity,              0, 1, ['foo', 'bar'], { foo: 'bar' }, null, { foo: { bar: { baz: true } } }, _.noop                ]
const nonStrings              = [           NaN, Infinity, true, false, 0, 1, ['foo', 'bar'], { foo: 'bar' }, null, { foo: { bar: { baz: true } } }, _.noop                ]
const nonFunctions            = ['', 'foo', NaN, Infinity, true, false, 0, 1, ['foo', 'bar'], { foo: 'bar' }, null, { foo: { bar: { baz: true } } }                        ]
const nonPlainObjects         = ['', 'foo', NaN, Infinity, true, false, 0, 1, ['foo', 'bar'],                 null,                                  _.noop                ]
const nonPositiveIntegers     = ['', 'foo', NaN, Infinity, true, false, 0,    ['foo', 'bar'], { foo: 'bar' },       { foo: { bar: { baz: true } } }, _.noop, -1, 1.1       ]
const invalidPercentageValues = ['', 'foo', NaN, Infinity, true, false,       ['foo', 'bar'], { foo: 'bar' },       { foo: { bar: { baz: true } } }, _.noop, -1,      100.1]

const nonStringsWithoutNull = nonStrings.filter(x => x !== null)
const nonPositiveIntegersWithout0 = nonPositiveIntegers.filter(x => x !== 0)

function now() { return new Date().getTime() }

describe('QueueWorker', () => {
  describe('initialize', () => {
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
        expect(() => new th.QueueWorker(tasksRef, '0', true, false, nonFunctionObject)).to.throw('No processing function provided.')
      )
    )

    it('should create a QueueWorker with a tasksRef, processId, sanitize option and a processing function', () =>
      new th.QueueWorker(tasksRef, '0', true, false, _.noop)
    )

    it('should not create a QueueWorker with a non-string processId specified', () => 
      nonStrings.forEach(nonStringObject =>
        expect(() => new th.QueueWorker(tasksRef, nonStringObject, true, false, _.noop)).to.throw('Invalid process ID provided.')
      )
    )

    it('should not create a QueueWorker with a non-boolean sanitize option specified', () =>
      nonBooleans.forEach(nonBooleanObject =>
        expect(() => new th.QueueWorker(tasksRef, '0', nonBooleanObject, false, _.noop)).to.throw('Invalid sanitize option.')
      )
    )

    it('should not create a QueueWorker with a non-boolean suppressStack option specified', () =>
      nonBooleans.forEach(nonBooleanObject =>
        expect(() => new th.QueueWorker(tasksRef, '0', true, nonBooleanObject, _.noop)).to.throw('Invalid suppressStack option.')
      )
    )
  })

  describe('#_resetTask', () => {
    let qw
    let testRef

    beforeEach(() => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop)
    })

    afterEach(done => {
      testRef.off()
      qw.shutdown()
        .then(_ => tasksRef.set(null))
        .then(done)
    })

    function resetTask({ task, forceReset }) {
      testRef = tasksRef.push()
      return testRef.set(task)
        .then(_ => qw._resetTask(testRef, forceReset))
        .then(_ => testRef.once('value'))
    }

    it('should reset a task that is currently in progress', done => {
      qw.setTaskSpec(th.validBasicTaskSpec)
      resetTask({ 
        forceReset: true,
        task: {
          '_state': th.validBasicTaskSpec.inProgressState,
          '_state_changed': now(),
          '_owner': qw._currentId()
        }
      }).then(snapshot => {
          const task = snapshot.val()
          // I don't understand why the _state, _owner and _progress properties are not checked
          expect(task).to.have.all.keys(['_state_changed'])
          expect(task._state_changed).to.be.closeTo(now() + th.offset, 250)
        })
        .then(done).catch(done)
    })

    it('should not reset a task if `forceReset` set but no longer owned by current worker', done => {
      qw.setTaskSpec(th.validBasicTaskSpec)
      const task = {
        '_state': th.validBasicTaskSpec.inProgressState,
        '_state_changed': now(),
        '_owner': 'someone-else'
      }
      resetTask({ task, forceReset: true })
        .then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
        .then(done).catch(done)
    })

    it('should not reset a task if `forceReset` not set and it is has changed state recently', done => {
      qw.setTaskSpec(th.validBasicTaskSpec);
      const task = {
        '_state': th.validBasicTaskSpec.inProgressState,
        '_state_changed': now(),
        '_owner': 'someone'
      }
      resetTask({ task, forceReset: false })
        .then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
        .then(done).catch(done)
    })

    it('should reset a task that is currently in progress that has timed out', done => {
      qw.setTaskSpec(th.validTaskSpecWithTimeout)
      resetTask({
        forceReset: false,
        task: {
          '_state': th.validBasicTaskSpec.inProgressState,
          '_state_changed': now() - th.validTaskSpecWithTimeout.timeout,
          '_owner': 'someone'
        }
      }).then(snapshot => {
          var task = snapshot.val()
          // we should probably check _state, _owner and _progress
          expect(task).to.have.all.keys(['_state_changed'])
          expect(task._state_changed).to.be.closeTo(now() + th.offset, 250)
        })
        .then(done).catch(done)
    })

    it('should not reset a task that no longer exists', done => {
      qw.setTaskSpec(th.validBasicTaskSpec)
      resetTask({ task: null, forceReset: true })
        .then(snapshot => {
          expect(snapshot.val()).to.be.null
        })
        .then(done).catch(done)
    })  

    it('should not reset a task if it is has already changed state', done => {
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      const task = {
        '_state': th.validTaskSpecWithFinishedState.finishedState,
        '_state_changed': now(),
        '_owner': qw._currentId()
      }
      resetTask({ task, forceReset: true })
        .then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
        .then(done).catch(done)
    })

    it('should not reset a task if it is has no state', done => {
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      const task = {
        '_state_changed': now(),
        '_owner': qw._currentId()
      }
      resetTask({ task, forceReset: true })
        .then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
        .then(done).catch(done)
    })
  })

  describe('#_resolve', () => {
    let qw
    let testRef

    beforeEach(() => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop)
    })

    afterEach(done => {
      testRef.off()
      qw.shutdown()
        .then(_ => tasksRef.set(null))
        .then(done)
    })

    function resolve({ task, taskNumber = qw._taskNumber(), newTask }) {
      testRef = tasksRef.push()
      return testRef.set(task)
        .then(_ => qw._resolve(testRef, taskNumber)(newTask))
        .then(_ => testRef.once('value'))
    }

    it('should resolve a task owned by the current worker and remove it when no finishedState is specified', done => {
      qw.setTaskSpec(th.validBasicTaskSpec)
      resolve({
        task: {
          '_state': th.validBasicTaskSpec.inProgressState,
          '_owner': qw._currentId()
        }
      }).then(snapshot => {
          expect(snapshot.val()).to.be.null
        })
        .then(done).catch(done)
    })

    it('should resolve a task owned by the current worker and change the state when a finishedState is specified and no object passed', done => {
      qw.setTaskSpec(th.validTaskSpecWithFinishedState)
      resolve({
        task: {
          '_state': th.validTaskSpecWithFinishedState.inProgressState,
          '_owner': qw._currentId()
        }
      }).then(snapshot => {
          var task = snapshot.val()
          expect(task).to.have.all.keys(['_state', '_state_changed', '_progress'])
          expect(task._progress).to.equal(100)
          expect(task._state).to.equal(th.validTaskSpecWithFinishedState.finishedState)
          expect(task._state_changed).to.be.closeTo(now() + th.offset, 250)
        })
        .then(done).catch(done)
    })

    nonPlainObjects.forEach(nonPlainObject => 
      it('should resolve an task owned by the current worker and change the state when a finishedState is specified and an invalid object ' + nonPlainObject + ' passed', done => {
        qw.setTaskSpec(th.validTaskSpecWithFinishedState)
        resolve({
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
            expect(task._state_changed).to.be.closeTo(now() + th.offset, 250)
          })
          .then(done).catch(done)
      })
    )

    it('should resolve a task owned by the current worker and change the state when a finishedState is specified and a plain object passed', done => {
      qw.setTaskSpec(th.validTaskSpecWithFinishedState)
      resolve({
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
          expect(task._state_changed).to.be.closeTo(now() + th.offset, 250)
          expect(task.foo).to.equal('bar')
        })
        .then(done).catch(done)
    })

    it('should resolve a task owned by the current worker and change the state to a provided valid string _new_state', done => {
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      resolve({
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
          expect(task._state_changed).to.be.closeTo(now() + th.offset, 250)
          expect(task.foo).to.equal('bar')
          // _new_state should be gone right?
        })
        .then(done).catch(done)
    })

    it('should resolve a task owned by the current worker and change the state to a provided valid null _new_state', done => {
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      resolve({
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
          expect(task._state_changed).to.be.closeTo(now() + th.offset, 250)
          expect(task.foo).to.equal('bar')
        })
        .then(done).catch(done)
    })

    it('should resolve a task owned by the current worker and remove the task when provided _new_state = false', done => {
      qw.setTaskSpec(th.validTaskSpecWithFinishedState)
      resolve({
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
        .then(done).catch(done)
    })

    it('should resolve a task owned by the current worker and change the state to finishedState when provided an invalid _new_state', done => {
      qw.setTaskSpec(th.validTaskSpecWithFinishedState)
      resolve({
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
          expect(task._state_changed).to.be.closeTo(now() + th.offset, 250)
          expect(task.foo).to.equal('bar')
        })
        .then(done).catch(done)
    })

    it('should not resolve a task that no longer exists', done => {
      qw.setTaskSpec(th.validTaskSpecWithFinishedState)
      resolve({
        task: null
      }).then(snapshot => {
          expect(snapshot.val()).to.be.null
        })
        .then(done).catch(done)
    })

    it('should not resolve a task if it is no longer owned by the current worker', done => {
      qw.setTaskSpec(th.validTaskSpecWithFinishedState)
      const task = {
        '_state': th.validTaskSpecWithFinishedState.inProgressState,
        '_owner': 'other_worker'
      };
      resolve({
        task
      }).then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
        .then(done).catch(done)
    })

    it('should not resolve a task if it is has already changed state', done => {
      qw.setTaskSpec(th.validTaskSpecWithFinishedState)
      const task = {
        '_state': th.validTaskSpecWithFinishedState.finishedState,
        '_owner': qw._currentId()
      };
      resolve({
        task
      }).then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
        .then(done).catch(done)
    })

    it('should not resolve a task if it is has no state', done => {
      qw.setTaskSpec(th.validTaskSpecWithFinishedState)
      const task = {
        '_owner': qw._processId + ':' + qw._taskNumber()
      }
      resolve({
        task
      }).then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
        .then(done).catch(done)
    })

    it('should not resolve a task if a new task is being processed', done => {
      qw.setTaskSpec(th.validTaskSpecWithFinishedState)
      const task = {
        '_state': th.validTaskSpecWithFinishedState.inProgressState,
        '_owner': qw._currentId()
      }
      resolve({
        taskNumber: qw._taskNumber() + 1,
        task
      }).then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
        .then(done).catch(done)
    })
  })

  describe('#_reject', () => {
    let qw
    let testRef

    beforeEach(() => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop)
    })

    afterEach(done => {
      testRef.off()
      qw.shutdown()
        .then(_ => tasksRef.set(null))
        .then(done)
    })

    function reject({ task, taskNumber = qw._taskNumber(), error }) {
      testRef = tasksRef.push()
      return testRef.set(task)
        .then(_ => qw._reject(testRef, taskNumber)(error))
        .then(_ => testRef.once('value'))
    }

    it('should reject a task owned by the current worker', done => {
      qw.setTaskSpec(th.validBasicTaskSpec)
      reject({
        task: {
          '_state': th.validBasicTaskSpec.inProgressState,
          '_owner': qw._currentId(),
          '_progress': 0
        }
      }).then(snapshot => {
          var task = snapshot.val()
          expect(task).to.have.all.keys(['_state', '_progress', '_state_changed', '_error_details'])
          expect(task._state).to.equal('error')
          expect(task._state_changed).to.be.closeTo(now() + th.offset, 250)
          expect(task._progress).to.equal(0)
          expect(task._error_details).to.have.all.keys(['previous_state', 'attempts'])
          expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState)
          expect(task._error_details.attempts).to.equal(1)
        })
        .then(done).catch(done)
    })

    it('should reject a task owned by the current worker and reset if more retries are specified', done => {
      qw.setTaskSpec(th.validTaskSpecWithRetries)
      reject({
        task: {
          '_state': th.validTaskSpecWithRetries.inProgressState,
          '_owner': qw._currentId(),
          '_progress': 0,
          '_error_details': {
            'previous_state': th.validTaskSpecWithRetries.inProgressState,
            'attempts': 1
          }
        }
      }).then(snapshot => {
          var task = snapshot.val()
          expect(task).to.have.all.keys(['_progress', '_state_changed', '_error_details'])
          expect(task._state_changed).to.be.closeTo(now() + th.offset, 250)
          expect(task._progress).to.equal(0)
          expect(task._error_details).to.have.all.keys(['previous_state', 'attempts'])
          expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState)
          expect(task._error_details.attempts).to.equal(2)
        })
        .then(done).catch(done)
    })

    it('should reject a task owned by the current worker and reset the attempts count if chaning error handlers', done => {
      qw.setTaskSpec(th.validTaskSpecWithRetries)
      reject({
        task: {
          '_state': th.validTaskSpecWithRetries.inProgressState,
          '_owner': qw._currentId(),
          '_progress': 0,
          '_error_details': {
            'previous_state': 'other_in_progress_state',
            'attempts': 1
          }
        }
      }).then(snapshot => {
          var task = snapshot.val()
          expect(task).to.have.all.keys(['_progress', '_state_changed', '_error_details'])
          expect(task._state_changed).to.be.closeTo(now() + th.offset, 250)
          expect(task._progress).to.equal(0)
          expect(task._error_details).to.have.all.keys(['previous_state', 'attempts'])
          expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState)
          expect(task._error_details.attempts).to.equal(1)
        })
        .then(done).catch(done)
    })

    it('should reject a task owned by the current worker and a non-standard error state', done => {
      qw.setTaskSpec(th.validTaskSpecWithErrorState)
      reject({
        task: {
          '_state': th.validBasicTaskSpec.inProgressState,
          '_owner': qw._currentId(),
          '_progress': 0
        }
      }).then(snapshot => {
          var task = snapshot.val()
          expect(task).to.have.all.keys(['_state', '_progress', '_state_changed', '_error_details'])
          expect(task._state).to.equal(th.validTaskSpecWithErrorState.errorState)
          expect(task._state_changed).to.be.closeTo(now() + th.offset, 250)
          expect(task._progress).to.equal(0)
          expect(task._error_details).to.have.all.keys(['previous_state', 'attempts'])
          expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState)
          expect(task._error_details.attempts).to.equal(1)
        })
        .then(done).catch(done)
    })

    nonStringsWithoutNull.forEach(nonStringObject =>
      it('should reject a task owned by the current worker and convert the error to a string if not a string: ' + nonStringObject, done => {
        qw.setTaskSpec(th.validBasicTaskSpec)
        reject({
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
            expect(task._state_changed).to.be.closeTo(now() + th.offset, 250)
            expect(task._progress).to.equal(0)
            expect(task._error_details).to.have.all.keys(['previous_state', 'error', 'attempts'])
            expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState)
            expect(task._error_details.error).to.equal(nonStringObject.toString())
            expect(task._error_details.attempts).to.equal(1)
          })
          .then(done).catch(done)
      })
    )

    it('should reject a task owned by the current worker and append the error string to the _error_details', done => {
      qw.setTaskSpec(th.validBasicTaskSpec)
      const error = 'My error message'
      reject({
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
          expect(task._state_changed).to.be.closeTo(now() + th.offset, 250)
          expect(task._progress).to.equal(0)
          expect(task._error_details).to.have.all.keys(['previous_state', 'error', 'attempts'])
          expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState)
          expect(task._error_details.attempts).to.equal(1)
          expect(task._error_details.error).to.equal(error)
        })
        .then(done).catch(done)
    })

    it('should reject a task owned by the current worker and append the error string and stack to the _error_details', done => {
      qw.setTaskSpec(th.validBasicTaskSpec)
      const error = new Error('My error message')
      reject({
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
          expect(task._state_changed).to.be.closeTo(now() + th.offset, 250)
          expect(task._progress).to.equal(0)
          expect(task._error_details).to.have.all.keys(['previous_state', 'error', 'attempts', 'error_stack'])
          expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState)
          expect(task._error_details.attempts).to.equal(1)
          expect(task._error_details.error).to.equal(error.message)
          expect(task._error_details.error_stack).to.be.a.string
        })
        .then(done).catch(done)
    })

    it('should reject a task owned by the current worker and append the error string to the _error_details', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, true, _.noop)
      qw.setTaskSpec(th.validBasicTaskSpec)
      const error = new Error('My error message')
      reject({
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
          expect(task._state_changed).to.be.closeTo(now() + th.offset, 250)
          expect(task._progress).to.equal(0)
          expect(task._error_details).to.have.all.keys(['previous_state', 'error', 'attempts'])
          expect(task._error_details.previous_state).to.equal(th.validBasicTaskSpec.inProgressState)
          expect(task._error_details.attempts).to.equal(1)
          expect(task._error_details.error).to.equal(error.message)
        })
        .then(done).catch(done)
    })

    it('should not reject a task that no longer exists', done => {
      qw.setTaskSpec(th.validTaskSpecWithFinishedState)
      reject({ task: null })
        .then(snapshot => {
          expect(snapshot.val()).to.be.null
        })
        .then(done).catch(done)
    })

    it('should not reject a task if it is no longer owned by the current worker', done => {
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      const task = {
        '_state': th.validTaskSpecWithFinishedState.inProgressState,
        '_owner': 'other_worker'
      }
      reject({ task })
        .then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
        .then(done).catch(done)
    })

    it('should not reject a task if it is has already changed state', done => {
      qw.setTaskSpec(th.validTaskSpecWithFinishedState)
      const task = {
        '_state': th.validTaskSpecWithFinishedState.finishedState,
        '_owner': qw._currentId(),
        '_progress': 0
      }
      reject({ task })
        .then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
        .then(done).catch(done)
    })

    it('should not reject a task if it is has no state', done => {
      qw.setTaskSpec(th.validTaskSpecWithFinishedState)
      const task = {
        '_owner': qw._currentId()
      }
      reject({ task })
        .then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
        .then(done).catch(done)
    })

    it('should not reject a task if a new task is being processed', done => {
      qw.setTaskSpec(th.validTaskSpecWithFinishedState)
      const task = {
        '_state': th.validTaskSpecWithFinishedState.inProgressState,
        '_owner': qw._currentId()
      }
      reject({ task, taskNumber: qw._taskNumber() + 1 })
        .then(snapshot => {
          expect(snapshot.val()).to.deep.equal(task)
        })
        .then(done).catch(done)
    })
  })

  describe('#_updateProgress', () => {
    let qw
    let testRef
    
    beforeEach(() => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
    })

    afterEach(done => {
      testRef.off()
      qw.shutdown()
        .then(_ => tasksRef.set(null))
        .then(done)
    })

    function updateProgress({ task, taskNumber = qw._taskNumber(), progress }) {
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
      qw.setTaskSpec(th.validBasicTaskSpec)
      return updateProgress({
        task: { 
          '_state': th.validBasicTaskSpec.inProgressState, 
          '_owner': 'someone_else' 
        },
        progress: 10
      }).should.eventually.be.rejectedWith('Can\'t update progress - current task no longer owned by this process')
    })

    it('should not update the progress of a task if the task is no longer in progress', () => {
      qw.setTaskSpec(th.validTaskSpecWithFinishedState)
      return updateProgress({
        task: { 
          '_state': th.validTaskSpecWithFinishedState.finishedState, 
          '_owner': qw._currentId() 
        },
        progress: 10
      }).should.eventually.be.rejectedWith('Can\'t update progress - current task no longer owned by this process')
    })

    it('should not update the progress of a task if the task has no _state', () => {
      qw.setTaskSpec(th.validBasicTaskSpec)
      return updateProgress({
        task: { 
          '_owner': qw._currentId() 
        },
        progress: 10
      }).should.eventually.be.rejectedWith('Can\'t update progress - current task no longer owned by this process')
    })

    it('should update the progress of the current task', () => {
      qw.setTaskSpec(th.validBasicTaskSpec);
      return updateProgress({
        task: { 
          '_state': th.validBasicTaskSpec.inProgressState, 
          '_owner': qw._currentId() 
        },
        progress: 10
      }).should.eventually.be.fulfilled
    })

    it('should not update the progress of a task if a new task is being processed', () => {
      qw.setTaskSpec(th.validBasicTaskSpec)
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
    var qw;

    beforeEach(() => {
      qw = new th.QueueWorker(tasksRef, '0', true, false, _.noop);
    });

    afterEach(done => {
      qw.setTaskSpec();
      tasksRef.set(null, done);
    });

    it('should not try and process a task if busy', done => {
      qw._startState(th.validTaskSpecWithStartState.startState);
      qw._inProgressState(th.validTaskSpecWithStartState.inProgressState);
      qw._busy(true);
      qw._newTaskRef(tasksRef);
      tasksRef.push({
        '_state': th.validTaskSpecWithStartState.startState
      }, function(errorA) {
        if (errorA) {
          return done(errorA);
        }
        return qw._tryToProcess().then(() => {
          try {
            expect(qw._currentTaskRef()).to.be.null;
            done();
          } catch (errorB) {
            done(errorB);
          }
        }).catch(done);
      });
    });

    it('should try and process a task if not busy', done => {
      qw._startState(th.validTaskSpecWithStartState.startState);
      qw._inProgressState(th.validTaskSpecWithStartState.inProgressState);
      qw._newTaskRef(tasksRef);
      tasksRef.push({
        '_state': th.validTaskSpecWithStartState.startState
      }, function(errorA) {
        if (errorA) {
          return done(errorA);
        }
        return qw._tryToProcess().then(() => {
          try {
            expect(qw._currentTaskRef()).to.not.be.null;
            expect(qw._busy()).to.be.true;
            done();
          } catch (errorB) {
            done(errorB);
          }
        }).catch(done);
      });
    });

    it('should try and process a task if not busy, rejecting it if it throws', done => {
      qw = new th.QueueWorker(tasksRef, '0', true, false, () => {
        throw new Error('Error thrown in processingFunction');
      });
      qw._startState(th.validTaskSpecWithStartState.startState);
      qw._inProgressState(th.validTaskSpecWithStartState.inProgressState);
      qw._finishedState(th.validTaskSpecWithFinishedState.finishedState);
      qw._taskRetries(0);
      qw._newTaskRef(tasksRef);
      var testRef = tasksRef.push({
        '_state': th.validTaskSpecWithStartState.startState
      }, function(errorA) {
        if (errorA) {
          return done(errorA);
        }
        return qw._tryToProcess().then(() => {
          try {
            expect(qw._currentTaskRef()).to.not.be.null;
            expect(qw._busy()).to.be.true;
            var initial = true;
            testRef.on('value', function(snapshot) {
              if (initial) {
                initial = false;
              } else {
                try {
                  testRef.off();
                  var task = snapshot.val();
                  expect(task).to.have.all.keys(['_state', '_progress', '_state_changed', '_error_details']);
                  expect(task._state).to.equal('error');
                  expect(task._state_changed).to.be.closeTo(now() + th.offset, 250);
                  expect(task._progress).to.equal(0);
                  expect(task._error_details).to.have.all.keys(['previous_state', 'attempts', 'error', 'error_stack']);
                  expect(task._error_details.previous_state).to.equal(th.validTaskSpecWithStartState.inProgressState);
                  expect(task._error_details.attempts).to.equal(1);
                  expect(task._error_details.error).to.equal('Error thrown in processingFunction');
                  expect(task._error_details.error_stack).to.be.a.string;
                  done();
                } catch (errorC) {
                  done(errorC);
                }
              }
            });
          } catch (errorB) {
            done(errorB);
          }
        }).catch(done);
      });
    });

    it('should try and process a task without a _state if not busy', done => {
      qw._startState(null);
      qw._inProgressState(th.validBasicTaskSpec.inProgressState);
      qw._newTaskRef(tasksRef);
      tasksRef.push({
        foo: 'bar'
      }, function(errorA) {
        if (errorA) {
          return done(errorA);
        }
        return qw._tryToProcess().then(() => {
          try {
            expect(qw._currentTaskRef()).to.not.be.null;
            expect(qw._busy()).to.be.true;
            done();
          } catch (errorB) {
            done(errorB);
          }
        }).catch(done);
      });
    });

    it('should not try and process a task if not a plain object [1]', done => {
      qw._inProgressState(th.validTaskSpecWithStartState.inProgressState);
      qw._suppressStack(true);
      qw._newTaskRef(tasksRef);
      var testRef = tasksRef.push('invalid', function(errorA) {
        if (errorA) {
          return done(errorA);
        }
        return qw._tryToProcess().then(() => {
          try {
            expect(qw._currentTaskRef()).to.be.null;
            expect(qw._busy()).to.be.false;
            testRef.once('value', function(snapshot) {
              try {
                var task = snapshot.val();
                expect(task).to.have.all.keys(['_error_details', '_state', '_state_changed']);
                expect(task._error_details).to.have.all.keys(['error', 'original_task']);
                expect(task._error_details.error).to.equal('Task was malformed');
                expect(task._error_details.original_task).to.equal('invalid');
                expect(task._state).to.equal('error');
                expect(task._state_changed).to.be.closeTo(now() + th.offset, 250);
                done();
              } catch (errorB) {
                done(errorB);
              }
            });
          } catch (errorC) {
            done(errorC);
          }
        }).catch(done);
      });
    });

    it('should not try and process a task if not a plain object [2]', done => {
      qw._inProgressState(th.validTaskSpecWithStartState.inProgressState);
      qw._newTaskRef(tasksRef);
      var testRef = tasksRef.push('invalid', function(errorA) {
        if (errorA) {
          return done(errorA);
        }
        return qw._tryToProcess().then(() => {
          try {
            expect(qw._currentTaskRef()).to.be.null;
            expect(qw._busy()).to.be.false;
            testRef.once('value', function(snapshot) {
              try {
                var task = snapshot.val();
                expect(task).to.have.all.keys(['_error_details', '_state', '_state_changed']);
                expect(task._error_details).to.have.all.keys(['error', 'original_task', 'error_stack']);
                expect(task._error_details.error).to.equal('Task was malformed');
                expect(task._error_details.original_task).to.equal('invalid');
                expect(task._error_details.error_stack).to.be.a.string;
                expect(task._state).to.equal('error');
                expect(task._state_changed).to.be.closeTo(now() + th.offset, 250);
                done();
              } catch (errorB) {
                done(errorB);
              }
            });
          } catch (errorC) {
            done(errorC);
          }
        }).catch(done);
      });
    });

    it('should not try and process a task if no longer in correct startState', done => {
      qw._startState(th.validTaskSpecWithStartState.startState);
      qw._inProgressState(th.validTaskSpecWithStartState.inProgressState);
      qw._newTaskRef(tasksRef);
      tasksRef.push({
        '_state': th.validTaskSpecWithStartState.inProgressState
      }, function(errorA) {
        if (errorA) {
          return done(errorA);
        }
        return qw._tryToProcess().then(() => {
          try {
            expect(qw._currentTaskRef()).to.be.null;
            done();
          } catch (errorB) {
            done(errorB);
          }
        }).catch(done);
      });
    });

    it('should not try and process a task if no task to process', done => {
      qw._startState(th.validTaskSpecWithStartState.startState);
      qw._inProgressState(th.validTaskSpecWithStartState.inProgressState);
      qw._newTaskRef(tasksRef);
      qw._tryToProcess().then(() => {
        try {
          expect(qw._currentTaskRef()).to.be.null;
          done();
        } catch (errorB) {
          done(errorB);
        }
      }).catch(done);
    });

    it('should invalidate callbacks if another process times the task out', done => {
      qw._startState(th.validTaskSpecWithStartState.startState);
      qw._inProgressState(th.validTaskSpecWithStartState.inProgressState);
      qw._newTaskRef(tasksRef);
      var testRef = tasksRef.push({
        '_state': th.validTaskSpecWithStartState.startState
      }, function(errorA) {
        if (errorA) {
          return done(errorA);
        }
        return qw._tryToProcess().then(() => {
          try {
            expect(qw._currentTaskRef()).to.not.be.null;
            expect(qw._busy()).to.be.true;
            testRef.update({
              '_owner': null
            }, function(errorB) {
              if (errorB) {
                return done(errorB);
              }
              try {
                expect(qw._currentTaskRef()).to.be.null;
                done();
              } catch (errorC) {
                done(errorC);
              }
              return undefined;
            });
          } catch (errorD) {
            done(errorD);
          }
        }).catch(done);
      });
    });

    it('should sanitize data passed to the processing function when specified', done => {
      qw = new th.QueueWorker(tasksRef, '0', true, false, function(data) {
        try {
          expect(data).to.have.all.keys(['foo']);
          done();
        } catch (error) {
          done(error);
        }
      });
      qw.setTaskSpec(th.validBasicTaskSpec);
      tasksRef.push({ foo: 'bar' });
    });

    it('should not sanitize data passed to the processing function when specified', done => {
      qw = new th.QueueWorker(tasksRef, '0', false, false, function(data) {
        try {
          expect(data).to.have.all.keys(['foo', '_owner', '_progress', '_state', '_state_changed', '_id']);
          done();
        } catch (error) {
          done(error);
        }
      });
      qw.setTaskSpec(th.validBasicTaskSpec);
      tasksRef.push({ foo: 'bar' });
    });
  });

  describe('#_setUpTimeouts', () => {
    var qw;
    var clock;

    beforeEach(() => {
      clock = sinon.useFakeTimers(now());
      qw = new th.QueueWorkerWithoutProcessing(tasksRef, '0', true, false, _.noop);
    });

    afterEach(done => {
      qw.setTaskSpec();
      clock.restore();
      tasksRef.set(null, done);
    });

    it('should not set up timeouts when no task timeout is set', done => {
      qw.setTaskSpec(th.validBasicTaskSpec);
      tasksRef.push({
        '_state': th.validBasicTaskSpec.inProgressState,
        '_state_changed': now()
      }, function(errorA) {
        if (errorA) {
          return done(errorA);
        }
        try {
          expect(qw._expiryTimeouts).to.deep.equal({});
          done();
        } catch (errorB) {
          done(errorB);
        }
        return undefined;
      });
    });

    it('should not set up timeouts when a task not in progress is added and a task timeout is set', done => {
      qw.setTaskSpec(th.validTaskSpecWithTimeout);
      tasksRef.push({
        '_state': th.validTaskSpecWithFinishedState.finishedState,
        '_state_changed': now()
      }, function(errorA) {
        if (errorA) {
          return done(errorA);
        }
        try {
          expect(qw._expiryTimeouts).to.deep.equal({});
          done();
        } catch (errorB) {
          done(errorB);
        }
        return undefined;
      });
    });

    it('should set up timeout listeners when a task timeout is set', () => {
      expect(qw._expiryTimeouts).to.deep.equal({});
      expect(qw._processingTasksRef()).to.be.null;
      expect(qw._processingTaskAddedListener()).to.be.null;
      expect(qw._processingTaskRemovedListener()).to.be.null;

      qw.setTaskSpec(th.validTaskSpecWithTimeout);

      expect(qw._expiryTimeouts).to.deep.equal({});
      expect(qw._processingTasksRef()).to.not.be.null;
      expect(qw._processingTaskAddedListener()).to.not.be.null;
      expect(qw._processingTaskRemovedListener()).to.not.be.null;
    });

    it('should remove timeout listeners when a task timeout is not specified after a previous task specified a timeout', () => {
      qw.setTaskSpec(th.validTaskSpecWithTimeout);

      expect(qw._expiryTimeouts).to.deep.equal({});
      expect(qw._processingTasksRef()).to.not.be.null;
      expect(qw._processingTaskAddedListener()).to.not.be.null;
      expect(qw._processingTaskRemovedListener()).to.not.be.null;

      qw.setTaskSpec(th.validBasicTaskSpec);

      expect(qw._expiryTimeouts).to.deep.equal({});
      expect(qw._processingTasksRef()).to.be.null;
      expect(qw._processingTaskAddedListener()).to.be.null;
      expect(qw._processingTaskRemovedListener()).to.be.null;
    });

    it('should set up a timeout when a task timeout is set and a task added', done => {
      var spy = sinon.spy(global, 'setTimeout');
      qw.setTaskSpec(th.validTaskSpecWithTimeout);
      var testRef = tasksRef.push({
        '_state': th.validTaskSpecWithTimeout.inProgressState,
        '_state_changed': now() - 5
      }, function(errorA) {
        if (errorA) {
          return done(errorA);
        }
        try {
          expect(qw._expiryTimeouts).to.have.all.keys([testRef.key]);
          expect(setTimeout.getCall(0).args[1]).to.equal(th.validTaskSpecWithTimeout.timeout - 5);
          spy.restore();
          done();
        } catch (errorB) {
          spy.restore();
          done(errorB);
        }
        return undefined;
      });
    });

    it('should set up a timeout when a task timeout is set and a task owner changed', done => {
      qw.setTaskSpec(th.validTaskSpecWithTimeout);
      var testRef = tasksRef.push({
        '_owner': qw._processId + ':0',
        '_state': th.validTaskSpecWithTimeout.inProgressState,
        '_state_changed': now() - 10
      }, function(errorA) {
        if (errorA) {
          return done(errorA);
        }
        try {
          expect(qw._expiryTimeouts).to.have.all.keys([testRef.key]);
          var spy = sinon.spy(global, 'setTimeout');
          testRef.update({
            '_owner': qw._processId + ':1',
            '_state_changed': now() - 5
          }, function(errorB) {
            if (errorB) {
              return done(errorB);
            }
            try {
              expect(qw._expiryTimeouts).to.have.all.keys([testRef.key]);
              expect(setTimeout.getCall(setTimeout.callCount - 1).args[1]).to.equal(th.validTaskSpecWithTimeout.timeout - 5);
              spy.restore();
              done();
            } catch (errorC) {
              spy.restore();
              done(errorC);
            }
            return undefined;
          });
        } catch (errorB) {
          done(errorB);
        }
        return undefined;
      });
    });

    it('should not set up a timeout when a task timeout is set and a task updated', done => {
      qw.setTaskSpec(th.validTaskSpecWithTimeout);
      var spy = sinon.spy(global, 'setTimeout');
      var testRef = tasksRef.push({
        '_owner': qw._processId + ':0',
        '_progress': 0,
        '_state': th.validTaskSpecWithTimeout.inProgressState,
        '_state_changed': now() - 5
      }, function(errorA) {
        if (errorA) {
          return done(errorA);
        }
        try {
          expect(qw._expiryTimeouts).to.have.all.keys([testRef.key]);
          testRef.update({
            '_progress': 1
          }, function(errorB) {
            if (errorB) {
              return done(errorB);
            }
            try {
              expect(qw._expiryTimeouts).to.have.all.keys([testRef.key]);
              expect(setTimeout.getCall(0).args[1]).to.equal(th.validTaskSpecWithTimeout.timeout - 5);
              spy.restore();
              done();
            } catch (errorC) {
              spy.restore();
              done(errorC);
            }
            return undefined;
          });
        } catch (errorB) {
          done(errorB);
        }
        return undefined;
      });
    });

    it('should set up a timeout when a task timeout is set and a task added without a _state_changed time', done => {
      var spy = sinon.spy(global, 'setTimeout');
      qw.setTaskSpec(th.validTaskSpecWithTimeout);
      var testRef = tasksRef.push({
        '_state': th.validTaskSpecWithTimeout.inProgressState
      }, function(errorA) {
        if (errorA) {
          return done(errorA);
        }
        try {
          expect(qw._expiryTimeouts).to.have.all.keys([testRef.key]);
          expect(setTimeout.getCall(0).args[1]).to.equal(th.validTaskSpecWithTimeout.timeout);
          spy.restore();
          done();
        } catch (errorB) {
          spy.restore();
          done(errorB);
        }
        return undefined;
      });
    });

    it('should clear timeouts when a task timeout is not set and a timeout exists', done => {
      qw.setTaskSpec(th.validTaskSpecWithTimeout);
      var testRef = tasksRef.push({
        '_state': th.validTaskSpecWithTimeout.inProgressState,
        '_state_changed': now()
      }, function(errorA) {
        if (errorA) {
          return done(errorA);
        }
        try {
          expect(qw._expiryTimeouts).to.have.all.keys([testRef.key]);
          qw.setTaskSpec();
          expect(qw._expiryTimeouts).to.deep.equal({});
          done();
        } catch (errorB) {
          done(errorB);
        }
        return undefined;
      });
    });

    it('should clear a timeout when a task is completed', done => {
      var spy = sinon.spy(qw, '_resetTask');
      var taskSpec = _.clone(th.validTaskSpecWithTimeout);
      taskSpec.finishedState = th.validTaskSpecWithFinishedState.finishedState;
      qw.setTaskSpec(taskSpec);
      var testRef = tasksRef.push({
        '_state': taskSpec.inProgressState,
        '_state_changed': now()
      }, function(errorA) {
        if (errorA) {
          spy.restore();
          return done(errorA);
        }
        try {
          expect(qw._expiryTimeouts).to.have.all.keys([testRef.key]);
          testRef.update({
            '_state': taskSpec.finishedState
          }, function(errorB) {
            if (errorB) {
              return done(errorB);
            }
            try {
              expect(qw._expiryTimeouts).to.deep.equal({});
              expect(qw._resetTask).to.not.have.been.called;
              spy.restore();
              done();
            } catch (errorC) {
              spy.restore();
              done(errorC);
            }
            return undefined;
          });
        } catch (errorD) {
          spy.restore();
          done(errorD);
        }
        return undefined;
      });
    });
  });

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
    var qw;

    afterEach(done => {
      qw.setTaskSpec();
      tasksRef.set(null, done);
    });

    it('should reset the worker when called with an invalid task spec', () => {
      ['', 'foo', NaN, Infinity, true, false, null, undefined, 0, -1, 10, ['foo', 'bar'], { foo: 'bar' }, { foo: 'bar' }, { foo: { bar: { baz: true } } }, _.noop].forEach(function(invalidTaskSpec) {
        qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
        var oldTaskNumber = qw._taskNumber();
        qw.setTaskSpec(invalidTaskSpec);
        expect(qw._taskNumber()).to.not.equal(oldTaskNumber);
        expect(qw._startState()).to.be.null;
        expect(qw._inProgressState()).to.be.null;
        expect(qw._finishedState()).to.be.null;
        expect(qw._taskTimeout()).to.be.null;
        expect(qw._newTaskRef()).to.be.null;
        expect(qw._newTaskListener()).to.be.null;
        expect(qw._expiryTimeouts).to.deep.equal({});
      });
    });

    it('should reset the worker when called with an invalid task spec after a valid task spec', () => {
      ['', 'foo', NaN, Infinity, true, false, null, undefined, 0, -1, 10, ['foo', 'bar'], { foo: 'bar' }, { foo: 'bar' }, { foo: { bar: { baz: true } } }, _.noop].forEach(function(invalidTaskSpec) {
        qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
        qw.setTaskSpec(th.validBasicTaskSpec);
        var oldTaskNumber = qw._taskNumber();
        qw.setTaskSpec(invalidTaskSpec);
        expect(qw._taskNumber()).to.not.equal(oldTaskNumber);
        expect(qw._startState()).to.be.null;
        expect(qw._inProgressState()).to.be.null;
        expect(qw._finishedState()).to.be.null;
        expect(qw._taskTimeout()).to.be.null;
        expect(qw._newTaskRef()).to.be.null;
        expect(qw._newTaskListener()).to.be.null;
        expect(qw._expiryTimeouts).to.deep.equal({});
      });
    });

    it('should reset the worker when called with an invalid task spec after a valid task spec with everythin', () => {
      ['', 'foo', NaN, Infinity, true, false, null, undefined, 0, -1, 10, ['foo', 'bar'], { foo: 'bar' }, { foo: 'bar' }, { foo: { bar: { baz: true } } }, _.noop].forEach(function(invalidTaskSpec) {
        qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
        qw.setTaskSpec(th.validTaskSpecWithEverything);
        var oldTaskNumber = qw._taskNumber();
        qw.setTaskSpec(invalidTaskSpec);
        expect(qw._taskNumber()).to.not.equal(oldTaskNumber);
        expect(qw._startState()).to.be.null;
        expect(qw._inProgressState()).to.be.null;
        expect(qw._finishedState()).to.be.null;
        expect(qw._taskTimeout()).to.be.null;
        expect(qw._newTaskRef()).to.be.null;
        expect(qw._newTaskListener()).to.be.null;
        expect(qw._expiryTimeouts).to.deep.equal({});
      });
    });

    it('should reset a worker when called with a basic valid task spec', () => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      var oldTaskNumber = qw._taskNumber();
      qw.setTaskSpec(th.validBasicTaskSpec);
      expect(qw._taskNumber()).to.not.equal(oldTaskNumber);
      expect(qw._startState()).to.be.null;
      expect(qw._inProgressState()).to.equal(th.validBasicTaskSpec.inProgressState);
      expect(qw._finishedState()).to.be.null;
      expect(qw._taskTimeout()).to.be.null;
      expect(qw._newTaskRef()).to.have.property('on').and.be.a('function');
      expect(qw._newTaskListener()).to.be.a('function');
      expect(qw._expiryTimeouts).to.deep.equal({});
    });

    it('should reset a worker when called with a valid task spec with a startState', () => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      var oldTaskNumber = qw._taskNumber();
      qw.setTaskSpec(th.validTaskSpecWithStartState);
      expect(qw._taskNumber()).to.not.equal(oldTaskNumber);
      expect(qw._startState()).to.equal(th.validTaskSpecWithStartState.startState);
      expect(qw._inProgressState()).to.equal(th.validTaskSpecWithStartState.inProgressState);
      expect(qw._finishedState()).to.be.null;
      expect(qw._taskTimeout()).to.be.null;
      expect(qw._newTaskRef()).to.have.property('on').and.be.a('function');
      expect(qw._newTaskListener()).to.be.a('function');
      expect(qw._expiryTimeouts).to.deep.equal({});
    });

    it('should reset a worker when called with a valid task spec with a finishedState', () => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      var oldTaskNumber = qw._taskNumber();
      qw.setTaskSpec(th.validTaskSpecWithFinishedState);
      expect(qw._taskNumber()).to.not.equal(oldTaskNumber);
      expect(qw._startState()).to.be.null;
      expect(qw._inProgressState()).to.equal(th.validTaskSpecWithFinishedState.inProgressState);
      expect(qw._finishedState()).to.equal(th.validTaskSpecWithFinishedState.finishedState);
      expect(qw._taskTimeout()).to.be.null;
      expect(qw._newTaskRef()).to.have.property('on').and.be.a('function');
      expect(qw._newTaskListener()).to.be.a('function');
      expect(qw._expiryTimeouts).to.deep.equal({});
    });

    it('should reset a worker when called with a valid task spec with a timeout', () => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      var oldTaskNumber = qw._taskNumber();
      qw.setTaskSpec(th.validTaskSpecWithTimeout);
      expect(qw._taskNumber()).to.not.equal(oldTaskNumber);
      expect(qw._startState()).to.be.null;
      expect(qw._inProgressState()).to.equal(th.validTaskSpecWithTimeout.inProgressState);
      expect(qw._finishedState()).to.be.null;
      expect(qw._taskTimeout()).to.equal(th.validTaskSpecWithTimeout.timeout);
      expect(qw._newTaskRef()).to.have.property('on').and.be.a('function');
      expect(qw._newTaskListener()).to.be.a('function');
      expect(qw._expiryTimeouts).to.deep.equal({});
    });

    it('should reset a worker when called with a valid task spec with everything', () => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      var oldTaskNumber = qw._taskNumber();
      qw.setTaskSpec(th.validTaskSpecWithEverything);
      expect(qw._taskNumber()).to.not.equal(oldTaskNumber);
      expect(qw._startState()).to.equal(th.validTaskSpecWithEverything.startState);
      expect(qw._inProgressState()).to.equal(th.validTaskSpecWithEverything.inProgressState);
      expect(qw._finishedState()).to.equal(th.validTaskSpecWithEverything.finishedState);
      expect(qw._taskTimeout()).to.equal(th.validTaskSpecWithEverything.timeout);
      expect(qw._newTaskRef()).to.have.property('on').and.be.a('function');
      expect(qw._newTaskListener()).to.be.a('function');
      expect(qw._expiryTimeouts).to.deep.equal({});
    });

    it('should not pick up tasks on the queue not for the current task', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validBasicTaskSpec);
      var spy = sinon.spy(qw, '_tryToProcess');
      tasksRef.once('child_added', () => {
        try {
          expect(qw._tryToProcess).to.not.have.been.called;
          spy.restore();
          done();
        } catch (error) {
          spy.restore();
          done(error);
        }
      });
      tasksRef.push({ '_state': 'other' }, function(error) {
        if (error) {
          return done(error);
        }
        return undefined;
      });
    });

    it('should pick up tasks on the queue with no "_state" when a task is specified without a startState', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validBasicTaskSpec);
      var spy = sinon.spy(qw, '_tryToProcess');
      var ref = tasksRef.push();
      tasksRef.once('child_added', () => {
        try {
          expect(qw._tryToProcess).to.have.been.calledOnce;
          spy.restore();
          done();
        } catch (error) {
          spy.restore();
          done(error);
        }
      });
      ref.set({ 'foo': 'bar' });
    });

    it('should pick up tasks on the queue with the corresponding "_state" when a task is specifies a startState', done => {
      qw = new th.QueueWorkerWithoutProcessingOrTimeouts(tasksRef, '0', true, false, _.noop);
      qw.setTaskSpec(th.validTaskSpecWithStartState);
      var spy = sinon.spy(qw, '_tryToProcess');
      var ref = tasksRef.push();
      tasksRef.once('child_added', () => {
        try {
          expect(qw._tryToProcess).to.have.been.calledOnce;
          spy.restore();
          done();
        } catch (error) {
          spy.restore();
          done(error);
        }
      });
      ref.set({ '_state': th.validTaskSpecWithStartState.startState });
    });
  });

  describe('#shutdown', () => {
    var qw;
    var callbackStarted;
    var callbackComplete;

    beforeEach(() => {
      callbackStarted = false;
      callbackComplete = false;
      qw = new th.QueueWorker(tasksRef, '0', true, false, function(data, progress, resolve) {
        callbackStarted = true;
        setTimeout(() => {
          callbackComplete = true;
          resolve();
        }, 500);
      });
    });

    afterEach(() => {
      qw.setTaskSpec();
    });

    it('should shutdown a worker not processing any tasks', () => {
      return qw.shutdown().should.eventually.be.fulfilled;
    });

    it('should shutdown a worker after the current task has finished', done => {
      qw.setTaskSpec(th.validBasicTaskSpec);
      tasksRef.push({
        foo: 'bar'
      }, function(errorA) {
        if (errorA) {
          return done(errorA);
        }
        return setTimeout(() => {
          try {
            expect(callbackStarted).to.be.true;
            expect(callbackComplete).to.be.false;
            qw.shutdown().then(() => {
              expect(callbackComplete).to.be.true;
            }).should.eventually.be.fulfilled.notify(done);
          } catch (errorB) {
            done(errorB);
          }
        }, 500);
      });
    });

    it('should return the same shutdown promise if shutdown is called twice', done => {
      qw.setTaskSpec(th.validBasicTaskSpec);
      tasksRef.push({
        foo: 'bar'
      }, function(errorA) {
        if (errorA) {
          return done(errorA);
        }
        try {
          var firstPromise = qw.shutdown();
          var secondPromise = qw.shutdown();
          expect(firstPromise).to.deep.equal(secondPromise);
          return done();
        } catch (errorB) {
          return done(errorB);
        }
      });
    });
  });
});
