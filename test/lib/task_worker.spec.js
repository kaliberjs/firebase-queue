'use strict';

const Helpers = require('../helpers')
const chai = require('chai')

const { expect } = chai

const { TaskWorker, serverNow, serverOffset, nonPlainObjects } = new Helpers()

const SERVER_TIMESTAMP = {'.sv': 'timestamp'}

describe('TaskWorker', () => {

  describe('#reset', () => {

    it('should not reset a task that no longer exists and explicitly return null', () => {
      const tw = new TaskWorker({ spec: {} })
      const result = tw.reset(null)
      expect(result).to.be.null
    })

    it('should reset a task that is currently in progress and owned by the TaskWorker', () => {
      const spec = { inProgressState: 'inProgress', startState: 'start' }
      const tw = new TaskWorker({ owner: 'owner', spec })

      const result = tw.reset({ 
        _state: spec.inProgressState,
        _owner: 'owner',
        _progress: 10,
        _state_changed: 1,
        _error_details: {},
        prop: 'value'
      })

      expect(result).to.deep.equal({
        _state: spec.startState,
        _owner: null,
        _progress: null,
        _state_changed: SERVER_TIMESTAMP,
        _error_details: null,
        prop: 'value'
      })

      expect(result).to.have.property('_state').that.equals(spec.startState)
      expect(result).to.have.property('_owner').that.is.null
      expect(result).to.have.property('_progress').that.is.null
      expect(result).to.have.property('_state_changed').that.deep.equals(SERVER_TIMESTAMP)
      expect(result).to.have.property('_error_details').that.is.null
      expect(result).to.have.property('prop').that.equals('value')
    })

    it('should not reset a task that is not owned by the TaskWorker', () => {
      const tw = new TaskWorker({ owner: 'us', spec: {} })
      const result = tw.reset({ _owner: 'them' })
      expect(result).to.be.undefined
    })

    it('should not reset a task if it is not in progress', () => {
      const tw = new TaskWorker({ owner: 'owner', spec: { inProgressState: 'inProgress'} })
      const result = tw.reset({ _owner: 'owner', _state: 'notInProgress' })
      expect(result).to.be.undefined
    })

    it('should not reset a task if it has no state', () => {
      const tw = new TaskWorker({ owner: 'owner', spec: { inProgressState: 'inProgress'} })
      const result = tw.reset({ _owner: 'owner' })
      expect(result).to.be.undefined
    })
    
  })

  describe('#resetIfTimedOut', () => {
    it('should not reset a task that no longer exists and explicitly return null', () => {
      const tw = new TaskWorker({ spec: {} })
      const result = tw.resetIfTimedOut(null)
      expect(result).to.be.null
    })

    it('should reset a task that is currently in progress that has timed out', () => {
      const spec = { inProgressState: 'inProgress', startState: 'start', timeout: 1000 }
      const tw = new TaskWorker({ serverOffset, spec })
      const result = tw.resetIfTimedOut({ 
        _state: spec.inProgressState,
        _owner: 'owner',
        _progress: 10,
        _state_changed: serverNow() - spec.timeout,
        _error_details: {},
        prop: 'value'
      })

      expect(result).to.have.property('_state').that.equals(spec.startState)
      expect(result).to.have.property('_owner').that.is.null
      expect(result).to.have.property('_progress').that.is.null
      expect(result).to.have.property('_state_changed').that.deep.equals(SERVER_TIMESTAMP)
      expect(result).to.have.property('_error_details').that.is.null
      expect(result).to.have.property('prop').that.equals('value')
    })

    it('should not reset a task if it is not in progress', () => {
      const spec = { inProgressState: 'inProgress', timeout: 1000 }
      const tw = new TaskWorker({ serverOffset, spec })
      const result = tw.resetIfTimedOut({ _state: 'notInProgress', _state_changed: serverNow() - spec.timeout })
      expect(result).to.be.undefined
    })

    it('should not reset a task if it has no state', () => {
      const spec = { inProgressState: 'inProgress', timeout: 1000 }
      const tw = new TaskWorker({ serverOffset, spec })
      const result = tw.resetIfTimedOut({ _state_changed: serverNow() - spec.timeout })
      expect(result).to.be.undefined
    })

    it('should not reset a task if it is has changed state recently', () => {
      const spec = { inProgressState: 'inProgress', timeout: 1000 }
      const tw = new TaskWorker({ serverOffset, spec })
      const result = tw.resetIfTimedOut({ _state: spec.inProgressState, _state_changed: serverNow() })
      expect(result).to.be.undefined
    })
  })

  describe('#resolveWith', () => {

    function basicResolveResult({ finishedState }) {
      return {
        _progress: 100,
        _state: finishedState,
        _state_changed: SERVER_TIMESTAMP,
        _error_details: null,
        _owner: null
      }
    }

    it('should not reset a task that no longer exists and explicitly return null', () => {
      const tw = new TaskWorker({ spec: {} })
      const result = tw.resolveWith(undefined)(null)
      expect(result).to.be.null
    })

    it('should resolve a task in progress and owned by the current worker and remove it when no finishedState is specified', () => {
      const spec = { inProgressState: 'inProgress' }
      const tw = new TaskWorker({ owner: 'owner', spec })
      const result = tw.resolveWith(undefined)({ _owner: 'owner', _state: spec.inProgressState })
      expect(result).to.be.null
    })

    it('should resolve a task owned by the current worker and change the state when a finishedState is specified and no object passed', () => {
      const spec = { inProgressState: 'inProgress', finishedState: 'finished' }
      const tw = new TaskWorker({ owner: 'owner', spec })
      const result = tw.resolveWith(undefined)({ _owner: 'owner', _state: spec.inProgressState, _error_details: {} })

      expect(result).to.deep.equal(basicResolveResult({ finishedState: spec.finishedState }))
    })


    nonPlainObjects.forEach(nonPlainObject =>
      it('should resolve an task owned by the current worker and change the state when a finishedState is specified and an invalid object ' + nonPlainObject + ' passed', () => {
        const spec = { inProgressState: 'inProgress', finishedState: 'finished' }
        const tw = new TaskWorker({ owner: 'owner', spec })
        const result = tw.resolveWith(nonPlainObject)({ _state: spec.inProgressState, _owner: 'owner' })

        expect(result).to.deep.equal(basicResolveResult({ finishedState: spec.finishedState }))
      })
    )

    it('should resolve a task owned by the current worker and change the state when a finishedState is specified and a plain object passed', () => {
      const spec = { inProgressState: 'inProgress', finishedState: 'finished' }
      const tw = new TaskWorker({ owner: 'owner', spec })
      const newTask = { foo: 'bar' }
      const result = tw.resolveWith(newTask)({ _state: spec.inProgressState, _owner: 'owner' })

      const expected = basicResolveResult({ finishedState: spec.finishedState })
      expected.foo = 'bar'

      expect(result).to.deep.equal(expected)

      expect(newTask).to.deep.equal({ foo: 'bar' })
    })

    it('should resolve a task owned by the current worker and change the state to a provided valid string _new_state', () => {
      const spec = { inProgressState: 'inProgress', finishedState: 'finished' }
      const tw = new TaskWorker({ owner: 'owner', spec })
      const result = tw.resolveWith({ foo: 'bar', _new_state: 'valid_new_state' })({ _state: spec.inProgressState, _owner: 'owner' })

      const expected = basicResolveResult({ finishedState: 'valid_new_state' })
      expected.foo = 'bar'

      expect(result).to.deep.equal(expected)
    })

    it('should resolve a task owned by the current worker and change the state to a provided valid null _new_state for a spec with finishedState', () => {
      const spec = { inProgressState: 'inProgress', finishedState: 'finished' }
      const tw = new TaskWorker({ owner: 'owner', spec })
      const result = tw.resolveWith({ foo: 'bar', _new_state: null })({ _state: spec.inProgressState, _owner: 'owner' })

      const expected = basicResolveResult({ finishedState: null })
      expected.foo = 'bar'

      expect(result).to.deep.equal(expected)
    })

    it('should resolve a task owned by the current worker and change the state to a provided valid null _new_state for a spec without finishedState', () => {
      const spec = { inProgressState: 'inProgress' }
      const tw = new TaskWorker({ owner: 'owner', spec })
      const result = tw.resolveWith({ foo: 'bar', _new_state: null })({ _state: spec.inProgressState, _owner: 'owner' })

      const expected = basicResolveResult({ finishedState: null })
      expected.foo = 'bar'

      expect(result).to.deep.equal(expected)
    })

    it('should resolve a task owned by the current worker and remove the task when provided _new_state = false', () => {
      const spec = { inProgressState: 'inProgress', finishedState: 'finished' }
      const tw = new TaskWorker({ owner: 'owner', spec })
      const result = tw.resolveWith({ foo: 'bar', _new_state: false })({ _state: spec.inProgressState, _owner: 'owner' })

      expect(result).to.be.null
    })

    it('should resolve a task owned by the current worker and change the state to finishedState when provided an invalid _new_state', () => {
      const spec = { inProgressState: 'inProgress', finishedState: 'finished' }
      const tw = new TaskWorker({ owner: 'owner', spec })
      const result = tw.resolveWith({ foo: 'bar', _new_state: { state: 'object_is_an_invalid_new_state' } })({ _state: spec.inProgressState, _owner: 'owner' })

      const expected = basicResolveResult({ finishedState: spec.finishedState })
      expected.foo = 'bar'

      expect(result).to.deep.equal(expected)
    })

    it('should resolve a task owned by the current worker and remove the task when provided an invalid _new_state when finishedState is absent', () => {
      const spec = { inProgressState: 'inProgress' }
      const tw = new TaskWorker({ owner: 'owner', spec })
      const result = tw.resolveWith({ foo: 'bar', _new_state: { state: 'object_is_an_invalid_new_state' } })({ _state: spec.inProgressState, _owner: 'owner' })

      expect(result).to.be.null
    })

    it('should not resolve a task if it is no longer owned by the current worker', () => {
      const spec = { inProgressState: 'inProgress', finishedState: 'finished' }
      const tw = new TaskWorker({ owner: 'us', spec })
      const result = tw.resolveWith({ foo: 'bar' })({ _state: spec.inProgressState, _owner: 'them' })
      expect(result).to.be.undefined
    })

    it('should not resolve a task if it is no longer owned by the current worker and if it would otherwise be deleted', () => {
      const spec = { inProgressState: 'inProgress' }
      const tw = new TaskWorker({ owner: 'us', spec })
      const result = tw.resolveWith({ foo: 'bar' })({ _state: spec.inProgressState, _owner: 'them' })
      expect(result).to.be.undefined
    })

    it('should not resolve a task if it is not in progress', () => {
      const spec = { inProgressState: 'inProgress', finishedState: 'finished' }
      const tw = new TaskWorker({ owner: 'owner', spec })
      const result = tw.resolveWith({ foo: 'bar' })({ _state: 'notInProgress', _owner: 'owner' })
      expect(result).to.be.undefined
    })

    it('should not resolve a task if it is has no state', () => {
      const spec = { inProgressState: 'inProgress', finishedState: 'finished' }
      const tw = new TaskWorker({ owner: 'owner', spec })
      const result = tw.resolveWith({ foo: 'bar' })({ _owner: 'owner' })
      expect(result).to.be.undefined
    })
  })
})
