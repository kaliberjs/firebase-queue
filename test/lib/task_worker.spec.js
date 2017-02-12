'use strict';

const Helpers = require('../helpers')
const chai = require('chai')

const { expect } = chai

const { TaskWorker, serverNow, serverOffset } = new Helpers()

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
})
