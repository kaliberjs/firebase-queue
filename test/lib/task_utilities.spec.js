const Helpers = require('../helpers')
const chai = require('chai')

const { expect } = chai

const { TaskUtilities, withTimeFrozen, withSnapshots, withSnapshot } = new Helpers()

describe('TaskUtilities', () => {

  describe('#isInErrorState', () => {

    it('should not say a task is in error state when it is not', () => {
      const tw = new TaskUtilities({ spec: { errorState: 'error' } })

      return withSnapshots(
        [{ _state: 'notError' }, {}],
        ([t1, t2]) => {
          expect(tw.isInErrorState(t1)).to.be.false
          expect(tw.isInErrorState(t2)).to.be.false
        }
      )
    })

    it('should say a task is in error state when it is', () => {
      const tw = new TaskUtilities({ spec: { errorState: 'error' } })

      return withSnapshot(
        { _state: 'error' },
        t1 => {
          expect(tw.isInErrorState(t1)).to.be.true
        }
      )
    })
  })

  describe('#expiresIn', () => {

    it('should return the value of timeout if the last change was now', () =>
      withTimeFrozen(now =>
        withSnapshot(
          { _state_changed: now },
          snapshot => {
            const tw = new TaskUtilities({ serverOffset: 0, spec: { timeout: 42 } })
            expect(tw.expiresIn(snapshot)).to.equal(42)
          }
        )
      )
    )

    it('should return 0 if the time passed since the last change is bigger than the timeout', () =>
      withTimeFrozen(now =>
        withSnapshot(
          { _state_changed: now - 43 },
          snapshot => {
            const tw = new TaskUtilities({ serverOffset: 0, spec: { timeout: 42 } })
            expect(tw.expiresIn(snapshot)).to.equal(0)
          }
        )
      )
    )

    it('should take server offset into account', () =>
      withTimeFrozen(now =>
        withSnapshot(
          { _state_changed: now },
          snapshot => {
            const tw = new TaskUtilities({ serverOffset: 10, spec: { timeout: 42 } })
            expect(tw.expiresIn(snapshot)).to.equal(32)
          }
        )
      )
    )

    it('should assume now if the task has no recorded state change timestamp', () =>
      withTimeFrozen(_ =>
        withSnapshot(
          {},
          snapshot => {
            const tw = new TaskUtilities({ serverOffset: 10, spec: { timeout: 42 } })
            expect(tw.expiresIn(snapshot)).to.equal(42)
          }
        )
      )
    )

    it('should correctly return the expiration time if it in the past', () =>
      withTimeFrozen(now =>
        withSnapshot(
          { _state_changed: now - 10 },
          snapshot => {
            const tw = new TaskUtilities({ serverOffset: 0, spec: { timeout: 42 } })
            expect(tw.expiresIn(snapshot)).to.equal(32)
          }
        )
      )
    )
  })

  describe('#getOwner', () => {

    it('should return null if no owner was present', () =>
      withSnapshot(
        {  },
        snapshot => {
          const tw = new TaskUtilities({ spec: {} })
          expect(tw.getOwner(snapshot)).to.equal(null)
        }
      )
    )

    it('should return the owner if it is present', () =>
      withSnapshot(
        { _owner: 'owner' },
        snapshot => {
          const tw = new TaskUtilities({ spec: {} })
          expect(tw.getOwner(snapshot)).to.equal('owner')
        }
      )
    )
  })

  describe('#sanitize', () =>

    it('should remove any fields introduced by the task worker methods', () => {
      const tw = new TaskUtilities({ spec: {} })
      const input = {
        _owner: tw.owner,
        _state: 'state',
        _state_changed: 'state changed',
        _progress: 'progress',
        _error_details: 'error details',
        foo: 'bar'
      }
      const expected = { foo: 'bar' }
      const result = tw.sanitize(input)
      expect(result).to.deep.equal(expected)
      expect(input).to.deep.equal(expected)
    })
  )
})
