'use strict';

const _ = require('lodash')
const Helpers = require('./helpers.js')
const chai = require('chai')
const chaiAsPromised = require('chai-as-promised')

chai.use(chaiAsPromised).should()
const { expect } = chai

const th = new Helpers()

const nonFirebaseObjects             = ['', 'foo', NaN, Infinity, true, false, 0, 1,     ['foo', 'bar'], { foo: 'bar' }, null, { foo: { bar: { baz: true } } }, _.noop]
const nonFunctionObjects             = ['', 'foo', NaN, Infinity, true, false, 0, 1,     ['foo', 'bar'], { foo: 'bar' }, null, { foo: { bar: { baz: true } } }        ]
const nonPlainObjects                = ['', 'foo', NaN, Infinity, true, false, 0, 1,     ['foo', 'bar'],                 null,                                  _.noop]
const nonStringObjects               = [           NaN, Infinity, true, false, 0, 1,     ['foo', 'bar'], { foo: 'bar' }, null, { foo: { bar: { baz: true } } }, _.noop]
const nonPositiveIntigerObjects      = ['', 'foo', NaN, Infinity, true, false, 0,    -1, ['foo', 'bar'], { foo: 'bar' }, null, { foo: { bar: { baz: true } } }, _.noop]
const nonBooleanObjects              = ['', 'foo', NaN, Infinity,              0, 1,     ['foo', 'bar'], { foo: 'bar' }, null, { foo: { bar: { baz: true } } }, _.noop]
const invalidRefConfigurationObjects = [{}, { foo: 'bar' }, { specsRef: th.testRef }]
const bools = [true, false]

function queueWorkerSpy({ init = () => {}, start = () => {}, shutdown = () => Promise.resolve() }) {
  return function QueueWorkerSpy(options) {
    init(options)
    this.start = start
    this.shutdown = () => Promise.resolve(shutdown())
  }
}

function allways(f, callback) {
  const stack = new Error().stack
  return new Promise(r => r(f())).then(_ => callback()).catch(e => Promise.resolve(callback()).then(_ => (e.stack = stack, Promise.reject(e))))
}

function withQueue({ ref, options = {}, processingFunction = _.noop }, f) {
  return withRef(ref, ref => {
    const q = new th.Queue(ref, options, processingFunction)
    return allways(() => f(q), q.shutdown)
  })
}

function withRef(ref = th.testRef, f) {
  return allways(() => f(ref), () => ref.off && (ref.off(), ref.set(null)))
}

function wait(time) {
  return new Promise(r => setTimeout(r, time))
}

describe.only('Queue', () => {

  describe('initialize', () => {

    nonPlainObjects.forEach(nonPlainObject => {
      it('should not create a Queue with a Firebase reference, a non-plain object options parameter (' + JSON.stringify(nonPlainObject) + '), and a processingCallback', () => {
        expect(() => { new th.Queue(th.testRef, nonPlainObject, _.noop) })
          .to.throw('Options parameter must be a plain object.')
      })
    })

    it('should not create a Queue with only a queue reference', () => {
      expect(() => { new th.Queue(th.testRef) })
        .to.throw('Queue must at least have the queueRef and processingFunction arguments.')
    })

    it('should not create a Queue when initialized with 4 parameters', () => {
      expect(() => { new th.Queue(th.testRef, {}, _.noop, null) })
        .to.throw('Queue can only take at most three arguments - queueRef, options (optional), and processingFunction.')
    })

    it('should create a default Queue with just a Firebase reference and a processing callback', () =>
      new th.Queue(th.testRef, _.noop).shutdown()
    )

    it('should create a default Queue with a Firebase reference, an empty options object, and a processing callback', () =>
      new th.Queue(th.testRef, {}, _.noop).shutdown()
    )

    nonPositiveIntigerObjects.forEach(nonPositiveIntigerObject => {
      it('should not create a Queue with a non-positive integer numWorkers specified ' + JSON.stringify(nonPositiveIntigerObject), () => {
        expect(() => { new th.Queue(th.testRef, { numWorkers: nonPositiveIntigerObject }, _.noop) })
          .to.throw('options.numWorkers must be a positive integer.')
      })
    })

    _.range(1, 20).forEach(numWorkers => {
      it('should create a Queue with ' + numWorkers + ' workers when specified in options.numWorkers', () => {
        let instanceCount = 0
        const QueueWorker = queueWorkerSpy({ init: _ => instanceCount++ })
        
        return withQueue({ options: { numWorkers: numWorkers, QueueWorker } }, q => {
          expect(q.getWorkerCount()).to.equal(numWorkers)
          expect(instanceCount).to.equal(numWorkers)
        })
      })
    })

    it('should create a single worker if no number of workers is specified', () => {
      let instanceCount = 0
      const QueueWorker = queueWorkerSpy({ init: _ => instanceCount++ })

      return withQueue({ options: { QueueWorker }}, q => {
        expect(q.getWorkerCount()).to.equal(1)
        expect(instanceCount).to.equal(1)
      })
    })

    const invalidSpecIds = ['', ...nonStringObjects]
    invalidSpecIds.forEach(nonStringObject => {
      it('should not create a Queue with an invalid specId specified ' + JSON.stringify(nonStringObject), () => {
        expect(() => { new th.Queue(th.testRef, { specId: nonStringObject }, _.noop) })
          .to.throw('options.specId must be a String.')
      })
    })

    it('should set the corerct processIdBase on QueueWorkers when a specId is specified', () => {
      let processIdBase = null
      const QueueWorker = queueWorkerSpy({ init: ({ processIdBase: p }) => processIdBase = p })
      const specId = 'test_task'

      return withQueue({ options: { specId, QueueWorker }}, q => 
        th.waitFor(_ => !!processIdBase).then(_ => { expect(processIdBase).to.equal(specId + ':0') })
      )
    })

    it('should set the corerct processIdBase on QueueWorkers when no specId is specified', () => {
      let processIdBase = null
      const QueueWorker = queueWorkerSpy({ init: ({ processIdBase: p }) => processIdBase = p })

      return withQueue({ options: { QueueWorker }}, q => 
        th.waitFor(_ => !!processIdBase).then(_ => { expect(processIdBase).to.equal('0') })
      )
    })

    nonBooleanObjects.forEach(nonBooleanObject => {
      it('should not create a Queue with a non-boolean sanitize option specified', () => {
        expect(() => { new th.Queue(th.testRef, { sanitize: nonBooleanObject }, _.noop) })
          .to.throw('options.sanitize must be a boolean.')
      })
    })

    bools.forEach(bool => {
      it('should create a Queue with a ' + bool + ' sanitize option when specified', () => {
        let sanitize = null
        const QueueWorker = queueWorkerSpy({ init: ({ sanitize: s }) => sanitize = s })

        return withQueue({ options: { sanitize: bool, QueueWorker }}, q => {
          expect(sanitize).to.equal(bool)
        })
      })
    })

    it('should set sanitize to true by default', () => {
      let sanitize = null
      const QueueWorker = queueWorkerSpy({ init: ({ sanitize: s }) => sanitize = s })

      return withQueue({ options: { QueueWorker }}, q => {
        expect(sanitize).to.be.true
      })
    })

    nonBooleanObjects.forEach(nonBooleanObject => {
      it('should not create a Queue with a non-boolean suppressStack option specified', () => {
        expect(() => { new th.Queue(th.testRef, { suppressStack: nonBooleanObject }, _.noop) })
          .to.throw('options.suppressStack must be a boolean.')
      })
    })

    bools.forEach(bool => {
      it('should create a Queue with a ' + bool + ' suppressStack option when specified', () => {
        let suppressStack = null
        const QueueWorker = queueWorkerSpy({ init: ({ suppressStack: s }) => suppressStack = s })

        return withQueue({ options: { suppressStack: bool, QueueWorker }}, q => {
          expect(suppressStack).to.equal(bool)
        })
      })
    })

    it('should set suppressStack to galse by default', () => {
      let suppressStack = null
      const QueueWorker = queueWorkerSpy({ init: ({ suppressStack: s }) => suppressStack = s })

      return withQueue({ options: { QueueWorker }}, q => {
        expect(suppressStack).to.be.false
      })
    })

    invalidRefConfigurationObjects.forEach(invalidRefConfigurationObject => {
      it('should not create a Queue with a ref configuration object that contains keys: {' + Object.keys(invalidRefConfigurationObject).join(', ') + '}', () => {
        expect(() => { new th.Queue(invalidRefConfigurationObject, _.noop) })
          .to.throw('When ref is an object it must contain the key \'tasksRef\'')
      })
    })

    const invalidRefConfigurationWhenSpecIdIsSet = [{ tasksRef: th.testRef }, ...invalidRefConfigurationObjects]
    invalidRefConfigurationWhenSpecIdIsSet.forEach(invalidRefConfigurationObject => {
      it('should not create a Queue when a specId is set with a ref configuration object that contains keys: {' + Object.keys(invalidRefConfigurationObject).join(', ') + '}', () => {
        expect(() => { new th.Queue(invalidRefConfigurationObject, { specId: 'test_task' }, _.noop) })
          .to.throw('When ref is an object and \'specId\' is given it must contain both keys \'tasksRef\' and \'specsRef\'')
      })
    })

    nonFirebaseObjects.forEach(nonFirebaseObject => {
      it('should not create a Queue with a non-firebase object: ' + JSON.stringify(nonFirebaseObject), () => {
        expect(() => { new th.Queue(nonFirebaseObject, _.noop) }).to.throw
      })
    })

    it('should pass a given tasks reference to QueueWorker', () => {
      let tasksRef = null
      const QueueWorker = queueWorkerSpy({ init: ({ tasksRef: t }) => tasksRef = t })
      
      return withRef(th.tasksRef, testRef => 
        testRef.set('value').then(_ => 
          withQueue({ ref: { tasksRef: testRef }, options: { QueueWorker } }, q =>
            th.waitFor(() => !!tasksRef)
              .then(_ => tasksRef.once('value'))
              .then(snapshot => { expect(snapshot.val()).to.equal('value') })
          )
        )
      )
    })
    
    it('pickup tasks from the `tasks` child of the given ref', () => {
      let tasksRef = null
      const QueueWorker = queueWorkerSpy({ init: ({ tasksRef: t }) => tasksRef = t })
      
      return withRef(th.tasksRef, testRef =>  
        testRef.child('tasks').set('value').then(_ => 
          withQueue({ ref: testRef, options: { QueueWorker } }, q =>
            th.waitFor(() => !!tasksRef)
              .then(_ => tasksRef.once('value'))
              .then(snapshot => { expect(snapshot.val()).to.equal('value') })
          )
        )
      )
    })

    nonFunctionObjects.forEach(nonFunctionObject => {
      it('should not create a Queue with a non-function callback: ' + JSON.stringify(nonFunctionObject), () => {
        expect(() => { new th.Queue(th.testRef, nonFunctionObject) })
          .to.throw('No processing function provided.')
      })
    })

    it('should set a default task spec if no `specId` was given', () => {
      let spec = null
      const QueueWorker = queueWorkerSpy({ init: ({ spec: s }) => spec = s })

      return withQueue({ options: { QueueWorker }}, q =>
        th.waitFor(() => !!spec).then(_ => {
          expect(spec).to.deep.equal({ inProgressState: 'in_progress', timeout: 300000 })
        })
      )
    })

    it('should pass on the correct processingFunction', () => {
      let processingFunction = null
      function test() {}
      const QueueWorker = queueWorkerSpy({ init: ({ processingFunction: p }) => processingFunction = p })

      return withQueue({ options: { QueueWorker }, processingFunction: test }, q =>
        th.waitFor(() => !!processingFunction).then(_ => {
          expect(processingFunction).to.deep.equal(test)
        })
      )
    })

    it('should start the QueueWorker', () => {
      let started = false
      const QueueWorker = queueWorkerSpy({ start: () => started = true })

      return withQueue({ options: { QueueWorker }}, q =>
        expect(started).to.be.true
      )
    })

    it('should not instantly create workers when a `specId` is given', () => {
      let queueWorkers = 0
      const QueueWorker = queueWorkerSpy({ init: _ => queueWorkers++ })

      return withQueue({ options: { QueueWorker, specId: 'something' }}, q => {
        expect(queueWorkers).to.equal(0)
      })
    })

    it('should pass a null-filled spec to the worker when a `spedId` is given', () => {
      let spec = null
      const QueueWorker = queueWorkerSpy({ init: ({ spec: s }) => spec = s })

      return withQueue({ options: { QueueWorker, specId: 'something' }}, q =>
        th.waitFor(() => !!spec).then(_ => {
          expect(spec).to.deep.equal({
            startState: null,
            inProgressState: null,
            finishedState: null,
            errorState: null,
            retries: null,
            timeout: null
          })
        })
      )
    })

    it('should shutdown pre-existing workers on a spec change', () => {
      let shutdownCalled = false
      let initialized = false
      const QueueWorker = queueWorkerSpy({ init: _ => initialized = true, shutdown: () => shutdownCalled = true })
      const specId = 'test_spec'

      return withQueue({ ref: th.tasksRef, options: { specId, QueueWorker }}, q =>
        th.waitFor(() => initialized)
          .then(_ => {
            expect(shutdownCalled).to.be.false
            return th.tasksRef.child('specs').child(specId).set({ foo: 'bar' })
          })
          .then(_ => th.waitFor(() => shutdownCalled))
      )
    })

    it('should not create new workers before shutting down the old ones when the spec changes', () => {
      let initialized = false
      const QueueWorker = queueWorkerSpy({ init: _ => initialized = true, shutdown: () => wait(400) })
      const specId = 'test_spec'

      return withQueue({ ref: th.tasksRef, options: { specId, QueueWorker }}, q =>
        th.waitFor(() => initialized)
          .then(_ => {
            initialized = false
            return th.tasksRef.child('specs').child(specId).set({ foo: 'bar' })
          })
          .then(_ => { expect(initialized).to.be.false })
          .then(_ => th.waitFor(() => initialized))
      )
    })

    it('should have the correct worker count after a spec change', () => {
      let initialized = false
      const QueueWorker = queueWorkerSpy({ init: _ => initialized = true })
      const specId = 'test_spec'

      return withQueue({ ref: th.tasksRef, options: { specId, QueueWorker }}, q =>
        th.waitFor(() => initialized)
          .then(_ => {
            initialized = false
            return th.tasksRef.child('specs').child(specId).set({ foo: 'bar' })
          })
          .then(_ => th.waitFor(() => initialized))
          .then(_ => expect(q.getWorkerCount()).to.equal(1))
      )
    })

    it('should pass the correct spec to the worker', () => {
      let spec = null
      const QueueWorker = queueWorkerSpy({ init: ({ spec: s }) => spec = s })
      const specId = 'test_spec'

      const originalSpec = {
        start_state: 'a',
        in_progress_state: 'b',
        finished_state: 'c',
        error_state: 'd',
        retries: 'e',
        timeout: 'f'
      }

      const expectedSpec = {
        startState: 'a',
        inProgressState: 'b',
        finishedState: 'c',
        errorState: 'd',
        retries: 'e',
        timeout: 'f'
      }

      return th.tasksRef.child('specs').child(specId).set(originalSpec).then(_ =>
        withQueue({ ref: th.tasksRef, options: { specId, QueueWorker } }, q =>
          th.waitFor(() => !!spec).then(_ => {
            expect(spec).to.deep.equal(expectedSpec)
          })
        )
      ) 
    })

    // it.skip('should listen to changes of the specId', () => {})
    // it.skip('should set the correct spec if given a specId', () => {})
    // it.skip('should do xyz when the spec is changed to an invalid one', () => {})

  })

  // describe('#addWorker', () => {
  //   // addWorker semantics will probably change a bit, in theory the spec might not be available yet

  //   it('should add worker', () => {
  //     let queueWorkers = 0
  //     const QueueWorker = queueWorkerSpy({ init: _ => queueWorkers++ })

  //     return withQueue({ options: { QueueWorker } }, q => {
  //       expect(queueWorkers).to.equal(1)
  //       expect(q.getWorkerCount()).to.equal(1)
  //       q.addWorker()
  //       expect(queueWorkers).to.equal(2)
  //       expect(q.getWorkerCount()).to.equal(2)
  //     })
  //   })

  //   it('should add worker with correct process id', () => {
  //     let processIdBase = null
  //     let queueWorkers = 0
  //     const QueueWorker = queueWorkerSpy({ init: ({ processIdBase: p }) => {
  //       processIdBase = p
  //       queueWorkers++
  //     }})
  //     const specId = 'test_task'
  //     return withQueue({ options: { specId, QueueWorker }}, q => {
  //       q.addWorker()
  //       return th.waitFor(() => queueWorkers === 2)
  //         .then(_ => { expect(processIdBase).to.equal(specId + ':1') })
  //     })
  //   })

  //   it ('should correctly handle the situation where the spec is invalid', () => {})

  //   it('should not allow a worker to be added if the queue is shutting down', () => {
  //     let queueWorkers = 0
  //     const QueueWorker = queueWorkerSpy({ init: _ => queueWorkers++ })

  //     return withQueue({ options: { QueueWorker }}, q => {
  //       expect(queueWorkers).to.equal(1)
  //       expect(q.getWorkerCount()).to.equal(1)
  //       q.shutdown()
  //       expect(() => { q.addWorker() }).to.throw('Cannot add worker while queue is shutting down')
  //       expect(queueWorkers).to.equal(1)
  //     })
  //   })
  // })

  // describe('#removeWorker', () => {

  //   it('should remove worker', () => {
  //     return withQueue({}, q => {
  //       expect(q.getWorkerCount()).to.equal(1)
  //       q.removeWorker()
  //       expect(q.getWorkerCount()).to.equal(0)
  //     })
  //   })

  //   it('should shutdown worker', () => {
  //     let shutdownCalled = false
  //     const QueueWorker = queueWorkerSpy({ shutdown: () => shutdownCalled = true })

  //     return withQueue({ options: { QueueWorker }}, q => {
  //       const workerRemovedPromise = q.removeWorker()
  //       expect(shutdownCalled).to.be.true
  //       return workerRemovedPromise
  //     })
  //   })

  //   it('should have the correct amount of workers after a spec change', () => {
  //     const specId = 'test_spec'
  //     let initialized = false
  //     const QueueWorker = queueWorkerSpy({ init: _ => initialized = true })

  //     return withQueue({ ref: th.testRef, options: { specId, QueueWorker } }, q =>
  //       th.waitFor(() => initialized)
  //         .then(_ => {
  //           q.addWorker()
  //           q.removeWorker()
  //           expect(q.getWorkerCount()).to.equal(1)
  //           return th.testRef.child('specs').child(specId).set(th.validBasicTaskSpec)
  //         })
  //         .then(_ => { expect(q.getWorkerCount()).to.equal(1) })
  //     )
  //   })

  //   it('should reject when no workers remaining', () => 
  //     withQueue({}, q => {
  //       q.removeWorker()
  //       return q.removeWorker().catch(error => {
  //         expect(error.message).to.equal('No workers to shutdown')
  //       })
  //     })
  //   )
  // })

  describe('#shutdown', () => {

  //   it('should shutdown a queue initialized with the default spec', () => {
  //     let shutdownCalled = false
  //     let startCalled = false
  //     const QueueWorker = queueWorkerSpy({ 
  //       start: () => startCalled = true,
  //       shutdown: () => shutdownCalled = true
  //     })

  //     return withQueue({ options: { QueueWorker } }, q => {
  //       expect(startCalled).to.be.true
  //       q.shutdown().then(_ => { expect(shutdownCalled).to.be.true })
  //     })
  //   })

  //   it('should shutdown a queue initialized with a custom spec before the value is processed', () => {
  //     let startCalled = false
  //     const QueueWorker = queueWorkerSpy({ start: () => startCalled = true })

  //     return withQueue({ options: { specId: 'test_task', QueueWorker }}, q => {
  //       expect(startCalled).to.be.false
  //       return q.shutdown()
  //     })
  //   })

    it('should stop listening to spec changes', () => {
      let startCalled = false
      const QueueWorker = queueWorkerSpy({ start: () => startCalled = true })
      const specId = 'test_task'

      return withQueue({ ref: th.testRef, options: { specId, QueueWorker }}, q =>
        th.waitFor(() => startCalled)
          .then(_ => q.shutdown())
          .then(_ => startCalled = false)
          .then(_ => th.testRef.child('specs').child(specId).set({ foo: 'bar' }))
          .then(_ => { expect(startCalled).to.be.false })
      )
    })
  })
})
