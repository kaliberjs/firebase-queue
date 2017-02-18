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
const invalidRefConfigurationObjects = [{}, { foo: 'bar' }, { tasksRef: th.testRef }, { specsRef: th.testRef }]
const bools = [true, false]

describe('Queue', () => {

  describe('initialize', () => {
    it('should not create a Queue with only a queue reference', () => {
      expect(() => { new th.Queue(th.testRef) })
        .to.throw('Queue must at least have the queueRef and processingFunction arguments.')
    })

    nonFirebaseObjects.forEach(nonFirebaseObject => {
      it('should not create a Queue with a non-firebase object: ' + JSON.stringify(nonFirebaseObject), () => {
        expect(() => { new th.Queue(nonFirebaseObject, _.noop) })
          .to.throw
      })
    })

    invalidRefConfigurationObjects.forEach(invalidRefConfigurationObject => {
      it('should not create a Queue with a ref configuration object that contains keys: {' + Object.keys(invalidRefConfigurationObject).join(', ') + '}', () => {
        expect(() => { new th.Queue(invalidRefConfigurationObject, { specId: 'test_task' }, _.noop) })
          .to.throw('When ref is an object it must contain both keys \'tasksRef\' and \'specsRef\'')
      })
    })

    it('should create a Queue with a ref configuration object that contains keys: { tasksRef }', () => {
      expect(() => { new th.Queue({ tasksRef: th.testRef }, _.noop) })
        .to.not.throw('When ref is an object it must contain both keys \'tasksRef\' and \'specsRef\'')
    })

    nonFunctionObjects.forEach(nonFunctionObject => {
      it('should not create a Queue with a non-function callback: ' + JSON.stringify(nonFunctionObject), () => {
        expect(() => { new th.Queue(th.testRef, nonFunctionObject) })
          .to.throw('No processing function provided.')
      })
    })

    it('should create a default Queue with just a Firebase reference and a processing callback', () => {
      new th.Queue(th.testRef, _.noop)
    })

    it('should create a default Queue with tasks and specs Firebase references and a processing callback', () => {
      new th.Queue({ tasksRef: th.testRef, specsRef: th.testRef }, _.noop)
    })

    nonPlainObjects.forEach(nonPlainObject => {
      it('should not create a Queue with a Firebase reference, a non-plain object options parameter (' + JSON.stringify(nonPlainObject) + '), and a processingCallback', () => {
        expect(() => { new th.Queue(th.testRef, nonPlainObject, _.noop) })
          .to.throw('Options parameter must be a plain object.')
      })
    })

    it('should create a default Queue with a Firebase reference, an empty options object, and a processing callback', () => {
      new th.Queue(th.testRef, {}, _.noop)
    })

    nonStringObjects.forEach(nonStringObject => {
      it('should not create a Queue with a non-string specId specified ' + JSON.stringify(nonStringObject), () => {
        expect(() => { new th.Queue(th.testRef, { specId: nonStringObject }, _.noop) })
          .to.throw('options.specId must be a String.')
      })
    })

    nonPositiveIntigerObjects.forEach(nonPositiveIntigerObject => {
      it('should not create a Queue with a non-positive integer numWorkers specified ' + JSON.stringify(nonPositiveIntigerObject), () => {
        expect(() => { new th.Queue(th.testRef, { numWorkers: nonPositiveIntigerObject }, _.noop) })
          .to.throw('options.numWorkers must be a positive integer.')
      })
    })

    nonBooleanObjects.forEach(nonBooleanObject => {
      it('should not create a Queue with a non-boolean sanitize option specified', () => {
        expect(() => { new th.Queue(th.testRef, { sanitize: nonBooleanObject }, _.noop) })
          .to.throw('options.sanitize must be a boolean.')
      })
    })

    nonBooleanObjects.forEach(nonBooleanObject => {
      it('should not create a Queue with a non-boolean suppressStack option specified', () => {
        expect(() => { new th.Queue(th.testRef, { suppressStack: nonBooleanObject }, _.noop) })
          .to.throw('options.suppressStack must be a boolean.')
      })
    })

    _.range(1, 20).forEach(numWorkers => {
      it('should create a Queue with ' + numWorkers + ' workers when specified in options.numWorkers', () => {
        let instanceCount = 0
        function QueueWorker() {
          instanceCount++
          this.setTaskSpec = () => {}
        }
        const q = new th.Queue(th.testRef, { numWorkers: numWorkers, QueueWorker }, _.noop)
        expect(q.getWorkerCount()).to.equal(numWorkers)
        expect(instanceCount).to.equal(numWorkers)
      })
    })

    it('should create a Queue with a specific specId when specified', () => {
      let processIdBase = null
      function QueueWorker(_, p) {
        this.setTaskSpec = () => {}
        processIdBase = p
      }
      const specId = 'test_task'
      const q = new th.Queue(th.testRef, { specId: specId, QueueWorker }, _.noop)

      return new Promise(resolve => {
        const interval = setInterval(() => {
          if (processIdBase) {
            clearInterval(interval)
            resolve()
          }
        }, 100)
      }).then(_ => {
        expect(processIdBase).to.equal(specId + ':0')
      })
    })

    it.skip('should listen to changes of the specId', () => {})
    it.skip('should set the correct spec if given a specId', () => {})

    bools.forEach(bool => {
      it('should create a Queue with a ' + bool + ' sanitize option when specified', () => {
        let sanitize = null
        function QueueWorker(_1, _2, s) {
          this.setTaskSpec = () => {}
          sanitize = s
        }
        const q = new th.Queue(th.testRef, { sanitize: bool, QueueWorker }, _.noop)
        expect(sanitize).to.equal(bool)
      })
    })

    bools.forEach(bool => {
      it('should create a Queue with a ' + bool + ' suppressStack option when specified', () => {
        let suppressStack = null
        function QueueWorker(_1, _2, _3, s) {
          this.setTaskSpec = () => {}
          suppressStack = s
        }
        const q = new th.Queue(th.testRef, { suppressStack: bool, QueueWorker }, _.noop)
        expect(suppressStack).to.equal(bool)
      })
    })

    it('should not create a Queue when initialized with 4 parameters', () => {
      expect(() => { new th.Queue(th.testRef, {}, _.noop, null) })
        .to.throw('Queue can only take at most three arguments - queueRef, options (optional), and processingFunction.')
    })
  })

  describe('#addWorker', () => {
    // addWorker semantics will probably change a bit, in theory the spec might not be available yet

    it('should add worker', () => {
      let queueWorkers = 0
      function QueueWorker() {
        this.setTaskSpec = () => {}
        queueWorkers++
      }
      const q = new th.Queue(th.testRef, { QueueWorker }, _.noop)
      expect(queueWorkers).to.equal(1)
      expect(q.getWorkerCount()).to.equal(1)
      q.addWorker()
      expect(queueWorkers).to.equal(2)
      expect(q.getWorkerCount()).to.equal(2)
    })

    it('should add worker with correct process id', () => {
      let processIdBase = null
      let queueWorkers = 0
      function QueueWorker(_, p) {
        this.setTaskSpec = () => {}
        processIdBase = p
        queueWorkers++
      }
      const specId = 'test_task'
      const q = new th.Queue(th.testRef, { specId: specId, QueueWorker }, _.noop)
      const worker = q.addWorker()
      return new Promise(resolve => {
        const interval = setInterval(() => {
          if (queueWorkers === 2) {
            clearInterval(interval)
            resolve()
          }
        }, 100)
      }).then(_ => {
        expect(processIdBase).to.equal(specId + ':1')
      })
    })

    it('should not allow a worker to be added if the queue is shutting down', () => {
      let queueWorkers = 0
      function QueueWorker() {
        this.shutdown = () => {}
        this.setTaskSpec = () => {}
        queueWorkers++
      }
      const q = new th.Queue(th.testRef, { QueueWorker }, _.noop)
      expect(queueWorkers).to.equal(1)
      expect(q.getWorkerCount()).to.equal(1)
      q.shutdown()
      expect(() => { q.addWorker() })
        .to.throw('Cannot add worker while queue is shutting down')
      expect(queueWorkers).to.equal(1)
    })
  })

  describe('#shutdownWorker', () => {

    it('should remove worker', () => {
      const q = new th.Queue(th.testRef, _.noop)
      expect(q.getWorkerCount()).to.equal(1)
      q.shutdownWorker()
      expect(q.getWorkerCount()).to.equal(0)
    });

    it('should shutdown worker', () => {
      let shutdownCalled = false
      function QueueWorker() {
        this.setTaskSpec = () => {}
        this.shutdown = () => { shutdownCalled = true }
      }
      const q = new th.Queue(th.testRef, { QueueWorker }, _.noop)
      expect(q.getWorkerCount()).to.equal(1)
      const workerShutdownPromise = q.shutdownWorker()
      expect(shutdownCalled).to.be.true
      return workerShutdownPromise
    })

    it('should reject when no workers remaining', () => {
      const q = new th.Queue(th.testRef, _.noop)
      expect(q.getWorkerCount()).to.equal(1)
      q.shutdownWorker()
      return q.shutdownWorker().catch(error => {
        expect(error.message).to.equal('No workers to shutdown')
      })
    })
  })

  describe('#shutdown', () => {

    it('should shutdown a queue initialized with the default spec', () => {
      let shutdownCalled = false
      function QueueWorker() {
        this.setTaskSpec = () => {}
        this.shutdown = () => { shutdownCalled = true }
      }
      const q = new th.Queue(th.testRef, { QueueWorker }, _.noop)
      return q.shutdown()
        .then(_ => {
          expect(shutdownCalled).to.be.true
        })
    })

    it('should shutdown a queue initialized with a custom spec before the listener callback', () => {
      let taskSpecSet = false
      function QueueWorker() {
        this.shutdown = () => {}
        this.setTaskSpec = () => { taskSpecSet = true }
      }
      const q = new th.Queue(th.testRef.child('this_needs_to_be_improved'), { specId: 'test_task', QueueWorker }, _.noop)
      expect(taskSpecSet).to.be.false
      return q.shutdown()
    })

    it('should shutdown a queue initialized with a custom spec after the listener callback', () => {
      let taskSpecSet = false
      function QueueWorker() {
        this.shutdown = () => {}
        this.setTaskSpec = () => { taskSpecSet = true }
      }
      const q = new th.Queue(th.testRef, { specId: 'test_task', QueueWorker }, _.noop)

      return new Promise(resolve => {
        const interval = setInterval(() => {
          if (taskSpecSet) {
            clearInterval(interval)
            resolve()
          }
        }, 100)
      }).then(_ => q.shutdown())
        .then(_ => { taskSpecSet = false })
        .then(_ => th.testRef.child('specs').child('test_task').set({ foo: 'bar' }))
        .then(_ => { expect(taskSpecSet).to.be.false })
    })
  })
})
