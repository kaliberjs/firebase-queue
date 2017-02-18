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

describe.only('Queue', () => {

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
        var q = new th.Queue(th.testRef, { numWorkers: numWorkers }, _.noop)
        expect(q.getWorkerCount()).to.equal(numWorkers)
      })
    })

    it('should create a Queue with a specific specId when specified', done => {
      // if we add an injectable constructor for queue worker we an check the passed in
      // progress Id of the worker 
      const specId = 'test_task'
      const q = new th.Queue(th.testRef, { specId: specId }, _.noop);
      expect(q._specId).to.equal(specId);
      const interval = setInterval(() => {
        if (q._initialized()) {
          clearInterval(interval)
          try {
            const specRegex = new RegExp('^' + specId + ':0:[a-f0-9\\-]{36}$')
            expect(q._workers[0]._processId).to.match(specRegex)
            done()
          } catch (error) { done(error) }
        }
      }, 100)
    })

    bools.forEach(bool => {
      it('should create a Queue with a ' + bool + ' sanitize option when specified', () => {
        // with an injectable constructor we can be sure this has the desired effect instead of 
        // checking that part of the implementation is correct
        var q = new th.Queue(th.testRef, { sanitize: bool }, _.noop)
        expect(q._sanitize).to.equal(bool)
      })
    })

    bools.forEach(bool => {
      it('should create a Queue with a ' + bool + ' suppressStack option when specified', () => {
        // with an injectable constructor we can be sure this has the desired effect instead of 
        // checking that part of the implementation is correct
        var q = new th.Queue(th.testRef, { suppressStack: bool }, _.noop)
        expect(q._suppressStack).to.equal(bool)
      })
    })

    it('should not create a Queue when initialized with 4 parameters', () => {
      expect(() => { new th.Queue(th.testRef, {}, _.noop, null) })
        .to.throw('Queue can only take at most three arguments - queueRef, options (optional), and processingFunction.')
    })
  })

  describe('#getWorkerCount', () => {
    it('should return worker count with options.numWorkers', () => {
      // is already tested in initialize
      const numWorkers = 10
      const q = new th.Queue(th.testRef, { numWorkers: numWorkers }, _.noop)
      expect(q.getWorkerCount()).to.equal(numWorkers)
    })
  })

  describe('#addWorker', () => {
    // addWorker semantics will probably change a bit, in theory the spec might not be available yet

    it('should add worker', () => {
      // we should check this with injectable constructor  for queue worker
      // this might not have created an actual worker
      const q = new th.Queue(th.testRef, _.noop)
      expect(q.getWorkerCount()).to.equal(1)
      q.addWorker()
      expect(q.getWorkerCount()).to.equal(2)
    })

    it('should add worker with correct process id', () => {
      // constructor
      const specId = 'test_task'
      const q = new th.Queue(th.testRef, { specId: specId }, _.noop)
      const worker = q.addWorker()
      const specRegex = new RegExp('^' + specId + ':1:[a-f0-9\\-]{36}$');
      expect(worker._processId).to.match(specRegex);
    })

    it('should not allow a worker to be added if the queue is shutting down', () => {
      const q = new th.Queue(th.testRef, _.noop)
      expect(q.getWorkerCount()).to.equal(1)
      q.shutdown()
      expect(() => { q.addWorker() })
        .to.throw('Cannot add worker while queue is shutting down')
      // also check the worker has not been added before the error was thrown
    })
  })

  describe('#shutdownWorker', () => {

    it('should remove worker', () => {
      // constructor
      const q = new th.Queue(th.testRef, _.noop)
      expect(q.getWorkerCount()).to.equal(1)
      q.shutdownWorker()
      expect(q.getWorkerCount()).to.equal(0)
    });

    it('should shutdown worker', () => {
      // needs investigation, it seems nothing is tested here apart from 
      // the completion of a promise
      const q = new th.Queue(th.testRef, _.noop)
      expect(q.getWorkerCount()).to.equal(1)
      const workerShutdownPromise = q.shutdownWorker()
      return workerShutdownPromise
    });

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
      // what is tested here?
      const q = new th.Queue(th.testRef, _.noop)
      return q.shutdown().should.eventually.be.fulfilled
    })

    it('should shutdown a queue initialized with a custom spec before the listener callback', () => {
      // what is tested here?
      const q = new th.Queue(th.testRef, { specId: 'test_task' }, _.noop)
      return q.shutdown().should.eventually.be.fulfilled
    })

    it('should shutdown a queue initialized with a custom spec after the listener callback', done => {
      // we need to examine this one
      const q = new th.Queue(th.testRef, { specId: 'test_task' }, _.noop)
      const interval = setInterval(() => {
        if (q._initialized()) {
          clearInterval(interval)
          try {
            const shutdownPromise = q.shutdown()
            expect(q._specChangeListener()).to.be.null
            shutdownPromise.should.eventually.be.fulfilled.notify(done)
          } catch (error) {
            done(error)
          }
        }
      }, 100)
    })
  })
})
