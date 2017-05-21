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
const nonPositiveIntigerObjects      = ['', 'foo', NaN, Infinity, true, false, 0,    -1, ['foo', 'bar'], { foo: 'bar' }, null, { foo: { bar: { baz: true } } }, _.noop]
const nonBooleanObjects              = ['', 'foo', NaN, Infinity,              0, 1,     ['foo', 'bar'], { foo: 'bar' }, null, { foo: { bar: { baz: true } } }, _.noop]
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
  return withRef(ref, tasksRef => {
    const q = new th.Queue({ tasksRef, options, processingFunction })
    return allways(() => f(q), q.shutdown)
  })
}

function withRef(ref = th.testRef, f) {
  return allways(() => f(ref), () => ref.off && (ref.off(), ref.set(null)))
}

function wait(time) {
  return new Promise(r => setTimeout(r, time))
}

describe('Queue', () => {

  describe('initialize', () => {

    it('should not create a Queue with only a tasks reference', () => {
      expect(() => { new th.Queue({ tasksRef: th.testRef }) })
        .to.throw('No processing function provided.')
    })

    it('should not create a Queue with only a processingFunction', () => {
      expect(() => { new th.Queue({ processingFunction: _.noop }) })
        .to.throw('No tasks reference provided.')
    })

    it('should create a default Queue with just a Firebase reference and a processing callback', () =>
      new th.Queue({ tasksRef: th.testRef, processingFunction: _.noop }).shutdown()
    )

    it('should create a default Queue with a Firebase reference, an empty options object, and a processing callback', () =>
      new th.Queue({ tasksRef: th.testRef, options: {}, processingFunction: _.noop }).shutdown()
    )

    nonPositiveIntigerObjects.forEach(nonPositiveIntigerObject => {
      it('should not create a Queue with a non-positive integer numWorkers specified ' + JSON.stringify(nonPositiveIntigerObject), () => {
        expect(() => { new th.Queue({ tasksRef: th.testRef, options: { numWorkers: nonPositiveIntigerObject }, processingFunction: _.noop }) })
          .to.throw('options.numWorkers must be a positive integer.')
      })
    })

    _.range(1, 20).forEach(numWorkers => {
      it('should create a Queue with ' + numWorkers + ' workers when specified in options.numWorkers', () => {
        let instanceCount = 0
        const QueueWorker = queueWorkerSpy({ init: _ => instanceCount++ })
        
        return withQueue({ options: { numWorkers: numWorkers, QueueWorker } }, q => {
          expect(instanceCount).to.equal(numWorkers)
        })
      })
    })

    it('should create a single worker if no number of workers is specified', () => {
      let instanceCount = 0
      const QueueWorker = queueWorkerSpy({ init: _ => instanceCount++ })

      return withQueue({ options: { QueueWorker }}, q => {
        expect(instanceCount).to.equal(1)
      })
    })

    it('should set the corerct processIdBase on QueueWorkers', () => {
      let processIdBase = null
      const QueueWorker = queueWorkerSpy({ init: ({ processIdBase: p }) => processIdBase = p })

      return withQueue({ options: { QueueWorker }}, q => 
        th.waitFor(_ => !!processIdBase).then(_ => { expect(processIdBase).to.equal('0') })
      )
    })

    nonBooleanObjects.forEach(nonBooleanObject => {
      it('should not create a Queue with a non-boolean sanitize option specified', () => {
        expect(() => { new th.Queue({ tasksRef: th.testRef, options: { sanitize: nonBooleanObject }, processingFunction: _.noop }) })
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
        expect(() => { new th.Queue({ tasksRef: th.testRef, options: { suppressStack: nonBooleanObject }, processingFunction: _.noop }) })
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

    nonFirebaseObjects.forEach(nonFirebaseObject => {
      it('should not create a Queue with a non-firebase object: ' + JSON.stringify(nonFirebaseObject), () => {
        expect(() => { new th.Queue({ tasksRef: nonFirebaseObject, processingFunction: _.noop }) }).to.throw
      })
    })

    it('should pass a given tasks reference to QueueWorker', () => {
      let tasksRef = null
      const QueueWorker = queueWorkerSpy({ init: ({ tasksRef: t }) => tasksRef = t })
      
      return withRef(th.tasksRef, testRef => 
        testRef.set('value').then(_ => 
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
        expect(() => { new th.Queue({ tasksRef: th.testRef, processingFunction: nonFunctionObject }) })
          .to.throw('No processing function provided.')
      })
    })

    it('should set a default task spec if no `spec` was given', () => {
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

    it('should pass the correct spec to the worker', () => {
      let spec = null
      const QueueWorker = queueWorkerSpy({ init: ({ spec: s }) => spec = s })
      const specId = 'test_spec'

      const originalSpec = {
        startState: 'a',
        inProgressState: 'b',
        finishedState: 'c',
        errorState: 'd',
        retries: 'e',
        timeout: 'f'
      }

      return withQueue({ ref: th.tasksRef, options: { spec: originalSpec, QueueWorker } }, q =>
        th.waitFor(() => !!spec).then(_ => {
          expect(spec).to.deep.equal(originalSpec)
        })
      )
    })
  })

  //   it ('should correctly handle the situation where the spec is invalid', () => {})

  describe('#shutdown', () => {

    it('should shutdown a queue initialized with the default spec', () => {
      let shutdownCalled = false
      const QueueWorker = queueWorkerSpy({ 
        shutdown: () => shutdownCalled = true
      })

      return withQueue({ options: { QueueWorker } }, q => {
        q.shutdown().then(_ => { expect(shutdownCalled).to.be.true })
      })
    })
  })
})
