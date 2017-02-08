'use strict';

const _ = require('lodash')
const path = require('path')
const util = require('util')
const admin = require('firebase-admin')

admin.initializeApp({
  credential: admin.credential.cert(require('./key.json')),
  databaseURL: require('./url.js')
})

module.exports = function() {
  var self = this;

  const testRef = admin.database().ref(_.random(1, 2 << 29))
  const tasksRef = testRef.child('tasks')
  
  this.testRef = testRef
  this.tasksRef = tasksRef
  this.offset = 0;
  self.testRef.root.child('.info/serverTimeOffset').on('value', function(snapshot) {
    self.offset = snapshot.val();
  });

  const Queue = require('../src/queue.js')
  const QueueWorker = require('../src/lib/queue_worker.js')

  this.Queue = Queue
  this.QueueWorker = QueueWorker

  this.QueueWorkerWithoutProcessingOrTimeouts = function() {
    self.QueueWorker.apply(this, arguments);

    this._tryToProcess = _.noop
    this._setUpTimeouts = _.noop
  };

  this.QueueWorkerWithoutProcessing = function() {
    self.QueueWorker.apply(this, arguments);

    this._tryToProcess = _.noop
  };

  this.validBasicTaskSpec = {
    inProgressState: 'in_progress'
  };
  this.validTaskSpecWithStartState = {
    inProgressState: 'in_progress',
    startState: 'start_state'
  };
  this.validTaskSpecWithFinishedState = {
    inProgressState: 'in_progress',
    finishedState: 'finished_state'
  };
  this.validTaskSpecWithErrorState = {
    inProgressState: 'in_progress',
    errorState: 'error_state'
  };
  this.validTaskSpecWithTimeout = {
    inProgressState: 'in_progress',
    timeout: 10
  };
  this.validTaskSpecWithRetries = {
    inProgressState: 'in_progress',
    retries: 4
  };
  this.validTaskSpecWithEverything = {
    inProgressState: 'in_progress',
    startState: 'start_state',
    finishedState: 'finished_state',
    errorState: 'error_state',
    timeout: 10,
    retries: 4
  };

  this.waitForState = waitForState
  this.waitForStates = waitForStates
  this.pushTasks = pushTasks
  this.chain = chain
  this.withTasksRef = withTasksRef
  this.withEchoQueueWorkerFor = withEchoQueueWorkerFor
  this.sideEffect = sideEffect
  this.waitFor = waitFor
  this.echo = echo
  this.withData = withData
  this.timeout = timeout

  function waitForState(valOrVals, state, time = 500) {
    if (Array.isArray(valOrVals)) return waitForStateMultiple(valOrVals, state, time) 
    else return waitForStateSingle(valOrVals, state, time)
  }

  function waitForStateMultiple(vals, state, time = 500) {
    return Promise.all(vals.map(val => waitForState(val, state, time)))
  }

  function waitForStates(...vals) {
    return Promise.all(vals.map(([val, state, time]) => waitForStateSingle(val, state, time)))
  }

  function waitForStateSingle(val, state, time = 500) {
    return Promise.race([
      timeout(time, `while waiting for ${toString(val)} to reach the ${state} state`),
      new Promise(resolve => {
        const handler = val.ref.on('value', snapshot => {
          if (snapshot.exists() && snapshot.val()._state === state) {
            val.ref.off('value', handler)
            resolve(snapshot)
          }
        })
      })
    ])
  }

  function waitFor(check, time) {
    return Promise.race([
      timeout(time),
      new Promise(resolve => {
        performCheck()
        function performCheck() {
          if (check()) resolve()
          else setTimeout(performCheck, 50)
        }
      })
    ])
  }

  function sideEffect(execute) {
    return val => execute().then(_ => val)
  }

  function echo(data, _, resolve) { resolve(data) }

  function withData(callback) {
    return (data, _, resolve) => { callback(data); resolve() }
  }

  function timeout(time, message = '') { 
    return new Promise((_, reject) => setTimeout(reject, time, new Error(`Timeout of ${time} milliseconds reached ${message}`)))
  }

  function chain(first, ...rest) {
    return rest.reduce((result, next) => result.then(next), first)
  }

  function pushTasks(ref, ...tasks) {
    return Promise.all(tasks.map(task => ref.push(task))) // if libraries stopped using `this` internally we could have used `.map(ref.push)`
  }

  function withEchoQueueWorkerFor(tasksRef, f) {
    const qw = new QueueWorker(tasksRef, '0', true, false, echo)
    return allways(f(qw), () => qw.shutdown()) /* as soon as we removed all `this` references in QueueWorker we can simplify to `qw.shutdown` */
  }

  function withTasksRef(f) {
    const clear = () => tasksRef.set(null)
    return allways(f(tasksRef), clear)
  }

  function allways(promise, f) {
    return promise
      .then(val => f().then(_ => val))
      .catch(e => f().then(_ => Promise.reject(e)))
  }

  function toString(val) {
    return val.val
      ? val.val()
      : val.key
      ? val.key
      : val.toString()
  }

  return this
}
