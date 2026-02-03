const firebase = require(`firebase-admin`)
const selfChecks = require('./machinery/self_checks')
const runUnitTests = require('./machinery/run_unit_tests')
const { runSpecs, checkExecutionResults } = require('./machinery/run_specs')
const createUnitTests = require('./unit_tests')
const createSpecs = require('./specs')
const { report } = require('./machinery/report_utils')
/** @import { Spec } from './machinery/run_specs' */

const timeout = 4000

const app = firebase.initializeApp({
  projectId: 'demo-kaliber-firebase-queue-tests',
  databaseURL: `http://localhost:5000?ns=demo-kaliber-firebase-queue-tests-default-rtdb`,
})
const db = app.database()
const rootRef = db.ref()

db.goOnline()
console.log('Running self checks...')
Promise.resolve(true)
selfChecks({ rootRef })
  .then(async previousSuccess => {
    console.log('Running unit tests...')
    const tests = createUnitTests({ rootRef, timeout })
    const { success } = await runUnitTests({ report: report(console), tests, timeout })

    return previousSuccess && success
  })
  .then(async previousSuccess => {
    console.log('Running specs...')
    const specs = createSpecs({ rootRef, timeout })
    const { success: specSuccess, results } = await runSpecs({ rootRef, report: report(console), specs, timeout })
    const { success: executionSuccess } = checkExecutionResults({ results, report: report(console) })

    return previousSuccess && specSuccess && executionSuccess
  })
  .then(success => {
    if (!success) process.exitCode = 1
  })
  .catch(e => {
    console.error(e)
    process.exitCode = 1
  })
  .then(_ => { db.goOffline() })
