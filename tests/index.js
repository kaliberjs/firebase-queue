const firebase = require(`firebase`)
const selfChecks = require('./machinery/self_checks')
const runUnitTests = require('./machinery/run_unit_tests')
const runSpecs = require('./machinery/run_specs')
const unitTests = require('./unit_tests')
const specs = require('./specs')
const { report, logSuccess, logFailure } = require('./machinery/report_utils')

const app = firebase.initializeApp({
  // apiKey: `api key not needed`,
  databaseURL: `ws://localhost:5000`,
})
const db = app.database()
const rootRef = db.ref()

db.goOnline()
selfChecks({ rootRef })
  .then(success => {
    /* istanbul ignore else */
    if (success) logSuccess(console, 'Self checks')
    else logFailure(console, 'Self checks', 'failed')

    return success && runUnitTests({ report: report(console), tests: unitTests(rootRef) })
  })
  .then(success => success && runSpecs({ rootRef, report: report(console), specs: specs(rootRef) }))
  .then(success => {
    /* istanbul ignore if */
    if (!success) process.exitCode = 1
  })
  .catch(/* istanbul ignore next */ e => {
    console.error(e)
    process.exitCode = 1
  })
  .then(_ => { db.goOffline() })
