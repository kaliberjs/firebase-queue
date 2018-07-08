const { expectError } = require('./test_utils')
const { wait, waitFor } = require('./promise_utils')
const { report: defaultReport, logSuccess, logFailure } = require('./report_utils')
const runSpecs = require('./run_specs')
const runUnitTests = require('./run_unit_tests')

module.exports = async function performSelfCheck({ rootRef }) {
  const selfCheckSpecs = [
    [`specs ops 'equal' - report failure when not equal`, {
      test: _ => [0, `equal`, 1]
    }],
    [`specs ops 'equal' - report failure when not equal`, {
      test: _ => [{ index: 1 }, `equal`, { index: 2 }]
    }],
    [`specs ops 'and' - report failure when first fails`, {
      test: _ => [[0, `equal`, 1], `and`, []]
    }],
    [`specs ops 'and' - report failure when second fails`, {
      test: _ => [[0, `equal`, 0], `and`, [0, `equal`, 1]]
    }],
    [`specs ops 'non existing op' - report failure when an op does not exist`, {
      test: _ => [0, `non existing op`]
    }],
    [`specs ops 'noDuplicates' - report failure when there are duplicates`, {
      test: _ => [[0, 0], `noDuplicates`]
    }],
    [`specs ops 'sameValue' - report failure when not all given values are the same`, {
      test: _ => [[0, 1], `sameValues`]
    }],
    [`specs ops 'haveFields' - report failure when not all fields are present`, {
      test: _ => [[{ a: 0 }, { b: 0 }], `haveFields`, [`a`, `b`]]
    }],
    [`specs - report timeout for long processes`, {
      process: async _ => { await wait(550) },
    }],
    [`specs - report errors if they occur in test`, {
      test: _ => { throw new Error(`custom error`) }
    }],
    [`specs - report errors if they are reported`, {
      process: async (_, { snapshot }) => {
        await snapshot.ref.child(`_state`).set('the state got changed')
      },
      test: () => [0, `equal`, 0],
    }],
    [`specs - report errors if they are reported and caught`, {
      process: async (_, { snapshot }) => {
        await snapshot.ref.child(`_state`).set('the state got changed')
      },
      test: () => [0, `equal`, 0],
      expectReportedErrors: x => x.length === 1,
    }],
    [`specs - report errors if they were expected`, {
      test: () => [0, `equal`, 0],
      expectReportedErrors: true,
    }],
    [`specs - report errors if timed out`, {
      test: async () => { await wait(1050) }
    }],
    [`specs - report processed if errors in sync process method`, {
      process: _ => { throw new Error(`custom error`) },
      test: async ({ processed }) => processed.length === 1
    }],
    [`specs - report processed if errors in async process method`, {
      process: async _ => { throw new Error(`custom error`) },
      test: async ({ processed }) => processed.length === 1
    }],
    [`specs - report errors if spec is defined as function`, () => ({
      test: () => true
    })],
  ]

  const selfCheckUnitTests = [
    ['expect error - fail when no error is thrown', () => expectError({
      code: () => {},
      test: [undefined, undefined]
    })],
    ['expect error - fail the incorrect error is thrown', () => expectError({
      code: [() => { throw null }],
      test: [e => e !== null, `incorrect error`]
    })],
    ['reports - there is a difference between success and failure', () => {
      let log = null
      let error = null

      const c = { log: x => { log = x }, error: x => { error = x } }

      logSuccess(c, ``)
      logFailure(c, ``, ``)

      return log && error && log !== error
    }],
    ['reports - success and failure are reported correctly', () => {
      let log = null
      let error = null

      const c = { log: x => { log = x }, error: x => { error = x } }

      defaultReport(c)({ success: true, title: 'success', error: 'none' })
      defaultReport(c)({ success: false, title: 'failure', error: 'failed' })

      return (
        log && log.includes('success') && !log.includes('none') &&
        error && error.includes('failure') && error.includes('failed')
      )
    }],
    ['unit tests - fail on timeout', async () => { await wait(1050) }],
    ['unit tests - fail if a timeout occurs', async () => { await waitFor(() => false, { timeout: 10 }) }],
    ['unit tests - fail on error', () => { throw new Error('custom error') }],
  ]

  const specResults = await runSpecs({ rootRef, report, specs: selfCheckSpecs })
  const unitTestResults = await runUnitTests({ report, tests: selfCheckUnitTests })

  const success = await runSpecs({ rootRef, report, specs: [] })
  report({ success, title: `specs - report if there are no specs that execute synchronously or asynchronously` })

  return !success && !specResults && !unitTestResults

  function report({ title, success }) {
    /* istanbul ignore if */
    if (success) logFailure(console, title, `Expected failure, but got success`)
  }
}