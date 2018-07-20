const { expectError } = require(`./test_utils`)
const { wait, waitFor } = require(`./promise_utils`)
const { report, logSuccess, logFailure } = require(`./report_utils`)
const { runSpecs, checkExecutionResults } = require(`./run_specs`)
const runUnitTests = require(`./run_unit_tests`)

module.exports = async function performSelfCheck({ rootRef, timeout }) {
  const specsCheck = await specsSelfCheck({ rootRef, timeout })
  const unitTestCheck = await unitTestSelfCheck({ timeout })
  const success = specsCheck && unitTestCheck

  /* istanbul ignore else */
  if (success) logSuccess(console, 'Self checks')
  else logFailure(console, 'Self checks', 'failed')

  return success
}

async function specsSelfCheck({ timeout, rootRef }) {
  const selfCheckSpecs = [
    [`specs ops 'equal' - report failure when not equal`, {
      test: _ => [0, `equal`, 1],
      check: ({ error }) => error.includes(`equal`),
    }],
    [`specs ops 'equal' - report failure when not equal`, {
      test: _ => [{ index: 1 }, `equal`, { index: 2 }],
      check: ({ error }) => error.includes(`equal`),
    }],
    [`specs ops 'and' - report failure when first fails`, {
      test: _ => [[0, `equal`, 1], `and`, []],
      check: ({ error }) => error.includes(`equal`),
    }],
    [`specs ops 'and' - report failure when second fails`, {
      test: _ => [[0, `equal`, 0], `and`, [0, `equal`, 1]],
      check: ({ error }) => error.includes(`equal`),
    }],
    [`specs ops 'non existing op' - report failure when an op does not exist`, {
      test: _ => [0, `non existing op`],
      check: ({ error }) => error.includes(`operation`),
    }],
    [`specs ops 'noDuplicates' - report failure when there are duplicates`, {
      test: _ => [[0, 0], `noDuplicates`],
      check: ({ error }) => error.includes(`duplicates`),
    }],
    [`specs ops 'sameValue' - report failure when not all given values are the same`, {
      test: _ => [[0, 1], `sameValues`],
      check: ({ error }) => error.includes(`same`),
    }],
    [`specs ops 'haveFields' - report failure when not all fields are present`, {
      test: _ => [[{ a: 0 }, { b: 0 }], `haveFields`, [`a`, `b`]],
      check: ({ error }) => error.includes(`fields`),
    }],
    [`specs - report timeout for long processes`, {
      process: async _ => { await wait(timeout * 1.1) },
      check: ({ error }) => error.includes(`timed out`),
    }],
    [`specs - report errors if they occur in test`, {
      test: _ => { throw new Error(`custom error`) },
      check: ({ error }) => error.includes(`custom error`),
    }],
    [`specs - report errors if they are reported`, {
      process: async (_, { snapshot }) => {
        await snapshot.ref.child(`_state`).set(`the state got changed`)
      },
      test: () => [0, `equal`, 0],
      check: ({ error }) => error.includes(`resolve`),
    }],
    [`specs - report errors if they are reported and caught`, {
      process: async (_, { snapshot }) => {
        await snapshot.ref.child(`_state`).set(`the state got changed`)
      },
      test: () => [0, `equal`, 0],
      expectReportedErrors: x => x.length === 1,
      check: ({ error }) => error === `true`,
    }],
    [`specs - report errors if they were expected`, {
      test: () => [0, `equal`, 0],
      expectReportedErrors: true,
      check: ({ error }) => error.includes(`Expected`),
    }],
    [`specs - report errors if timed out`, {
      test: async () => { await wait(timeout * 2.1) },
      check: ({ error }) => error.includes(`timed out`),
    }],
    [`specs - report errors if timed out`, {
      process: async () => { await wait(timeout * 1.1) },
      check: ({ error }) => error.includes(`timed out`),
    }],
    [`specs - report processed if errors in sync process method`, {
      process: _ => { throw new Error(`custom error`) },
      test: async ({ processed }) => [processed.length, `equal`, 1],
      check: ({ success }) => success,
    }],
    [`specs - report processed if errors in async process method`, {
      process: async _ => { throw new Error(`custom error`) },
      test: async ({ processed }) => [processed.length, `equal`, 1],
      check: ({ success }) => success,
    }],
    [`specs - report errors if spec is defined as function`, () => ({
      test: () => [0, `equal`, 1],
      check: ({ error }) => error.includes(`equal`),
    })],
    [`specs - report errors if they occur in an unexpected place`, {
      process: async (_, { snapshot }) => {
        await snapshot.ref.child(`_state`).set(`the state got changed`)
      },
      expectReportedErrors: () => { throw new Error(`custom error`) },
      check: ({ error }) => error.includes(`custom error`),
    }],
  ]

  const { results: specResults } = await runSpecs({ rootRef, report: () => {}, specs: selfCheckSpecs, timeout })
  const specSuccess = specResults.every(
    ({ title, spec, result }) => {
      const success = spec.check(result)
      /* istanbul ignore if */
      if (!success) logFailure(console, title, `Self check failed`)
      return success
    }
  )

  const { success: s1 } = checkExecutionResults({ results: [{ result: { info: { async: false, sync: true }}}], report: () => {} })
  if (s1) logFailure(console, `specs - report if there are no specs that execute asynchronously`, `failed`)
  const { success: s2 } = checkExecutionResults({ results: [{ result: { info: { async: true, sync: false }}}], report: () => {} })
  if (s2) logFailure(console, `specs - report if there are no specs that execute synchronously`, `failed`)

  return !s1 && !s2 && specSuccess
}

async function unitTestSelfCheck({ timeout }) {

  const selfCheckUnitTests = [
    [`expect error - fail when no error is thrown`, withCheck(
      () => expectError({
        code: () => {},
        test: [undefined, undefined]
      }),
      ({ error }) => error.includes('thrown'),
    )],
    [`expect error - fail the incorrect error is thrown`, withCheck(
      () => expectError({
        code: [() => { throw null }],
        test: [e => e !== null, `incorrect error`]
      }),
      ({ error }) => error.includes('incorrect error'),
    )],
    [`reports - there is a difference between success and failure`, withCheck(
      () => {
        let log = null
        let error = null

        const c = { log: x => { log = x }, error: x => { error = x } }

        logSuccess(c, ``)
        logFailure(c, ``, ``)

        return log && error && log !== error
      },
      ({ error }) => error === true,
    )],
    [`reports - success and failure are reported correctly`, withCheck(
      () => {
        let log = null
        let error = null

        const c = { log: x => { log = x }, error: x => { error = x } }

        report(c)({ title: `success`, result: { success: true, error: `none` } })
        report(c)({ title: `failure`, result: { success: false, error: `failed` } })

        return (
          log && log.includes(`success`) && !log.includes(`none`) &&
          error && error.includes(`failure`) && error.includes(`failed`)
        )
      },
      ({ error }) => error === true,
    )],
    [`unit tests - fail on timeout`, withCheck(
      async () => { await wait(1050) },
      ({ error }) => error === `timed out`,
    )],
    [`unit tests - fail if a timeout occurs`, withCheck(
      async () => { await waitFor(() => false, { timeout: 10 }) },
      ({ error }) => error === `timed out`,
    )],
    [`unit tests - fail on error`, withCheck(
      () => { throw new Error(`custom error`) },
      ({ error }) => error.includes(`custom error`)
    )],
  ]

  const { results: unitTestResults } = await runUnitTests({ report: () => {}, tests: selfCheckUnitTests, timeout })

  const unitTestSuccess = unitTestResults.every(
    ({ title, test, result }) => {
      const success = test.check(result)
      /* istanbul ignore if */
      if (!success) logFailure(console, title, `Self check failed`)
      return success
    }
  )

  return unitTestSuccess

  function withCheck(test, check) {
    test.check = check
    return test
  }
}