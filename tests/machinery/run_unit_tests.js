const { sequence, wait, TIMEOUT } = require('./promise_utils')

module.exports = runUnitTests

async function runUnitTests({ report, tests, timeout }) {
  const results = await runUnitTests()

  return { success: results.every(x => x.result.success), results }

  async function runUnitTests() {
    return sequence(tests, async ([title, test]) => {
      const runResult = await Promise.race([runUnitTest(title, test), wait(timeout * 2)])
      const result = runResult === TIMEOUT
        ? { success: false, error: `timed out` }
        : runResult
      report({ title, test, result })
      return { title, test, result }
    })
  }
}

async function runUnitTest(title, test) {
  try {
    const error = await test()
    return { success: !error, error }
  } catch (e) {
    if (e === TIMEOUT) return TIMEOUT
    else return { success: false, error: `${e}` }
  }
}