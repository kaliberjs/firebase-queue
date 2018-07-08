const { sequence, wait, TIMEOUT } = require('./promise_utils')

module.exports = runUnitTests

async function runUnitTests({ report, tests }) {
  const result = await runUnitTests()

  return result.every(x => x.success)

  async function runUnitTests() {
    return sequence(tests, async ([title, test]) => {
      const result = await Promise.race([runUnitTest(title, test), wait(1000)])
      const actualResult = result === TIMEOUT
        ? { title, success: false, error: `timed out` }
        : result
      report(actualResult)
      return actualResult
    })
  }
}

async function runUnitTest(title, test) {
  try {
    const error = await test()
    return { title, success: !error, error }
  } catch (e) {
    if (e === TIMEOUT) return TIMEOUT
    else return { title, success: false, error: `${e}` }
  }
}