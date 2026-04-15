const { sequence, wait, TIMEOUT } = require('./promise_utils')
/** @import { Falsy } from '../../src/types' */
/** @import { Result } from './run_specs' */

module.exports = runUnitTests

/**
 * @typedef {() => string | Falsy | Promise<string | Falsy>} Test
 */

/**
 * @template {Record<string, any>} [T={}]
 * @arg {{
 *   report(result: { title: string, test: any, result: Result }): void,
 *   tests: [string, Test & T][],
 *   timeout: number,
 * }} props
 */
async function runUnitTests({ report, tests, timeout }) {
  const results = await runUnitTests()

  return { success: results.every(x => x.result.success), results }

  async function runUnitTests() {
    return sequence(tests, async ([title, test]) => {
      const runResult = await Promise.race([runUnitTest(title, test), wait(timeout * 2)])
      /** @type {Result} */
      const result = runResult === TIMEOUT
        ? { success: false, error: `timed out` }
        : runResult

      report({ title, test, result })
      return { title, test, result }
    })
  }
}

/**
 * @arg {string} title
 * @arg {Test} test
 * @returns {Promise<Result | TIMEOUT>}
 */
async function runUnitTest(title, test) {
  try {
    const error = await test()
    return error ? { success: false, error } : { success: true }
  } catch (e) {
    if (e === TIMEOUT) return TIMEOUT
    else return { success: false, error: `${e}` }
  }
}
