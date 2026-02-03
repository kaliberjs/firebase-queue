module.exports = { expectError }

/** @arg {{ code: (() => void) | (() => void)[], test: [test: (e: Error) => boolean, error: string] }} props */
function expectError({ code, test: [test, error] }) {
  if (Array.isArray(code)) {
    return code.map(code => run(code, test, error))
      .map((result, i) => result && `[${i}] - ${result}`)
      .filter(Boolean)
      .join(`\n`)
  } else return run(code, test, error)

  /**
   * @arg {() => void} code
   * @arg {(e: Error) => boolean} test
   * @arg {string} error
   */
  function run(code, test, error) {
    try {
      code(); return `No error thrown`
    } catch (e) {
      const error = e instanceof Error ? e : new Error(`Unknown error type (${typeof e}): ${e}`)
      return !test(error) && `${error}\n${e}`
    }
  }
}
