module.exports = { expectError }

function expectError({ code, test: [test, error] }) {
  if (Array.isArray(code)) {
    return code.map(code => run(code, test, error))
      .map((result, i) => result && `[${i}] - ${result}`)
      .filter(Boolean)
      .join(`\n`)
  } else return run(code, test, error)

  function run(code, test, error) {
    try { code(); return `No error thrown` }
    catch (e) { return !test(e) && `${error}\n${e}` }
  }
}
