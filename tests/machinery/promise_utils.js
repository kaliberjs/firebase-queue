const TIMEOUT = Symbol(`timeout`)

module.exports = {
  sequence,
  wait,
  waitFor,
  TIMEOUT,
}

/**
 * @template T
 * @template R
 * @arg {Array<T>} a
 * @arg {(value: T) => Promise<R>} f
 */
async function sequence(a, f) {
  return a.reduce(async (result, x) => [...await result, await f(x)], /** @type {Promise<Array<R>>} */ (Promise.resolve([])))
}

/** @arg {number} milliseconds @returns {Promise<TIMEOUT>} */
function wait(milliseconds) {
  return new Promise(resolve => { setTimeout(() => resolve(TIMEOUT), milliseconds) })
}

/**
 * @arg {() => any | Promise<any>} f
 * @arg {{ timeout: number }} config
 */
function waitFor(f, { timeout }) {
  return new Promise((resolve, reject) => {
    const start = Date.now()
    check()

    function check() {
      setTimeout(
        async () => {
          const result = await Promise.race([f(), wait(timeout)])
          if (result && result !== TIMEOUT) resolve(undefined)
          else if (Date.now() - start > timeout) reject(TIMEOUT)
          else check()
        },
        10
      )
    }

  })
}
