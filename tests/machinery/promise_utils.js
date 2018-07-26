const TIMEOUT = Symbol(`timeout`)

module.exports = {
  sequence,
  wait,
  waitFor,
  TIMEOUT,
}

async function sequence(a, f) {
  return a.reduce(async (result, x) => [...await result, await f(x)], [])
}

function wait(x) {
  return new Promise(resolve => { setTimeout(() => resolve(TIMEOUT), x) })
}

function waitFor(f, { timeout }) {
  return new Promise((resolve, reject) => {
    const start = Date.now()
    check()

    function check() {
      setTimeout(
        async () => {
          const result = await Promise.race([f(), wait(timeout)])
          if (result && result !== TIMEOUT) resolve()
          else if (Date.now() - start > timeout) reject(TIMEOUT)
          else check()
        },
        10
      )
    }

  })
}
