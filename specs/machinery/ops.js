module.exports = { execute }

const ops = {
  equal: (a, b) => {
    const preparedA = JSON.stringify(prepare(a), null, 2)
    const preparedB = JSON.stringify(prepare(b), null, 2)
    return preparedA !== preparedB &&
      `Expected 'a' to equal 'b'\n'a': ${preparedA}\n'b': ${preparedB}`

    function prepare(x) {
      if (!x) return x
      if (Array.isArray(x)) return x.map(prepare)
      if (typeof x === 'object') return Object.entries(x)
        .map(([k, v]) => [k, prepare(v)])
        .sort()
        .reduce((o, [k, v]) => ({ ...o, [k]: v }), {})

      return x
    }
  },
  and: (aa, ...bb) => bb.filter(x => x !== `and`).reduce(
    (error, bb) => error ? error : execute(bb),
    execute(aa)
  ),
  noDuplicates: a => new Set(a).size !== a.length &&
    `Expected no duplicates in ${JSON.stringify(a, null, 2)}`,
}

function execute([a, op, ...b]) {
  const f = ops[op]
  return f ? f(a, ...b) : `Could not find operation with name '${op}'`
}