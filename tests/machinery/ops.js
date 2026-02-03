
/**
 * @typedef {{
 *   equal(a: any, b: any): false | string,
 *   and(a: Operation<any>, b: Operation<any>): false | string,
 *   noDuplicates(a: Array<any>): false | string,
 *   sameValues(a: Array<any>): false | string,
 *   haveFields(a: Array<Record<string, any>>, fields: Array<string>): false | string,
 *   gte(a: number, b: number): false | string,
 * }} Ops
 */

/**
 * @template {keyof Ops} [T=keyof Ops]
 * @typedef {readonly [
 *   Parameters<Ops[T]>[0],
 *   T,
 *   ...NotFirst<Parameters<Ops[T]>>
 * ]} Operation
 */

/**
 * @template {any[]} T
 * @typedef {(
 *   T extends [any, ...infer Rest] ? Rest : []
 * )} NotFirst
 */

module.exports = { execute }

const ops = /** @satisfies {Ops} */ ({
  /** @arg {any} a @arg {any} b */
  equal: (a, b) => {
    const preparedA = JSON.stringify(prepare(a), null, 2)
    const preparedB = JSON.stringify(prepare(b), null, 2)
    return preparedA !== preparedB &&
      `Expected 'a' to equal 'b'\n'a': ${preparedA}\n'b': ${preparedB}`

    /** @arg {any} x @returns {any} */
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
  /**
   * @arg {Operation<any>} aa
   * @arg  {Array<Operation<any> | 'and'>} bb
   */
  and: (aa, ...bb) => bb.filter(x => x !== `and`).reduce(
    (error, bb) => error ? error : execute(bb),
    execute(aa)
  ),
  /** @arg {Array<any>} a */
  noDuplicates: a => new Set(a).size !== a.length &&
    `Expected no duplicates in ${JSON.stringify(a, null, 2)}`,
  /** @arg {Array<any>} a */
  sameValues: a => new Set(a).size !== 1 &&
    `Expected all values to be the same ${JSON.stringify(a, null, 2)}`,
  /** @arg {number} a @arg {number} b */
  gte: (a, b) => a < b && `Expected ${a} to be greater than or equal to ${b}`,
  /** @arg {Array<Record<string, any>>} a @arg {Array<string>} fields */
  haveFields: (a, fields) =>
    a.reduce(
      /** @arg {Array<string | false>} result @arg {Record<string, any>} x */
      (result, x) => {
        const error = execute(/** @type const */ ([Object.keys(x).sort(), `equal`, fields.sort()])) &&
          `Expected (only) the fields ${fields.join(`, `)} to be present in\n${JSON.stringify(x, null, 2)}`
        return [...result, error]
      },
      /** @type {Array<string | false>} */ ([])
    ).filter(Boolean).join(`\n\n`)
})

/**
 * @template {keyof Ops} T
 * @arg {Operation<T>} operation
 * @returns {string | false}
 */
function execute([a, op, ...b]) {
  const f = ops[op]
  // @ts-ignore
  return f ? f(a, ...b) : `Could not find operation with name '${op}'`
}
