/** @import { Result } from './run_specs' */

module.exports = {
  report,
  logSuccess,
  logFailure,
}

/**
 * @typedef {{
 *   log(message: string): void
 *   error(message: string): void
 * }} Console
 */

/** @arg {Console} console */
function report(console){
  /** @arg {{ title: string, result: Result }} props */
  return ({ title, result: { success, error } }) => {
    if (success) logSuccess(console, title)
    else logFailure(console, title, error)
  }
}

/**
 * @arg {Console} console
 * @arg {string} title
 */
function logSuccess(console, title) { console.log(`${successColor(`✓`)} ${title}`) }
/**
 * @arg {Console} console
 * @arg {string} title
 * @arg {string} error
 */
function logFailure(console, title, error) {
  console.error(`${failureColor(`x`)} ${title}\n\n    ${error.replace(/\n/g, `\n    `)}\n`)
}
/** @arg {string} s */
function successColor(s) { return color(s, 10) }
/** @arg {string} s */
function failureColor(s) { return color(s, 9) }
/**
 * @arg {string} s
 * @arg {number} color
 */
function color(s, color) { return `\x1B[38;5;${color}m${s}\x1B[0m` }
