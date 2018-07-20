module.exports = {
  report,
  logSuccess,
  logFailure,
}

function report(console){
  return ({ title, result: { success, error } }) => {
    if (success) logSuccess(console, title)
    else logFailure(console, title, error)
  }
}

function logSuccess(console, title) { console.log(`${successColor(`✓`)} ${title}`) }
function logFailure(console, title, error) {
  console.error(`${failureColor(`x`)} ${title}\n\n    ${error.replace(/\n/g, `\n    `)}\n`)
}
function successColor(s) { return color(s, 10) }
function failureColor(s) { return color(s, 9) }
function color(s, color) { return `\x1B[38;5;${color}m${s}\x1B[0m` }
