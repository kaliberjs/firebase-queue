[![Coverage Status](https://coveralls.io/repos/github/kaliberjs/firebase-queue/badge.svg)](https://coveralls.io/github/kaliberjs/firebase-queue) [![Build Status](https://travis-ci.org/kaliberjs/firebase-queue.svg?branch=master)](https://travis-ci.org/kaliberjs/firebase-queue) [![npm version](https://badge.fury.io/js/%40kaliber%2Ffirebase-queue.svg)](https://badge.fury.io/js/%40kaliber%2Ffirebase-queue)

# Firebase Queue

A fault-tolerant, multi-worker, multi-stage job pipeline built on the [Firebase Realtime
Database](https://firebase.google.com/docs/database/).

This is a heavily modified fork from the original [Firebase Queue library](https://github.com/firebase/firebase-queue).

```bash
yarn add @kaliber/firebase-queue
```

## Table of Contents

 * [Usage](#usage)
 * [Documentation](#documentation)
 * [Contributing](#contributing)
 * [Thanks](#thanks)
 * [Differences](#differences)
 * [Motivation](#motivation)

## Usage

Basic usage example

```js
const firebase = require('firebase-admin')
const Queue = require('@kaliber/firebase-queue')

const app = firebase.initializeApp(..., 'my-queue')
const tasksRef = app.database().ref('tasks')

// the queue starts processing as soon as you create an instance
const queue = new Queue({ tasksRef, processTask, reportError })

// capture shutdown signal to perform a gracefull shutdown
process.on('SIGINT', async () => {
  await queue.shutdown()
  process.exit(0)
})

async function processTask(task) {
  try {
    // do the work and optionally return a new task
  } catch (e) {
    reportError(e)
    throw e // this marks the task as failed
  }
}

function reportError(e) {
  console.error(e)
  // also report the error to your error tracker (Rollbar, Sentry, RayGun, ...)
}
```

## Documentation

* [Guide](docs/guide.md)


## Contributing

If you'd like to contribute to Firebase Queue, please first read through our [contribution
guidelines](.github/CONTRIBUTING.md). Local setup instructions are available [here](.github/CONTRIBUTING.md#local-setup).


## Thanks

I want to thank the original authors of Firebase Queue. Their work has helped me tremendously in
creating the version that is today. While I do not agree with some of the design decisions, I
understand how the code evolved. Any negative comment from me about their code is not aimed at the
original developers themselves, I am well aware how different priorities can lead to other choices.


## Differences

This library is a stripped down version of the original one. Here I will try to motivate why I made
certain choices.

### No more timeout

The original library had a way to specify a timeout. I eventually figured out this was a completely
separate system and moved it out of the worker. When I was done moving it I realized that providing
a mechanism for timeouts should not be in this library. The biggest issue with timeouts is that you
somehow have to cancel the process or worker that is handling the task and how this works is very
dependent on the specific processing.

There are very valid reasons for having a concept of a time, most notably the case where the node
process gets killed before the task is resolved. It however is very application specific how you
want to deal with these kinds of situations.

I can imagine creating a separate library (or maybe just another component) for these types of
situations. We would need some real use cases to figure out how to implement this.

Finally, a last reason to remove timeouts is because that one setting accounted for quite a large
part of the code and introduced numerous tricky scenario's.

An easy workaround for timeouts is the following:

```js
function wait(x) { return new Promise(resolve, setTimeout(_ => { resolve('timeout') }, x)) }

async function processTask(task) {
  const result = await Promise.race([doTheWork(task), wait(3000)])
  if (result === 'timeout') ...
  else ...
}
```

### No more retries

The original library allowed you to retry tasks when they failed. Retrying is very much dependent on
the type of error. If a code / syntax error is happening there is no point in retrying. If it's a
flaky internet connection there is.

Adding retries to the process function itself is trivial, so I moved the burden to the users of the
library. To achieve similar behavior you can catch any error in the process function and return a
new task with an incremented `_numRetries` prop if the existing `_numRetries` prop is still below
the threshold.

```js
async function processTask({ _numRetries = 0, ...task }) {
  try {
    await doTheWork(task)
  } catch (e) {
    if (_numRetries > 2) throw e
    else return { ...task, _numRetries: _numRetries + 1 }
  }
}
```

### No more specs from Firebase

The original library retrieved it's specs from Firebase. In the last few years that I used the
library I never found the need to change the spec at runtime. The mechanism made the library harder
to use with custom specs and required addition security rules. On top of that it introduced more
complexity in the library itself.

Similar behavior can be achieved outside of the library. Simply listen for spec changes in Firebase
and shutdown / recreate previously created queues.

```js
let queue = null
specsRef.on('value', async snapshot => {
  if (queue) {
    const q = queue
    queue = null
    await q.shutdown()
  }
  if (!queue) {
    queue = new Queue({ spec: snapshot.val() || undefined })
  }
})
```

### No more sanitize

The process function now by default receives the sanitized tasks. It however now also receives the
snapshot that contained that data, allowing the user access to all queue specific properties.

In practice we never needed sanitize, but we often required access to the key of the snapshot. This
key was only available when sanitize was set to `false`.

The new design has a cleaner interface with a few advantages:

- It allows access to the `key`
- It allows access to the `ref`
- It allows access to all queue specific properties

```js
function processTask(sanitizedTask, { snapshot }) {
  ...
}
```

### No more suppress stack

We never used the option to suppress the stack trace because in our minds it is valuable
information. I can see why you would however want to do it. A simple workaround would be to catch
the error and throw only it's message while logging the full error to another location.

```js
async function processTask(task) {
  try {
    await doTheWork(task)
  } catch (e) {
    logError(e)
    throw e.message
  }
}
```

### No more callbacks

Promises won and especially with the async/await syntax it is a lot more useful. The `processTask`
function can now just return a promise and based on it's result the task will be resolved or
rejected. Updating the progress of a task now also returns a promise.

```js
async function processTask(task, { setProgress }) {
  const result = await doTheWorkPart1(task)
  await setProgress(50)
  await doTheWorkPart2(result)
}
```

### No more dependencies

The original library had some dependencies on other libraries. I totally understand some of the
choices for these libraries. I however tend to want to keep my dependencies to a total minimum.

- `rsvp`    - We do not need this anymore in the current landscape.
- `winston` - This might be a bad choice, but the library now requires a `reportError` function when
              the communication with firebase fails. This allows us to report errors to services
              like Rollbar and Raygun. I personally never liked logging for development purposes and
              prefer step debuggers or simple `console.log` statements. On top of that, the library
              is very much simplified and a lot easier to grasp.
- `uuid`    - Firebase has a perfect utility to generate unique enough id's. We'll just use that.
- `lodash`  - While the lodash library is a great one, I found that we do not need it's power
              anymore.

As for testing, I replaced `istanbul` with `nyc` because it is so much easier to use. I removed the
testing frameworks because they only added unneeded complexity. I also threw out Gulp, I don't see
any reason to use it.

## No more dynamic worker count

The original library had functions that allowed users of the queue to change the amount of workers
at runtime. In practice we never needed to do that. Providing a similar behavior is quite easy to
achieve by either creating extra queue instances or recreating the queue with `numWorkers` set to
another value.

It really depends on your use case which is most appropriate in your situation.

In the situation that you need to continue processing at all costs, just add / remove queues to
change the amount of active workers. Note that a queue is a very lightweight object, so creating
more than one is no problem.

```js
const queues = [createQueue()]

function createQueue() {
  return new Queue({ ... })
}

function scaleUp() {
  queues.push(createQueue())
}
async function scaleDown() {
  if (queues.length > 1) {
    const queue = queues.pop()
    await queue.shutdown()
  }
}
```

Another solution is to just shutdown the current queue and restart it with a different amount of
workers.

```js
let numWorkers = 1
let queue = createQueue(numWorkers)

function createQueue(numWorkers) {
  return new Queue({ ..., options: { numWorkers } })
}

async function scaleUp() {
  await queue.shutdown()
  numWorkers += 1
  queue = createQueue(numWorkers)
}
async function scaleDown() {
  await queue.shutdown()
  numWorkers = Math.max(1, numWorkers - 1)
  queue = createQueue(numWorkers)
}
```

## Motivation

### Future

The original Firebase Queue is no longer actively being maintained. The original repository points
to Firebase functions as a way to achieve the same thing. The problem with functions is that they do
not have the desired execution guarantees. This requires you to combine them with a library like
Firebase Queue if you want specific guarantees.

We use Firebase Queue extensively for request/reponse systems and in a lot of cases the Firebase
REST API is the API of our application. Firebase Queue neatly helps us with handling requests. This
design helps us greatly:

- No more writing REST API's
- Better security - the business logic is no longer accessible from the internet
- Separation of concerns, we can simply add another listener to the same location in Firebase

### Tests, design and bugs

When I worked on Firebase Queue I noticed that the tests were not ideal and very heavily tied to the
implementation. While trying to untangle that I encountered non ideal design in the code and noticed
that some important features were not actually being tested.

During this extensive period of refactoring I encountered several bugs for specific edge cases and
also a few places where errors would vanish without a trace.

While slowly moving towards a better design and better and more complete set of tests I realized
that I should start from scratch with testing. Before I could do that however I needed to remove the
amount of moving parts and only keep the essentials. I have now added (I think) 100% coverage of all
functionality in files that are a lot smaller than the original 2000+ lines.

The production code itself should now be very readable and more easily understood. The line count
has dropped dramatically and this is not only caused by removing functionality, but also by slowly
carving out a more ideal version. This is in no way critique to the original authors, it is not
something that can be done in a time-constraint environment (work).

### Fun

I really like the concept and have used the original library a lot. On top of that, I like
untangling code and carving it into something (I think is) more beautiful.

I also enjoy the idea that this might be useful for other people.
