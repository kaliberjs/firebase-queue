_This is a heavily modified fork from the original [Firebase Queue library](https://github.com/firebase/firebase-queue). Everything, including the documentation, has been given a complete overhaul._

----
[![npm version](https://img.shields.io/npm/v/@kaliber/firebase-queue.svg)](https://www.npmjs.com/package/@kaliber/firebase-queue)


# Firebase Queue

A fault-tolerant, multi-worker, multi-stage job pipeline built on the [Firebase Realtime
Database](https://firebase.google.com/docs/database/).

```bash
yarn add @kaliber/firebase-queue
```

## Table of Contents

 * [Usage](#usage)
 * [Retry](#retry)
 * [Pause/Resume](#pauseresume)
 * [Observability](#observability)
 * [Documentation](#documentation)
 * [Contributing](#contributing)
 * [Thanks](#thanks)
 * [Differences](#differences)
 * [Motivation](#motivation)

## Usage

Basic usage example

```js
const firebase = require('firebase-admin')
const { createQueue } = require('@kaliber/firebase-queue')

const app = firebase.initializeApp(..., 'my-queue')
const tasksRef = app.database().ref('tasks')

// the queue starts processing as soon as you create an instance
const queue = createQueue({ tasksRef, processTask, reportError })

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

## Retry

Failed tasks can automatically retry with configurable backoff strategies:

```js
const queue = createQueue({
  tasksRef,
  processTask,
  reportError,
  options: {
    retry: {
      maxAttempts: 5,           // Maximum retry attempts (required)
      backoff: 'exponential',   // 'exponential', 'linear', 'fixed', or custom function
      initialDelayMs: 1000,     // Initial delay before first retry (default: 1000)
      maxDelayMs: 3600000,      // Maximum delay cap (default: 1 hour)
      retryableErrors: (error) => {  // Optional: control which errors retry
        return error.code !== 'PERMANENT_FAILURE'
      }
    },
    lifecycle: {
      onTaskRetryScheduled: (taskId, attempt, delayMs, error) => {
        console.log(`Task ${taskId} scheduled for retry #${attempt} in ${delayMs}ms`)
      }
    }
  }
})
```

### Backoff Strategies

| Strategy | Formula | Use Case |
|----------|---------|----------|
| `'exponential'` | `initialDelay * 2^(attempt-1)` | Most failures (network, rate limits) |
| `'linear'` | `initialDelay * attempt` | Predictable delays |
| `'fixed'` | `initialDelay` | Consistent retry timing |
| Custom function | `(attempt, initialDelay) => ms` | Custom logic |

### Retry Metadata

Tasks being retried have additional fields:

| Field | Description |
|-------|-------------|
| `_retry_attempt` | Current retry attempt number |
| `_retry_at` | Scheduled retry timestamp |
| `_last_error` | Error message from last failure |

### RetryScheduler

For handling orphaned retries (from crashed workers), use the RetryScheduler:

```js
const { createRetryScheduler } = require('@kaliber/firebase-queue/retry-scheduler')

const scheduler = createRetryScheduler({
  tasksRef,
  startState: null,           // Match your queue's startState
  pollIntervalMs: 60000,      // How often to check for ready retries
  reportError: console.error  // Optional: error handler for polling failures
})

scheduler.start()

// On shutdown
scheduler.stop()
```

## Pause/Resume

Queues can be paused and resumed dynamically:

```js
const queue = createQueue({
  tasksRef,
  processTask,
  reportError,
  options: {
    lifecycle: {
      onQueuePaused: () => console.log('Queue paused'),
      onQueueResumed: () => console.log('Queue resumed')
    }
  }
})

// Pause processing (current task will complete)
queue.pause()

// Check if paused
queue.isPaused() // true

// Resume processing
queue.resume()
```

> **Note:** Pause takes effect between tasks. A task that's already being processed will complete before the pause takes effect.

## Observability

The queue provides built-in observability features for monitoring and debugging production workloads.

### Heartbeat

Workers automatically update a `_heartbeat` timestamp on tasks while processing. This allows you to detect stuck workers.

```js
const queue = createQueue({
  tasksRef,
  processTask,
  reportError,
  options: {
    heartbeatInterval: 30000, // update every 30s (default)
    // heartbeatInterval: null, // disable heartbeat
  }
})
```

**Detecting stuck tasks:**

```js
const staleThreshold = 2 * 60 * 1000 // 2 minutes
const now = Date.now()

const stuckTasks = await tasksRef
  .orderByChild('_state')
  .equalTo('in_progress')
  .once('value')

stuckTasks.forEach(snap => {
  const task = snap.val()
  if (now - task._heartbeat > staleThreshold) {
    console.log('Stuck task:', snap.key)
  }
})
```

### Lifecycle Hooks

Subscribe to task lifecycle events for logging, metrics, or alerting:

```js
const queue = createQueue({
  tasksRef,
  processTask,
  reportError,
  options: {
    lifecycle: {
      onTaskClaimed: (taskId, workerId) => {
        console.log(`Task ${taskId} claimed by ${workerId}`)
      },
      onTaskCompleted: (taskId, durationMs, result) => {
        metrics.histogram('task_duration_ms', durationMs)
      },
      onTaskFailed: (taskId, durationMs, error) => {
        alerting.notify(`Task ${taskId} failed: ${error.message}`)
      },
      onTransactionRetry: (taskId, attempt, error) => {
        console.warn(`Transaction retry ${attempt} for ${taskId}`)
      }
    }
  }
})
```

### Queue Stats

Get real-time statistics about the queue:

```js
const stats = queue.getStats()
// {
//   numWorkers: 5,
//   busyWorkers: 3,
//   totalProcessed: 142,
//   totalFailed: 3,
//   totalRetried: 12,
//   isPaused: false
// }
```

### Task Metadata

Tasks in progress now include additional timing fields:

| Field | Description |
|-------|-------------|
| `_heartbeat` | Last heartbeat timestamp (updated every `heartbeatInterval`) |
| `_started_at` | Timestamp when processing began |
| `_duration_ms` | Processing duration (set on completion/failure) |

### Custom Observability

For production monitoring, you can provide your own logging, metrics, and tracing implementations:

```js
const queue = createQueue({
  tasksRef,
  processTask,
  reportError,
  options: {
    observability: {
      // Structured logger interface
      logger: {
        debug: (event, meta) => console.log(JSON.stringify({ level: 'debug', event, ...meta })),
        info: (event, meta) => console.log(JSON.stringify({ level: 'info', event, ...meta })),
        warn: (event, meta) => console.warn(JSON.stringify({ level: 'warn', event, ...meta })),
        error: (event, meta) => console.error(JSON.stringify({ level: 'error', event, ...meta })),
      },
      
      // Metrics interface (e.g., Prometheus, Datadog)
      metrics: {
        increment: (name, labels) => { /* count: queue.tasks.completed, etc */ },
        histogram: (name, value, labels) => { /* duration: queue.task.duration_ms */ },
        gauge: (name, value, labels) => { /* current: queue.workers.busy */ },
      },
      
      // OpenTelemetry tracer (optional)
      tracer: trace.getTracer('firebase-queue'),
    }
  }
})
```

#### Metrics Emitted

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `queue.tasks.claimed` | counter | `queue`, `worker` | Tasks claimed |
| `queue.tasks.completed` | counter | `queue`, `worker` | Tasks completed successfully |
| `queue.tasks.failed` | counter | `queue`, `worker` | Tasks failed permanently |
| `queue.tasks.retried` | counter | `queue`, `worker`, `attempt` | Tasks scheduled for retry |
| `queue.task.duration_ms` | histogram | `queue`, `status` | Processing time |
| `queue.workers.busy` | gauge | `queue` | Currently processing |
| `queue.workers.total` | gauge | `queue` | Total workers |
| `queue.paused` | gauge | `queue` | 1 if paused, 0 otherwise |

#### Log Events

| Event | Level | Description |
|-------|-------|-------------|
| `queue.started` | info | Queue initialized |
| `queue.paused` | info | Queue paused |
| `queue.resumed` | info | Queue resumed |
| `queue.shutdown` | info | Queue shut down |
| `task.claimed` | debug | Task claimed by worker |
| `task.completed` | info | Task completed successfully |
| `task.failed` | warn | Task failed permanently |
| `task.retry_scheduled` | info | Task scheduled for retry |

## Documentation

* [Guide](docs/guide.md)


## Contributing

If you'd like to contribute to Firebase Queue, please first read through our [contribution
guidelines](.github/CONTRIBUTING.md).


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

### Retries are back (v1.4.0+)

The original library allowed retries, which were initially removed in this fork. As of v1.4.0, 
retries are back with a more flexible implementation:

- **Opt-in**: Retries are disabled by default, preserving backward compatibility
- **Configurable backoff**: Exponential, linear, fixed, or custom strategies
- **Error filtering**: Use `retryableErrors` to control which errors should retry
- **Lifecycle hooks**: Get notified when retries are scheduled

See the [Retry](#retry) section for usage details.

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

### No more dynamic worker count

The original library had functions that allowed users of the queue to change the amount of workers
at runtime. In practice we never needed to do that. Providing a similar behavior is quite easy to
achieve by either creating extra queue instances or recreating the queue with `numWorkers` set to
another value.

It really depends on your use case which is most appropriate in your situation.

In the situation that you need to continue processing at all costs, just add / remove queues to
change the amount of active workers. Note that a queue is a very lightweight object, so creating
more than one is no problem.

```js
const queues = [makeQueue()]

function makeQueue() {
  return createQueue({ ... })
}

function scaleUp() {
  queues.push(makeQueue())
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
let queue = makeQueue(numWorkers)

function makeQueue(numWorkers) {
  return createQueue({ ..., options: { numWorkers } })
}

async function scaleUp() {
  await queue.shutdown()
  numWorkers += 1
  queue = makeQueue(numWorkers)
}
async function scaleDown() {
  await queue.shutdown()
  numWorkers = Math.max(1, numWorkers - 1)
  queue = makeQueue(numWorkers)
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
that I should start from scratch with testing. Before I could do that however I needed to reduce the
amount of moving parts and only keep the essentials. I have now added (I think) 100% coverage of all
functionality in files which are now a lot smaller than the original 2000+ lines.

The production code itself should now be very readable and more easily understood. The line count
has dropped dramatically and this is not only caused by removing functionality, but also by slowly
carving out a more ideal version. This is in no way critique to the original authors, it is not
something that can be done in a time-constraint environment (work).

### Fun

I really like the concept and have used the original library a lot. On top of that, I like
untangling code and carving it into something (I think is) more beautiful.

I also enjoy the idea that this might be useful for other people.
