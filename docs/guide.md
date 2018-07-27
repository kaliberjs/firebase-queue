# Guide | Firebase Queue


## Table of Contents

 * [Purpose of a Queue](#purpose-of-a-queue)
 * [The Queue in Your Firebase Database](#the-queue-in-your-firebase-database)
 * [Queue Workers](#queue-workers)
 * [Pushing Tasks Onto the Queue](#pushing-tasks-onto-the-queue)
 * [The `processTask` function](#the-processTask-function)
 * [Queue Security](#queue-security)
 * [Defining Specs (Optional)](#defining-specs-optional)
 * [Graceful Shutdown](#graceful-shutdown)
 * [Message Sanitization, Revisited](#message-sanitization-revisited)
 * [Wrap Up](#wrap-up)


## Purpose of a Queue

Queues can be used in your Firebase app to organize workers or perform background work like
generating thumbnails of images, filtering message contents and censoring data, or fanning
data out to multiple locations in your Firebase Database. First, let's define a few terms we'll
use when talking about a queue:
  - `task` - a unit of work that a queue worker can process
  - `spec` - a definition of an operation that the queue will perform on matching tasks
  - `job` - one or more `spec`'s that specify a series of ordered operations to be performed
  - `worker` - an individual process that picks up tasks with a certain spec and processes them

Let's take a look at a simple example to see how this works. Imagine you wanted to build a chat
application that does two things:
  1. Sanitize chat message input
  2. Fan data out to multiple rooms and users

Since chat message sanitization can't happen purely on the client side, as that would allow a
malicious client to circumvent client side restrictions, you'll have to run this process on a
trusted server process.

Using Firebase Queue, you can create specs for each of these tasks, and then use workers to process
the individual tasks to complete the job. We'll explore the queue, adding tasks, assigning workers,
and creating custom specs to create full jobs, then [revisit the example](#message-sanitization-revisited) above.

## The Queue in Your Firebase Database

The queue relies on having a Firebase Database reference to coordinate workers e.g.
`https://databaseName.firebaseio.com/tasks`. This queue can be stored at any path in your Firebase
Database, and you can have multiple queues as well. The queue will respond to tasks pushed onto it.

Firebase Queue works with a Firebase Database reference from either the [`firebase-admin`](https://www.npmjs.com/package/firebase-admin) (for admin access) or [`firebase`](https://www.npmjs.com/package/firebase) (for end-user access) npm package, though it is mainly intended to perform administrative actions.
Check out [this blog post](https://firebase.googleblog.com/2016/11/bringing-firebase-to-your-server.html) for an introduction to `firebase-admin`.


## Queue Workers

The basic unit of the queue is the queue worker: the function that claims a task, performs the
appropriate processing on the data, and either returns the transformed data, or an appropriate error.

You can start a worker process by passing in a Firebase Database  [`ref`](https://firebase.google.com/docs/server/setup#initialize_the_sdk) along with a processing
function ([described below](#the-processing-function)), as follows:

`my_queue_worker.js`
```js
const Queue = require('@kaliber/firebase-queue')
const firebase = require('firebase-admin')

const serviceAccount = require('path/to/serviceAccountCredentials.json')
firebase.initializeApp({
  credential: firebase.credential.cert(serviceAccount),
  databaseURL: '<your-database-url>'
})

const tasksRef = firebase.database().ref('tasks')
const queue = new Queue({ tasksRef, reportError, processTask })

async function processTask(data, { setProgress }) {
  // Read and process task data
  console.log(data)

  // Do some work
  await setProgress(50)

  // Finish the task asynchronously
  await new Promise(resolve => { setTimeout(resolve, 1000) })
}

function reportError(e) { console.error(e) }
```

```shell
node my_queue_worker.js
```

Multiple queue workers can be initialized on multiple machines and Firebase-Queue will ensure that only one worker is processing a single queue task at a time.


#### Queue Worker Options (Optional)

Queue workers can take an optional options object to specify:
  - `spec` - specifies the spec for this worker. This is important when creating multiple specs.
    Defaults to
    ```
      {
        startState = null,
        inProgressState = 'in_progress',
        finishedState = null,
        errorState = 'error'
      }
    ```
  - `numWorkers` - specifies the number of workers to run simultaneously on a single node.js thread.
    Defaults to 1 worker.

Example:

```js
var options = {
  spec: {
    startState = 'run',
    inProgressState = 'processing',
    finishedState = 'done',
    errorState = 'failed'
  },
  numWorkers: 5
}
var queue = new Queue({ tasksRef, processTask, reportError, options })
```


## Pushing Tasks Onto the Queue

Using any Firebase client or the REST API, push an object with some data to the queue. Queue workers
listening on that queue will automatically pick up and process the new task.

```shell
# Using curl in shell
curl -X POST -d '{"foo": "bar"}' https://databaseName.firebaseio.com/tasks.json
```
or
```js
// Using the web JavaScript client
var ref = firebase.database().ref('tasks');
ref.push({ foo: 'bar' });
```

### Starting Tasks in Specific States (Optional)

When using a custom spec, you can pass a `_state` key in with your object, which will allow a custom
spec's worker(s) to pick up your task at a specific state, rather than starting with the default
start state (`null`).

```js
{
  foo: 'bar',
  boo: 'baz',
  _state: 'spec_n_start'
}
```


## The `processTask` function

The processing function provides the body of the data transformation, and allows for completing
tasks successfully or with error conditions, as well as reporting the progress of a task. Because
this function is an essential part of the queue (it defines the work that the worker must perform)
it is required. It can take two parameters (`data` and `meta`) and can be `async`.

```js
async function processTask(data, meta) {
  ...
}
```


#### `data`

A JavaScript object containing the claimed task's data, and can contain any keys and values with the
exception of several reserved keys, which are used for tracking worker progress.

The reserved keys are:
 - `_state` - The current state of the task. Will always be the tasks `inProgressState` when passed
   to the processing function.
 - `_state_changed` - The timestamp that the task changed into its current state. This will always
   be the server time when the processing function was called.
 - `_owner` - A unique ID for the worker and task number combination to ensure only one worker is
   responsible for the task at any time.
 - `_progress` - A number between 0 and 100, reset at the start of each task to 0 and set to 100 at
   completion if the task is not removed (has a finished state).
 - `_error_details` - An object containing the error details from a previous task execution. If
   present, it may contain an `error` string from the failed promise of the `processTask` function.
   There may also be a `error_stack` field containing a stack dump of if the error from `processTask`
   contained a `stack` field.

 By default the data is sanitized of these keys, but you can still access these keys through the
 snapshot supplied with the second argument (`meta`).

#### `meta`

Meta contains two keys: `{ snapshot, setProgress }`

`setProgress` is a callback function for reporting the progress of the task. `setProgress` takes a
single parameter that must be a number between 0 and 100, and returns a `Promise` that's fulfilled
when successfully updated. If this promise is rejected, it's likely that the task is no longer owned
by this process or the worker has lost its connection to Firebase.

By catching when this call fails and canceling the current task early, the worker can minimize the
extra work it does and return to processing new queue tasks sooner:

```js
async function processTask(data, { setProgress }) {
  ...
  await setProgress(currentProgress)
  ...
}
```

#### Return value

Returning a result or 'falsy' value resolves the task; reporting that the current task has been
completed and the worker is ready to process another task. Any plain JavaScript object returned from
the `processTasks` function will be written to the `tasks` location and will be available to the
next worker if the tasks are chained (using custom specs). When a task is resolved, the `_progress`
field is updated to 100 and the `_state` is replaced with either the `_state` key of the object
returned, or the `finishedState` of the task spec. If the task does not have a `finishedState` or
a 'falsy' value is returned, the task will be removed from the queue.

Throwing an error or returning a 'failed' promise rejects the task; reporting that the current task
failed and the worker is ready to process another task. When this happens, the task will go into the
`errorState` for the job with an additional `_error_details` object. If a string is thrown, the
`_error_details` will also contain an `error` key containing that string. If an Error is thrown, the
`error` key will contain the `error.message`, and the `error_stack` key will contain the
`error.stack`.


## Queue Security

Securing your queue is an important step in securely processing events that come in. Below is a
sample set of security rules that can be tailored to your particular use case.

In this example, there are three categories of users, represented using fields of a
[Database Auth Variable Override](https://firebase.google.com/docs/database/server/start#authenticate-with-limited-privileges):
- `auth.canAddTasks`: Users who can add tasks to the queue (could be an authenticated client or a
                      secure server)
- `auth.canProcessTasks`: Users who can process tasks (usually on a secure server)

These don't have to use a custom token, for instance you could use `auth != null` in place of
`auth.canAddTasks` if application's users can write directly to the queue. Similarly,
`auth.canProcessTasks` could be `auth.admin === true` if a single trusted server process was used to
perform all queue functions.

Please note that this is an elaborate set of rules. You can simplify it to your needs, the one thing
that is very important is the `".indexOn": "_state"` part. But even if you forget that one, Firebase
will likely warn you about it.

```json
{
  "rules": {
    "tasks": {
      ".read": "auth.canProcessTasks",
      ".write": "auth.canAddTasks || auth.canProcessTasks",
      ".indexOn": "_state",
      "$taskId": {
        ".validate": "newData.hasChildren(['property_1', ..., 'property_n'])
                      || (auth.canProcessTasks
                      && newData.hasChildren(['_state', '_state_changed']))",
        "_state": {
          ".validate": "newData.isString()"
        },
        "_state_changed": {
          ".validate": "newData.isNumber() && (newData.val() === now
                        || data.val() === newData.val())"
        },
        "_owner": {
          ".validate": "newData.isString()"
        },
        "_progress": {
          ".validate": "newData.isNumber()
                        && newData.val() >= 0
                        && newData.val() <= 100"
        },
        "_error_details": {
            "error": {
              ".validate": "newData.isString()"
            },
            "error_stack": {
              ".validate": "newData.isString()"
            },
            "$other": {
              ".validate": false
            }
        },
        "property_1": {
          ".validate": "/* Insert custom data validation code here */"
        },
        ...
        "property_n": {
          ".validate": "/* Insert custom data validation code here */"
        }
      }
    }
  }
}
```


## Defining Specs (Optional)

#### Default Spec

A default spec configuration is assumed if no `spec` is passed in to the `options` of the queue. The
default spec has the following characteristics:

```js
{
  startState: null,
  inProgressState: 'in_progress',
  finishedState: null,
  errorState: 'error'
}
```

This essentially states that any object will be processed and when processing is complete the task
will be removed.

- `startState` - The default spec has no `startState`, which means any task pushed into the queue
  without a `_state` key will be picked up by default spec workers. If `startState` is specified,
  only tasks with that `_state` may be claimed by the worker. This is most often used for multi spec
  jobs.
- `inProgressState` - When a worker picks up a task and begins processing it, it will change the
  tasks `_state` to the value of `inProgressState`. This is a required spec property, and it cannot
  equal any other state.
- `finishedState` - The default spec has no `finishedState` so the worker will remove tasks from the
  queue upon successful completion. If `finishedState` is specified, then the tasks `_state` value
  will be updated to the `finishedState` upon task completion. Setting this value to another specs
  `startState` is useful for chaining tasks together to create a job. It's possible to override the
  `finishedState` on a per-task basis by returning an object from the `processTask` function. The
  returned object replaces the task on the queue if no `finishedState` was set, so adding a `_state`
  key to it allows you to set a different `finishedState`. If a `finishedState` was set the it will
  override any `_state` in the object.
- `errorState` - If the task gets rejected the `_state` will be updated to this value and an
  additional key `_error_details` will be populated with an optional error message from the thrown
  value. If this isn't specified, it defaults to 'error'. This can be useful for specifying
  different error states for different tasks, or chaining errors so that they can be logged.

#### Creating Jobs using Custom Specs and Task Chaining

In order to use a job specification other than the default, the specification must be passed into
the queue options `spec` value.

In this example, we're chaining three specs to make a job. New tasks pushed onto the queue without a
`_state` key will be picked up by `spec_1` and go into the `spec_1_in_progress` state. Once `spec_1`
completes and the task goes into the `spec_1_finished` state, `spec_2` takes over and puts it into
the `spec_2_in_progress` state. Again, once `spec_2` completes and the task goes into the
`spec_2_finished` state, `spec_3` takes over and puts it into the `spec_3_in_progress` state.
Finally, `spec_3` removes it once complete. If, during any stage in the process there's an error,
the task will end up in an `error` state.

```js
const spec_1 = {
  inProgressState: 'spec_1_in_progress',
  finishedState: 'spec_1_finished'
}
const spec_2 = {
  startState: 'spec_1_finished',
  inProgressState: 'spec_2_in_progress',
  finishedState: 'spec_2_finished'
}
const spec_3 = {
  startState: 'spec_2_finished',
  inProgressState: 'spec_3_in_progress'
}
```


## Graceful Shutdown

Once initialized, a queue can be gracefully shutdown by calling its `shutdown()` function. This
prevents workers from claiming new tasks, removes all Firebase listeners, and waits until all the
current tasks have been completed before resolving the `Promise` returned by the function.

By intercepting for the `SIGINT` termination signal like this, you can ensure the queue shuts down
gracefully:

```js
...
var queue = new Queue(...)

process.on('SIGINT', async () => {
  console.log('Starting queue shutdown')
  await queue.shutdown()
  console.log('Finished queue shutdown')
  process.exit(0)
})
```


## Message Sanitization, Revisited

In our example at the beginning, you wanted to perform several actions on your chat system:
  1. Sanitize chat message input
  2. Fan data out to multiple rooms and users

Together, these two actions form a job, and you can use custom specs, as shown above, to define the
flow of tasks in this job. When you start, your Firebase should look like this:

```
root
  - tasks /* null, no data */
```

And we will have specs defined like this:

```js
const sanitizeMessageSpec = {
  inProgressState: 'sanitize_message_in_progress'
  finishedState: 'sanitize_message_finished'
}

const fanoutMessageSpec = {
  startState: 'sanitize_message_finished'
  inProgressState: 'fanout_message_in_progress'
  errorState: 'fanout_message_failed'
}
```

Let's imagine that you have some front end that allows your users to write their name and a message,
and send that to your queue as its `data`. Let's assume your user writes something like the
following:

```js
// Using the web JavaScript client
var tasksRef = firebase.database().ref('tasks');
tasksRef.push({
  'message': 'Hello Firebase Queue Users!',
  'name': 'Chris'
});
```

Your Firebase Database should now look like this:

```
root
  - tasks
    - $taskId
      - message: "Hello Firebase Queue Users!"
      - name: "Chris"
```

When your users push `data` like the above into the `tasks` subtree, tasks will initially start in
the `sanitizeMessageSpec` because the task has no `startState`. The associated queue can be
specified using the following processing function:

```js
// chat_message_sanitization.js

const Queue = require('@kaliber/firebase-queue')
const firebase = require('firebase-admin')

const serviceAccount = require('path/to/serviceAccountCredentials.json')
firebase.initializeApp({
  credential: firebase.credential.cert(serviceAccount),
  databaseURL: '<your-database-url>'
})

const db = firebase.database()
const tasksRef = db.ref('tasks')

function reportError(e) { console.error(e) }

const options = { spec: sanitizeMessageSpec }
const sanitizeQueue = new Queue({ tasksRef, options, reportError, processTask: sanitizeTask })

async function sanitizeTask(data) {
  // sanitize input message
  data.message = await sanitize(data.message)

  // pass sanitized message and username along to be fanned out
  return data
}

...
```

The queue worker will take this task, begin to process it, and update the reserved keys of the task:

```
root
  - tasks
    - $taskId
      - _owner: $workerUid
      - _progress: 0
      - _state: "sanitize_message_in_progress"
      - _state_changed: $serverTimestamp
      - message: "Hello Firebase Queue Users!"
      - name: "Chris"
```

Once the message is sanitized, it will be resolved and both the reserved keys and the data will be
updated in the task (imagine for a minute that queue is a blacklisted word):

```
root
  - tasks
    - $taskId
      - _owner: null
      - _progress: 100
      - _state: "sanitize_message_finished"
      - _state_changed: $serverTimestamp
      - message: "Hello Firebase ***** Users!"
      - name: "Chris"
```

Now, you want to fan the data out to the `messages` subtree of your Firebase Database, using the spec, `fanout_message`, so you can set up a second processing function to find tasks whose `_state` is `sanitize_message_finished`:

```js
...

const messagesRef = db.ref('messages')

var options = {
  spec: fanoutMessageSpec,
  numWorkers: 5
};
var fanoutQueue = new Queue({ tasksRef, options, reportError, processTask: fanoutTask })
async function fanoutTask(data) {
  // fan data out to /messages; ensure that errors are caught and cause the task to fail
  await messagesRef.push(data)
}
```

Since there is no `finishedState` in the `fanoutMessageSpec` spec, the task will be purged from the
queue after the data is fanned out to the messages node.

While this example is a little contrived since you could perform the sanitization and fanout in a
single task, creating multiple specs for our tasks allows us to do things like putting additional
workers on more expensive tasks, or add expressive error states.


## Wrap Up

As you can see, Firebase Queue is a powerful tool that allows you to securely and robustly perform
background work on your Firebase data, from sanitization to data fanout and more. We'd love to hear
about how you're using Firebase-Queue in your project!

Let us know on [Twitter](https://twitter.com/kaliberinteract).
