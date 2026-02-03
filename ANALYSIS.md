# Firebase Queue Usage Analysis

This document analyzes how `@kaliber/firebase-queue` is used across Kaliber projects to inform modernization priorities.

## Summary

| Metric | Value |
|--------|-------|
| Projects using library | 7 |
| Total services | ~18 |
| Manual retry implementations | 1 (should be 0) |
| Services with no error recovery | 17 |

---

## Projects Overview

| Project | Services | Primary Use Case |
|---------|----------|------------------|
| **gemini-queue** | 1 | AI-powered Rollbar error fixing |
| **bol-com-cadeaukaarten** | 5 | Email, quotes, CRM sync, config processing |
| **sanaccent** | 4 | Form handling (jury, reservations, orders) |
| **rabobank-jobs** | 5 | Job applications, alerts, tracking |
| **rabobank-rabo-en-co** | 1 | Publication/article processing |
| **rabobank-trending-topics** | 1 | Elasticsearch indexing |
| **rabobank-kunstcollectie** | 1 | (Publication processing) |

---

## Usage Patterns

### 1. Custom State Transitions

All projects use the `finishedState` option to control task cleanup:

```javascript
// Production: remove completed tasks
const options = { spec: { finishedState: null } }

// Development: keep for debugging
const options = { spec: { finishedState: 'finished' } }

// Common pattern
const options = { 
  spec: { 
    finishedState: process.env.CONFIG_ENV !== 'prd' ? 'finished' : undefined 
  } 
}
```

**Found in:** All projects

---

### 2. Multi-Queue Services

Several services manage multiple queues for different task types:

```javascript
// sanaccent/form-handler-service.js
const queueAanmeldenJury = new Queue({ tasksRef: aanmeldenJuryRef, ... })
const queuePaginasReserveren = new Queue({ tasksRef: paginasReserverenRef, ... })
const queueKaartenReserveren = new Queue({ tasksRef: kaartenReserverenRef, ... })
const queuejaarboekBestellen = new Queue({ tasksRef: jaarboekBestellenRef, ... })

return {
  shutdown: () => Promise.all([
    queueAanmeldenJury.shutdown(),
    queuePaginasReserveren.shutdown(),
    ...
  ])
}
```

**Found in:** sanaccent, bol-com-cadeaukaarten, rabobank-trending-topics

---

### 3. External API Integration

All services make external API calls that can fail transiently:

| Service | External APIs |
|---------|---------------|
| job-application-processing | Workday SOAP API |
| synchronization-service | SuperOffice, Trade API |
| elasticsearch-service | Elasticsearch |
| offerte-accept-service | Trade API, Email |
| gemini-queue | Gemini CLI, GitHub API |

**Problem:** No built-in retry means transient failures become permanent.

---

### 4. Manual Retry Implementation

> [!IMPORTANT]
> `rabobank-jobs/job-application-processing-service.js` implements manual retry with exponential backoff—functionality that should be in the library.

```javascript
const RETRY_INTERVAL = 60 * 5 * 1000 // 5 minutes
const MAX_RETRY_ATTEMPTS = 8
const EXPONENTIAL_BACKOFF_FACTOR = 4

// Periodic polling to find failed tasks
const retryHandling = runPeriodically(retryFailedApplications, { 
  timeBetweenInMillis: RETRY_INTERVAL, 
  reportError 
})

async function retryFailedApplications() {
  // Find all tasks in error state
  const tasksWithErrors = await tasksRef
    .orderByChild('_state')
    .equalTo('error')
    .once('value')
  
  // Filter to retryable tasks
  const retryableTasks = Object.entries(tasksWithErrors.val() || {})
    .filter(([_, x]) => (
      (!x.retryAttemptDate || isBeforeNow(x.retryAttemptDate)) &&
      notAtRetryLimit(x)
    ))
  
  // Calculate exponential backoff and reset state
  const updates = retryableTasks.reduce((result, [key, task]) => {
    const retryAttemptNumber = (task.retryAttempt || 0) + 1
    const nextAttemptInMinutes = retryAttemptNumber ** EXPONENTIAL_BACKOFF_FACTOR
    return {
      ...result,
      [`${key}/retryAttempt`]: retryAttemptNumber,
      [`${key}/retryAttemptDate`]: dayjs.utc().add(nextAttemptInMinutes, 'minutes').valueOf(),
      [`${key}/_state`]: null, // Reset to requeue
    }
  }, {})
  
  await tasksRef.update(updates)
}
```

**Lines of code:** ~50  
**This pattern should be a library feature.**

---

### 5. Dynamic Queue Control

`synchronization-service` implements pause/resume for external service availability:

```javascript
function createChangeHandler({ ... }) {
  let isRunning = false
  let queue = null

  return { start, stop, shutdown }

  function start() {
    if (isRunning) return
    isRunning = true
    queue = new Queue({ ... })
  }

  async function stop() {
    if (!isRunning) return
    await queue.shutdown()
    isRunning = false
    queue = null
  }
}

// Usage: pause when SuperOffice is down
startDetectingSuperOfficeStatus({
  onUp() { changeHandler.start() },
  onDown() { changeHandler.stop() },
})
```

**This pattern suggests a built-in pause/resume feature would be valuable.**

---

### 6. Scheduled/Periodic Tasks

Several services use `node-schedule` or `runPeriodically` alongside the queue:

```javascript
// job-alert-processing-service.js
const scheduledTask = scheduler.scheduleJob('0 10 * * 1', () => sendBulk())

// publication-service.js
const scheduler = runPeriodically(
  () => pollWithdrawSchedule({ ... }),
  { timeBetweenInMillis: 60 * 60 * 1000 }
)
```

**Delayed job support would partially address this.**

---

## Feature Gap Analysis

### Comparison with BullMQ

| Feature | Firebase Queue | BullMQ | Gap |
|---------|---------------|--------|-----|
| Basic task processing | ✅ | ✅ | — |
| Multi-worker | ✅ | ✅ | — |
| Progress tracking | ✅ | ✅ | — |
| Graceful shutdown | ✅ | ✅ | — |
| Heartbeat | ✅ | ✅ | — |
| Lifecycle hooks | ✅ | ✅ | — |
| **Retry with backoff** | ❌ | ✅ | 🔴 Critical |
| **Delayed jobs** | ❌ | ✅ | 🟡 High |
| **Pause/Resume** | ❌ | ✅ | 🟡 High |
| **Concurrency limit** | ❌ | ✅ | 🟡 Medium |
| Rate limiting | ❌ | ✅ | 🟢 Low |
| Job prioritization | ❌ | ✅ | ⚪ Not needed |
| Repeatable/Cron | ❌ | ✅ | ⚪ Not needed |
| Job dependencies | ❌ | ✅ | ⚪ Not needed |

---

## Priority Features

Based on actual usage patterns, these are the recommended additions:

### 🔴 P0: Retry with Exponential Backoff

- Already manually implemented in rabobank-jobs
- Would benefit all 18 services
- Prevents data loss from transient failures

### 🟡 P1: Pause/Resume

- Already manually implemented in synchronization-service
- Useful for external service availability

### 🟡 P2: Delayed Jobs

- Would simplify retry implementation
- Could replace some `runPeriodically` patterns

### 🟡 P3: Concurrency Control

- Useful for rate-limited APIs (Workday, Gemini)
- Simple to implement

---

## Modernization Opportunities

Beyond features, the codebase could be modernized:

| Area | Current | Proposed |
|------|---------|----------|
| Module system | CommonJS | ES Modules |
| Syntax | Constructor functions | Classes |
| Types | None | TypeScript or .d.ts |
| Firebase SDK | v8.1.1 | v9+ modular |
| Testing | firebase-server | Firebase Emulator |
| Deferred pattern | Manual | `Promise.withResolvers()` |

---

## Recommendation

**Phase 1:** Add retry with exponential backoff (highest impact)  
**Phase 2:** Add pause/resume and concurrency control  
**Phase 3:** Modernize to ES Modules + TypeScript  
**Phase 4:** Migrate to Firebase v9+ (breaking change)
