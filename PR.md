> [!NOTE]
> This is an experiment/brain-child exploring v2.0 improvements.
> Not in a finished state. Welcome to discuss.

## Overview

This PR introduces **v2.0** of `@kaliber/firebase-queue` with breaking changes and significant improvements driven by real-world usage analysis.

---

## Why These Changes?

### Usage Analysis ([ANALYSIS.md](./ANALYSIS.md))

Analyzed **7 projects** and **18 services**:

| Finding | Impact |
|---------|--------|
| **17/18 services** have no error recovery | Transient failures become permanent |
| **1 service** manually implements retry (~50 LOC) | This should be a library feature |
| **1 service** implements dynamic pause/resume | Useful for external service availability |

### Code Quality Issues

The original codebase had several code smells:

- **Inconsistent patterns**: Mix of pseudo-classes, factory functions, and module patterns
- **Pseudo-class oddity**: `Queue` used `new` keyword but was not a real constructor (no `this`, weird `instanceof` check)
- **No types**: Zero TypeScript or JSDoc type annotations
- **Large functions**: `process()` was 155 lines with 6 nested functions

### Feature Gap vs BullMQ

| Feature | Before | After |
|---------|--------|-------|
| Retry with backoff | ❌ Manual (~50 LOC) | ✅ Built-in |
| Pause/Resume | ❌ Manual | ✅ Built-in |
| Concurrency limit | ❌ | ✅ Built-in |
| Observability | ❌ | ✅ Logging + Metrics |
| Types | ❌ | ✅ JSDoc + TypeScript |

---

## Breaking Changes ⚠️

Factory functions instead of constructors:

```diff
-const Queue = require('@kaliber/firebase-queue')
-const queue = new Queue({ ... })

+const { createQueue } = require('@kaliber/firebase-queue')
+const queue = createQueue({ ... })
```

**Rationale**: The old `new Queue()` pattern was a pseudo-class that did not actually use `this` - it was a factory function pretending to be a constructor. Converting to explicit factory functions makes the code honest and consistent.

---

## New Features ✨

### 🔴 P0: Retry with Backoff
*Previously manually implemented in rabobank-jobs (~50 lines)*

```javascript
options: {
  retry: {
    maxAttempts: 5,
    backoff: 'exponential', // or 'linear', 'fixed', custom fn
    initialDelayMs: 1000,
    maxDelayMs: 3600000,
    retryableErrors: (error) => error.code !== 'PERMANENT'
  }
}
```

### 🟡 P1: Pause/Resume
*Previously manually implemented in synchronization-service*

```javascript
queue.pause()    // Stop processing new tasks
queue.resume()   // Continue processing
queue.isPaused() // Check state
```

### Observability

```javascript
options: {
  observability: {
    logger: winston,      // Structured logging
    metrics: prometheus,  // Counters, histograms, gauges
    tracer: opentelemetry // Distributed tracing
  }
}
```

### Lifecycle Hooks

```javascript
options: {
  lifecycle: {
    onTaskClaimed: (taskId, workerId) => {},
    onTaskCompleted: (taskId, durationMs, result) => {},
    onTaskFailed: (taskId, durationMs, error) => {},
    onTaskRetryScheduled: (taskId, attempt, delayMs, error) => {},
    onQueuePaused: () => {},
    onQueueResumed: () => {}
  }
}
```

### Queue Stats

```javascript
queue.getStats()
// { numWorkers, busyWorkers, totalProcessed, totalFailed, totalRetried, isPaused }
```

---

## Code Quality Improvements

- ✅ **Consistent factory pattern** throughout (no more pseudo-classes)
- ✅ **Comprehensive JSDoc** type annotations on all functions
- ✅ **Extended types.ts** with 50+ type definitions
- ✅ **Extracted helpers** to `task_helpers.js` (smaller, focused functions)
- ✅ **Merged pnpm branch** (Firebase emulator tests, pnpm, eslint)

---

## Testing

```bash
pnpm test  # Requires Java for Firebase emulator
```

---

## Related

- [ANALYSIS.md](./ANALYSIS.md) - Full usage analysis across projects
