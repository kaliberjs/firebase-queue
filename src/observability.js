
/**
 * Creates an observability instance that wraps logging, metrics, and tracing.
 * All interfaces are optional - if not provided, operations are no-ops.
 * 
 * @param {Object} config
 * @param {Object} config.logger - Logger with debug, info, warn, error methods
 * @param {Object} config.metrics - Metrics with increment, histogram, gauge methods
 * @param {Object} config.tracer - OpenTelemetry Tracer instance
 * @param {string} config.queueId - Queue identifier for labels
 */
function createObservability(config = {}) {
  const logger = config.logger || noopLogger
  const metrics = config.metrics || noopMetrics
  const tracer = config.tracer || null
  const queueId = config.queueId || 'default'

  return {
    /**
     * Log a structured event
     * @param {'debug'|'info'|'warn'|'error'} level
     * @param {string} event - Event name like 'task.claimed'
     * @param {Object} meta - Additional metadata
     */
    log(level, event, meta = {}) {
      const logFn = logger[level]
      if (logFn) {
        try { logFn(event, { event, queue: queueId, ...meta }) } catch (_) {}
      }
    },

    /**
     * Emit a counter metric (increment by 1)
     * @param {string} name - Metric name
     * @param {Object} labels - Additional labels
     */
    increment(name, labels = {}) {
      if (metrics.increment) {
        try { metrics.increment(name, { queue: queueId, ...labels }) } catch (_) {}
      }
    },

    /**
     * Emit a histogram metric (for durations, sizes, etc.)
     * @param {string} name - Metric name
     * @param {number} value - Observed value
     * @param {Object} labels - Additional labels
     */
    histogram(name, value, labels = {}) {
      if (metrics.histogram) {
        try { metrics.histogram(name, value, { queue: queueId, ...labels }) } catch (_) {}
      }
    },

    /**
     * Emit a gauge metric (for current values)
     * @param {string} name - Metric name
     * @param {number} value - Current value
     * @param {Object} labels - Additional labels
     */
    gauge(name, value, labels = {}) {
      if (metrics.gauge) {
        try { metrics.gauge(name, value, { queue: queueId, ...labels }) } catch (_) {}
      }
    },

    /**
     * Start a tracing span (no-op if no tracer configured)
     * @param {string} name - Span name
     * @param {Function} fn - Async function to wrap
     * @returns {Promise} Result of fn
     */
    async startSpan(name, fn) {
      if (!tracer) return fn()
      
      return tracer.startActiveSpan(name, async span => {
        try {
          const result = await fn()
          span.setStatus({ code: 1 }) // SpanStatusCode.OK
          return result
        } catch (e) {
          span.setStatus({ code: 2, message: e.message }) // SpanStatusCode.ERROR
          span.recordException(e)
          throw e
        } finally {
          span.end()
        }
      })
    },

    /**
     * Update worker gauges
     * @param {number} busy - Number of busy workers
     * @param {number} total - Total number of workers
     */
    updateWorkerGauges(busy, total) {
      this.gauge('queue.workers.busy', busy)
      this.gauge('queue.workers.total', total)
    },
  }
}

const noopLogger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
}

const noopMetrics = {
  increment: () => {},
  histogram: () => {},
  gauge: () => {},
}

/**
 * A no-op observability instance for when no observability is configured
 */
const noopObservability = createObservability()

module.exports = { createObservability, noopObservability }
