import type { database } from 'firebase-admin'

export type Task = ReservedFields & Record<string, any>

export type Meta = {
  snapshot: database.DataSnapshot,
  setProgress(progress: number): Promise<void>,
}

export type ReservedFields = {
  _state?: string,
  _state_changed?: number,
  _owner?: string,
  _progress?: number,
  _heartbeat?: number,
  _started_at?: number,
  _duration_ms?: number,
  _retry_at?: number,
  _attempt?: number,
  _error_details?: {
    error?: string,
    error_stack?: string,
  } & Record<string, any>,
}

export type Falsy = null | undefined | 0 | false | '' | void

export type Config = {
  tasksRef: database.Reference,
  processTask: ProcessTask,
  reportError: ReportError,
  options?: Options
}

export type Options = {
  spec?: Spec,
  errorToErrorDetails?: ErrorToErrorDetails | null,
  numWorkers?: number,
  heartbeatInterval?: number | null,
  lifecycle?: Lifecycle | null,
  retry?: RetryConfig | null,
  maxConcurrent?: number | null,
  observability?: ObservabilityConfig | null,
}

export type Spec = {
  startState?: null | string,
  inProgressState?: string,
  finishedState?: null | string,
  errorState?: string,
}

export type SpecWithDefaults = Required<Spec>

export type ErrorToErrorDetails = (e: any) => Record<string, any>

export type ProcessTask = (task: Task, meta: Meta) => Falsy | Task | Promise<Falsy | Task>

export type ReportError = (e: Error) => void

// Lifecycle hooks
export type Lifecycle = {
  onTaskClaimed?: (taskId: string, workerId: string) => void,
  onTaskCompleted?: (taskId: string, durationMs: number, result: any) => void,
  onTaskFailed?: (taskId: string, durationMs: number, error: Error) => void,
  onTaskRetryScheduled?: (taskId: string, attempt: number, delayMs: number, error: Error) => void,
  onTransactionRetry?: (taskId: string, attempt: number, error: Error) => void,
  onQueuePaused?: () => void,
  onQueueResumed?: () => void,
}

// Retry configuration
export type RetryConfig = {
  maxAttempts?: number,
  backoff?: 'exponential' | 'linear' | 'fixed' | BackoffFunction,
  initialDelayMs?: number,
  maxDelayMs?: number,
  retryableErrors?: (error: Error) => boolean,
}

export type BackoffFunction = (attempt: number, initialDelayMs: number) => number

// Observability
export type ObservabilityConfig = {
  logger?: Logger,
  metrics?: Metrics,
  tracer?: any,
}

export type Logger = {
  debug?: (event: string, meta: Record<string, any>) => void,
  info?: (event: string, meta: Record<string, any>) => void,
  warn?: (event: string, meta: Record<string, any>) => void,
  error?: (event: string, meta: Record<string, any>) => void,
}

export type Metrics = {
  increment?: (name: string, labels?: Record<string, any>) => void,
  histogram?: (name: string, value: number, labels?: Record<string, any>) => void,
  gauge?: (name: string, value: number, labels?: Record<string, any>) => void,
}

// Queue instance
export type Queue = {
  shutdown(): Promise<void>,
  getStats(): QueueStats,
  pause(): void,
  resume(): void,
  isPaused(): boolean,
}

export type QueueStats = {
  numWorkers: number,
  busyWorkers: number,
  totalProcessed: number,
  totalFailed: number,
  totalRetried: number,
  isPaused: boolean,
}

// RetryScheduler
export type RetryScheduler = {
  start(): void,
  stop(): Promise<void>,
  isRunning(): boolean,
}
