import type { database } from 'firebase-admin'

export type Task = ReservedFields & Record<string, any>
export type Meta = {
  snapshot: database.DataSnapshot,
  setProgress(progress: number): void,
}

export type ReservedFields = {
  _state?: string,
  _state_changed?: number,
  _owner?: string,
  _progress?: number,
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
  numWorkers?: number
}

export type Spec = {
  startState?: null | string,
  inProgressState?: string,
  finishedState?: null | string,
  errorState?: string,
}

export type SpecWithDefaults = Required<Spec>

export type ErrorToErrorDetails = ((e: any) => Record<string, any>)

export type ProcessTask = (task: Task, meta: Meta) => Falsy | Task | Promise<Falsy | Task>

export type ReportError = (e: Error) => void
