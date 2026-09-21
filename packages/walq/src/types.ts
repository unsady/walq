import type { RetentionPolicy, Storage } from '@walq/core/storage'

/** Queue retention overrides; omitted statuses use the queue defaults. */
export type RetentionOptions = Partial<RetentionPolicy>

export interface QueueOptions {
  storage: Storage
  /** Total allowed executions for every job in this queue. */
  attempts?: number
  /**
   * Called when a claim, lease mutation, cleanup, or handler fails. Absent
   * means errors are written to `console.error`. The callback may be async; its
   * own errors are reported without affecting queue execution.
   */
  onError?: ProcessErrorHandler
  /**
   * Terminal-job retention for this queue. Defaults to
   * `{ completed: 0, failed: 100 }`: completed jobs are removed as soon as a
   * cleanup pass runs, while the newest 100 failures are kept. Null keeps every
   * job of that status.
   */
  retention?: RetentionOptions
}

export interface ProcessOptions {
  /** Maximum number of handlers running at once. */
  concurrency?: number
}

export interface AddedJob {
  id: string
}

export type ProcessContext = {
  readonly signal: AbortSignal
  readonly jobId: string
  /** Current execution number, starting at 1. */
  readonly attempt: number
}

/** Queue operation that produced an error. */
export type ProcessErrorOperation =
  | 'claim'
  | 'cleanup'
  | 'heartbeat'
  | 'complete'
  | 'fail'
  | 'handler'

/** A claim failed before any job was acquired, so no job context exists. */
export interface ClaimErrorContext {
  readonly queue: string
  readonly operation: 'claim'
}

/** A bounded cleanup pass failed for a queue, so no job context exists. */
export interface CleanupErrorContext {
  readonly queue: string
  readonly operation: 'cleanup'
}

/** A lease mutation failed for a claimed job. */
export interface JobErrorContext {
  readonly queue: string
  readonly operation: 'heartbeat' | 'complete' | 'fail'
  readonly jobId: string
  /** Execution number of the claimed job, starting at 1. */
  readonly attempt: number
}

/** A handler threw or rejected for a claimed job. */
export interface HandlerErrorContext {
  readonly queue: string
  readonly operation: 'handler'
  readonly jobId: string
  /** Execution number of the claimed job, starting at 1. */
  readonly attempt: number
  /**
   * The claim's attempt budget is spent, so no retry can be issued. Derived
   * from the claimed job's attempt limits; it does not confirm that a terminal
   * failure was committed to storage.
   */
  readonly attemptsExhausted: boolean
}

/** Typed description of the operation that failed, discriminated by `operation`. */
export type ProcessErrorContext =
  | ClaimErrorContext
  | CleanupErrorContext
  | JobErrorContext
  | HandlerErrorContext

export type ProcessErrorHandler = (
  error: unknown,
  context: ProcessErrorContext,
) => void | Promise<void>

export type Processor<Data> = (data: Data, context: ProcessContext) => void | Promise<void>

export interface WorkerHandle {
  /** Stop claiming jobs and wait for active handlers to finish. */
  close(): Promise<void>
}
