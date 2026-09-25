import type { Storage } from '@walq/core/storage'

/**
 * Public retention rule for one terminal status. A `number` is the count
 * shorthand, `null` keeps every job of that status, and an object sets either
 * bound independently.
 */
export type RetentionStatus =
  | number
  | null
  | {
      /** Newest rows to keep; omitted uses the status default, null disables it. */
      count?: number | null
      /** Maximum age in milliseconds; omitted or null disables the age bound. */
      maxAge?: number | null
    }

/** Queue retention overrides; omitted statuses use the queue defaults. */
export interface RetentionOptions {
  completed?: RetentionStatus
  failed?: RetentionStatus
}

export type RetryBackoff =
  | {
      type: 'fixed'
      /** Base delay in milliseconds. */
      delay: number
      /** Positive-only jitter fraction from 0 to 1; defaults to 0. */
      jitter?: number
    }
  | {
      type: 'exponential'
      /** Initial delay in milliseconds, doubled after each failed attempt. */
      delay: number
      /** Positive-only jitter fraction from 0 to 1; defaults to 0. */
      jitter?: number
    }

export interface RetryOptions {
  backoff: RetryBackoff
}

export interface QueueOptions {
  storage: Storage
  /** Total allowed executions for every job in this queue. */
  attempts?: number
  /** Optional backoff for handler failures; omitted retries immediately. */
  retry?: RetryOptions
  /**
   * Called when a claim, lease mutation, cleanup, or handler fails. Absent
   * means errors are written to `console.error`. The callback may be async; its
   * own errors are reported without affecting queue execution.
   */
  onError?: ProcessErrorHandler
  /**
   * Terminal-job retention for this queue, per status. Defaults to
   * `{ completed: 0, failed: 100 }`: completed jobs are removed as soon as a
   * cleanup pass runs, while the newest 100 failures are kept. A number keeps
   * that many newest rows, null keeps every row of that status, and an object
   * sets `count` and/or `maxAge` independently. `maxAge` is milliseconds; a row
   * is eligible when it exceeds either bound.
   */
  retention?: RetentionOptions
}

export interface ProcessOptions {
  /** Maximum number of handlers running at once. */
  concurrency?: number
}

/** Scheduling options for `queue.add()`; `delay` and `runAt` are mutually exclusive. */
export interface AddOptions {
  /** Relative delay in nonnegative safe-integer milliseconds. */
  delay?: number
  /** Absolute Unix timestamp in nonnegative safe-integer milliseconds. */
  runAt?: number
}

export interface AddedJob {
  id: string
}

export interface ProcessContext {
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
