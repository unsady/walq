import type { Storage } from '@walq/core/storage'

export interface QueueOptions {
  storage: Storage
  /** Total allowed executions for every job in this queue. */
  attempts?: number
  /**
   * Called when a claim, lease mutation, or handler fails. Absent means errors
   * are written to `console.error`. The callback may be async; its own errors
   * are reported without affecting queue execution.
   */
  onError?: ProcessErrorHandler
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
export type ProcessErrorOperation = 'claim' | 'heartbeat' | 'complete' | 'fail' | 'handler'

/** A claim failed before any job was acquired, so no job context exists. */
export interface ClaimErrorContext {
  readonly queue: string
  readonly operation: 'claim'
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
export type ProcessErrorContext = ClaimErrorContext | JobErrorContext | HandlerErrorContext

export type ProcessErrorHandler = (
  error: unknown,
  context: ProcessErrorContext,
) => void | Promise<void>

export type Processor<Data> = (data: Data, context: ProcessContext) => void | Promise<void>

export interface WorkerHandle {
  /** Stop claiming jobs and wait for active handlers to finish. */
  close(): Promise<void>
}
