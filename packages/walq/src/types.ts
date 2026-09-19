import type { Storage } from '@walq/core/storage'

export interface QueueOptions {
  storage: Storage
  /** Total allowed executions for every job in this queue. */
  attempts?: number
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

export type Processor<Payload> = (payload: Payload, context: ProcessContext) => void | Promise<void>

export interface WorkerHandle {
  /** Stop claiming jobs and wait for active handlers to finish. */
  close(): Promise<void>
}
