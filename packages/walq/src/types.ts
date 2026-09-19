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

export type Processor<Payload> = (payload: Payload) => void | Promise<void>

export interface WorkerHandle {
  /** Stop claiming jobs and wait for active handlers to finish. */
  close(): Promise<void>
}
