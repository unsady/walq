import type {
  ClaimedJob,
  ClaimInput,
  CompleteInput,
  EnqueueInput,
  FailInput,
  HeartbeatInput,
  LeaseMutationResult,
  StoredJob,
} from './types.js'

export type * from './types.js'

/**
 * Asynchronous adapter contract exposed through @walq/core/storage.
 * Behavioral guarantees and input requirements: docs/storage-contract.md.
 */
export interface Storage {
  /** Insert a pending job with a generated ID and zero attempts. */
  enqueue(input: EnqueueInput): Promise<StoredJob>

  /** Recover expired leases and atomically claim up to limit eligible jobs. */
  claim(input: ClaimInput): Promise<ClaimedJob[]>

  /** Complete a job only while its token matches and its lease is unexpired. */
  complete(input: CompleteInput): Promise<LeaseMutationResult>

  /** Record failure and either schedule a retry or mark the job failed. */
  fail(input: FailInput): Promise<LeaseMutationResult>

  /** Extend a live lease without changing its token or consuming an attempt. */
  heartbeat(input: HeartbeatInput): Promise<LeaseMutationResult>
}
