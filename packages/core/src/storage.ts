import type {
  ClaimedJob,
  ClaimInput,
  ClaimQueuesInput,
  ClaimQueuesResult,
  CleanupInput,
  CleanupResult,
  CompleteInput,
  EnqueueInput,
  FailInput,
  HeartbeatInput,
  LeaseMutationResult,
  StoredJob,
} from './storage-types.js'

export type * from './storage-types.js'

/**
 * Asynchronous adapter contract exposed through `@walq/core/storage`.
 * See https://github.com/unsady/walq/blob/main/docs/storage-contract.md.
 */
export interface Storage {
  /** Insert a pending job with a generated ID and zero attempts made. */
  enqueue(input: EnqueueInput): Promise<StoredJob>

  /** Atomically insert all pending jobs and return them in input order. */
  enqueueMany(inputs: EnqueueInput[]): Promise<StoredJob[]>

  /** Recover expired leases and atomically claim up to limit eligible jobs. */
  claim(input: ClaimInput): Promise<ClaimedJob[]>

  /**
   * Optional grouped variant of claim: apply every request in order and return
   * one result per request. Each request keeps the guarantees of claim. An
   * adapter may share one transaction across the whole batch; callers must not
   * assume atomicity across requests beyond what the adapter documents.
   * Coordinators fall back to claim() when this method is absent.
   */
  claimQueues?(input: ClaimQueuesInput): Promise<ClaimQueuesResult>

  /** Complete a job only while its token matches and its lease is unexpired. */
  complete(input: CompleteInput): Promise<LeaseMutationResult>

  /** Record failure and either schedule a retry or mark the job failed. */
  fail(input: FailInput): Promise<LeaseMutationResult>

  /** Extend a live lease without changing its token or consuming an attempt. */
  heartbeat(input: HeartbeatInput): Promise<LeaseMutationResult>

  /** Bounded removal of terminal jobs beyond a queue's retention policy. */
  cleanup(input: CleanupInput): Promise<CleanupResult>
}
