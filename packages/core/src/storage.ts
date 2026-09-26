import type {
  CancelInput,
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
  InspectInput,
  JobSnapshot,
  LeaseMutationResult,
  ListInput,
  RemoveInput,
  RescheduleInput,
  RetryInput,
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

  /** Inspect one job by its exact queue and ID; return null when absent. */
  inspect(input: InspectInput): Promise<JobSnapshot | null>

  /** List jobs in one required status, with a positive safe-integer limit. */
  list(input: ListInput): Promise<JobSnapshot[]>

  /**
   * Retry a failed job immediately while retaining attemptsMade and error. If
   * exhausted, increase attempts to attemptsMade + 1. Return false unless the
   * job is failed in this queue.
   */
  retry(input: RetryInput): Promise<boolean>

  /** Cancel a pending job and retain it as a terminal cancelled snapshot. */
  cancel(input: CancelInput): Promise<boolean>

  /** Change the absolute availability time of a pending job. */
  reschedule(input: RescheduleInput): Promise<boolean>

  /** Physically remove a job unless it is active. */
  remove(input: RemoveInput): Promise<boolean>

  /** Complete a job only while its token matches and its lease is unexpired. */
  complete(input: CompleteInput): Promise<LeaseMutationResult>

  /** Record failure and either schedule a retry or mark the job failed. */
  fail(input: FailInput): Promise<LeaseMutationResult>

  /** Extend a live lease without changing its token or consuming an attempt. */
  heartbeat(input: HeartbeatInput): Promise<LeaseMutationResult>

  /** Bounded removal of terminal jobs beyond a queue's retention policy. */
  cleanup(input: CleanupInput): Promise<CleanupResult>
}
