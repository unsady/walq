/** Internal identifiers. Callers must not rely on their format. */
export type JobId = string
export type QueueName = string
export type LeaseToken = string

export type JobStatus = 'pending' | 'active' | 'completed' | 'failed' | 'cancelled'

/** All timestamps are Unix time in milliseconds. Data is serialized JSON. */
export interface StoredJob {
  id: JobId
  queue: QueueName
  name: string
  data: string
  status: JobStatus
  createdAt: number
  availableAt: number
  /** Number of successful claims, including claims whose workers crashed. */
  attemptsMade: number
  /** Total allowed attempts, including the first one. */
  attempts: number
  /** Most recent handler error; lease expiration does not overwrite it. */
  error: string | null
}

/** A persisted job snapshot, including the time it entered a terminal state. */
export interface JobSnapshot extends StoredJob {
  finishedAt: number | null
}

export interface ClaimedJob extends StoredJob {
  status: 'active'
  leaseToken: LeaseToken
  expiresAt: number
}

/** Storage generates a unique ID. Enqueue does not provide deduplication. */
export interface EnqueueInput {
  queue: QueueName
  name: string
  data: string
  now: number
  availableAt: number
  attempts: number
}

export interface ClaimInput {
  queue: QueueName
  limit: number
  now: number
  /** Positive duration in milliseconds, measured from now. */
  leaseDuration: number
}

/**
 * A grouped claim over several queues. Requests are applied in order and the
 * result at each index corresponds to the request at the same index.
 */
export interface ClaimQueuesInput {
  /** Each request keeps the exact semantics of a standalone claim. */
  requests: ClaimInput[]
}

/** Result of a grouped claim; `results[k]` belongs to `requests[k]`. */
export type ClaimQueuesResult = ClaimedJob[][]

export interface InspectInput {
  queue: QueueName
  id: JobId
}

/** List persisted jobs of one required status, ordered deterministically by status. */
export interface ListInput {
  queue: QueueName
  status: JobStatus
  /** Maximum number of snapshots to return; the storage contract has no default. */
  limit: number
}

export interface RetryInput extends InspectInput {
  /** Current time; a retried job is immediately available. */
  now: number
}

export interface CancelInput extends InspectInput {
  now: number
}

export interface RescheduleInput extends InspectInput {
  /** Absolute Unix time in milliseconds at which the pending job becomes available. */
  availableAt: number
}

export type RemoveInput = InspectInput

export interface CompleteInput {
  id: JobId
  leaseToken: LeaseToken
  now: number
}

export interface FailInput {
  id: JobId
  leaseToken: LeaseToken
  now: number
  error: string
  /** Null means terminal failure. Retry cannot exceed attempts. */
  retryAt: number | null
}

/**
 * Retention bounds for one terminal status. A row is eligible when it is older
 * than the newest `count` rows or finished before `now - maxAge`; the two bounds
 * are combined as a union. A null bound is disabled.
 */
export interface RetentionRule {
  /** Newest terminal rows to keep; null disables the count bound. */
  count: number | null
  /** Maximum terminal age in milliseconds; null disables the age bound. */
  maxAge: number | null
}

/** Retention bounds for both terminal statuses. */
export interface RetentionPolicy {
  completed: RetentionRule
  failed: RetentionRule
}

/** Bounded removal of terminal jobs that exceed a queue's retention policy. */
export interface CleanupInput {
  queue: QueueName
  retention: RetentionPolicy
  /** Current time for age bounds; finite nonnegative safe-integer milliseconds. */
  now: number
  /** Maximum number of rows this call may delete. */
  limit: number
}

export interface CleanupResult {
  /** Terminal rows removed by this call. */
  removed: number
  /** Another call may still find eligible terminal rows. */
  more: boolean
}

export interface HeartbeatInput {
  id: JobId
  leaseToken: LeaseToken
  now: number
  /** Extend expiry to max(current expiry, now + leaseDuration). */
  leaseDuration: number
}

/** Missing, expired, or superseded leases all produce lease_lost. */
export type LeaseMutationResult = 'applied' | 'lease_lost'
