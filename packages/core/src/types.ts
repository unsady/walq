/** Internal identifiers. Callers must not rely on their format. */
export type JobId = string
export type QueueName = string
export type LeaseToken = string

export type JobStatus = 'pending' | 'active' | 'completed' | 'failed'

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

export interface HeartbeatInput {
  id: JobId
  leaseToken: LeaseToken
  now: number
  /** Extend expiry to max(current expiry, now + leaseDuration). */
  leaseDuration: number
}

/** Missing, expired, or superseded leases all produce lease_lost. */
export type LeaseMutationResult = 'applied' | 'lease_lost'
