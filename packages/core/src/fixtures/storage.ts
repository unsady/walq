import type {
  ClaimedJob,
  ClaimInput,
  ClaimQueuesInput,
  CleanupInput,
  CleanupResult,
  CompleteInput,
  EnqueueInput,
  FailInput,
  HeartbeatInput,
  LeaseMutationResult,
  QueueInput,
  Storage,
  StoredJob,
} from '@walq/core/storage'

export const now = 1_000

export function claimedJob(
  id: string,
  options: { queue?: string; data?: string } = {},
): ClaimedJob {
  const queue = options.queue ?? 'email'
  return {
    id,
    queue,
    name: queue,
    data: options.data ?? '{}',
    status: 'active',
    createdAt: now,
    availableAt: now,
    priority: 0,
    attemptsMade: 1,
    attempts: 3,
    error: null,
    leaseToken: `lease-${id}`,
    expiresAt: now + 30_000,
  }
}

export class TestStorage implements Storage {
  readonly enqueues: EnqueueInput[] = []
  readonly enqueueManyCalls: EnqueueInput[][] = []
  readonly enqueueManyErrors: unknown[] = []
  readonly pauses: QueueInput[] = []
  readonly resumes: QueueInput[] = []
  readonly claims: ClaimInput[] = []
  readonly completions: CompleteInput[] = []
  readonly failures: FailInput[] = []
  readonly heartbeats: HeartbeatInput[] = []
  readonly claimErrors: unknown[] = []
  readonly completeErrors: unknown[] = []
  readonly failErrors: unknown[] = []
  readonly heartbeatErrors: unknown[] = []
  heartbeatResult: LeaseMutationResult = 'applied'
  readonly heartbeatResults = new Map<string, LeaseMutationResult>()
  jobs: ClaimedJob[] = []
  maxConcurrentCalls = 0
  #runningCalls = 0
  #nextJobId = 0
  #deduplicatedJobs = new Map<string, StoredJob>()

  #enqueue(input: EnqueueInput): StoredJob {
    const key = input.dedupe === undefined ? undefined : JSON.stringify([input.queue, input.dedupe])
    const existing = key === undefined ? undefined : this.#deduplicatedJobs.get(key)
    if (existing !== undefined) return existing

    const job: StoredJob = {
      id: `job-${++this.#nextJobId}`,
      queue: input.queue,
      name: input.name,
      data: input.data,
      status: 'pending',
      createdAt: input.now,
      availableAt: input.availableAt,
      priority: input.priority,
      attemptsMade: 0,
      attempts: input.attempts,
      error: null,
    }
    if (key !== undefined) this.#deduplicatedJobs.set(key, job)
    return job
  }

  async enqueue(input: EnqueueInput): Promise<StoredJob> {
    this.#enter()
    this.enqueues.push(input)
    return this.#leave(this.#enqueue(input))
  }

  async enqueueMany(inputs: EnqueueInput[]): Promise<StoredJob[]> {
    this.#enter()
    this.enqueueManyCalls.push(inputs)
    this.#maybeThrow(this.enqueueManyErrors)
    return this.#leave(inputs.map((input) => this.#enqueue(input)))
  }

  async pause(input: QueueInput): Promise<void> {
    this.pauses.push(input)
  }

  async resume(input: QueueInput): Promise<void> {
    this.resumes.push(input)
  }

  async claim(input: ClaimInput): Promise<ClaimedJob[]> {
    this.#enter()
    this.claims.push(input)
    this.#maybeThrow(this.claimErrors)
    const claimed = this.jobs.filter((job) => job.queue === input.queue).slice(0, input.limit)
    this.jobs = this.jobs.filter((job) => !claimed.includes(job))
    return this.#leave(claimed)
  }

  async inspect() {
    return null
  }

  async count() {
    return { pending: 0, active: 0, completed: 0, failed: 0, cancelled: 0 }
  }

  async list() {
    return []
  }

  async retry() {
    return false
  }

  async cancel() {
    return false
  }

  async reschedule() {
    return false
  }

  async remove() {
    return false
  }

  async complete(input: CompleteInput): Promise<LeaseMutationResult> {
    this.#enter()
    this.completions.push(input)
    this.#maybeThrow(this.completeErrors)
    return this.#leave('applied')
  }

  async fail(input: FailInput): Promise<LeaseMutationResult> {
    this.#enter()
    this.failures.push(input)
    this.#maybeThrow(this.failErrors)
    return this.#leave('applied')
  }

  async heartbeat(input: HeartbeatInput): Promise<LeaseMutationResult> {
    this.#enter()
    this.heartbeats.push(input)
    this.#maybeThrow(this.heartbeatErrors)
    return this.#leave(this.heartbeatResults.get(input.id) ?? this.heartbeatResult)
  }

  async cleanup(_input: CleanupInput): Promise<CleanupResult> {
    return { removed: 0, more: false }
  }

  #enter(): void {
    this.#runningCalls += 1
    this.maxConcurrentCalls = Math.max(this.maxConcurrentCalls, this.#runningCalls)
  }

  async #leave<T>(value: T): Promise<T> {
    await Promise.resolve()
    this.#runningCalls -= 1
    return value
  }

  #maybeThrow(errors: unknown[]): void {
    if (errors.length === 0) return
    this.#runningCalls -= 1
    throw errors.shift()
  }
}

/** Adds the optional grouped claim capability routed through `claim`. */
export class GroupedTestStorage extends TestStorage {
  groupedError: unknown = undefined

  async claimQueues({ requests }: ClaimQueuesInput): Promise<ClaimedJob[][]> {
    const error = this.groupedError
    if (error !== undefined) {
      this.groupedError = undefined
      throw error
    }
    return Promise.all(requests.map((request) => this.claim(request)))
  }
}

/** Records and controls bounded cleanup calls. */
export class CleanupTestStorage extends TestStorage {
  readonly cleanups: CleanupInput[] = []
  readonly cleanupResults: CleanupResult[] = []
  readonly cleanupErrors: unknown[] = []
  cleanupResult: CleanupResult = { removed: 0, more: false }
  cleanupGate: Promise<void> | undefined

  async cleanup(input: CleanupInput): Promise<CleanupResult> {
    this.cleanups.push(input)
    const error = this.cleanupErrors.shift()
    if (error !== undefined) throw error
    await this.cleanupGate
    return this.cleanupResults.shift() ?? this.cleanupResult
  }
}
