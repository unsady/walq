import type { ClaimedJob, CleanupResult, RetentionPolicy } from '@walq/core/storage'

import type { CoordinatedWorker, StorageCoordinator } from './coordinator.js'
import { deferred, delay, type Delay } from './delay.js'
import type {
  ProcessErrorContext,
  ProcessErrorHandler,
  ProcessManyJob,
  Processor,
  ProcessorMany,
  RetryBackoff,
} from './types.js'

const leaseDuration = 30_000
const heartbeatInterval = 10_000
const cleanupInterval = 1_000
const cleanupBatch = 500

export interface WorkerOptions {
  concurrency: number
  batchSize: number
  processMany: boolean
  retryBackoff: RetryBackoff | undefined
  retention: RetentionPolicy
  attempts: number
  onError: ProcessErrorHandler | undefined
}

interface ActiveJob {
  job: ClaimedJob
  controller: AbortController
  data: unknown
  succeeded: boolean
  failure: unknown
  leaseLost: boolean
  stopped: boolean
  heartbeatDelay: Delay | undefined
}

function retryAt(now: number, attemptsMade: number, backoff: RetryBackoff | undefined): number {
  if (backoff === undefined) return now

  const baseBackoff =
    backoff.type === 'fixed'
      ? backoff.delay
      : backoff.delay === 0
        ? 0
        : backoff.delay * 2 ** (attemptsMade - 1)
  const maxRetryAt = Number.MAX_SAFE_INTEGER
  const maxDelay = maxRetryAt - now
  // Storage timestamps must remain safe integers, so saturate extreme backoffs.
  if (baseBackoff >= maxDelay) return maxRetryAt

  const jitter = backoff.jitter ?? 0
  const jitteredDelay =
    jitter === 0 || baseBackoff === 0 ? baseBackoff : baseBackoff * (1 - Math.random() * jitter)

  return Math.min(maxRetryAt, now + Math.ceil(jitteredDelay))
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.stack ?? error.message
  try {
    return String(error)
  } catch {
    return 'Unknown handler error'
  }
}

function describeContext(context: ProcessErrorContext): string {
  const parts = [`walq queue "${context.queue}" ${context.operation} failed`]
  if (
    context.operation !== 'claim' &&
    context.operation !== 'cleanup' &&
    context.operation !== 'schedule'
  ) {
    parts.push(`job ${context.jobId}`, `attempt ${context.attempt}`)
  }
  return parts.join(', ')
}

function safeConsoleLog(...args: unknown[]): void {
  try {
    console.error(...args)
  } catch {
    // Observability must never break queue execution.
  }
}

function safeConsoleError(error: unknown, context: ProcessErrorContext): void {
  safeConsoleLog(describeContext(context), error, context)
}

function safeConsoleCallbackError(
  error: unknown,
  context: ProcessErrorContext,
  callbackError: unknown,
): void {
  safeConsoleLog(
    `walq onError callback failed (${context.operation} in queue "${context.queue}")`,
    error,
    context,
    callbackError,
  )
}

export class QueueWorker<Data> implements CoordinatedWorker {
  readonly #coordinator: StorageCoordinator
  readonly #queue: string
  readonly #processor: Processor<Data> | ProcessorMany<Data>
  readonly #concurrency: number
  readonly #batchSize: number
  readonly #processMany: boolean
  readonly #retryBackoff: RetryBackoff | undefined
  readonly #onError: ProcessErrorHandler | undefined
  readonly #retention: RetentionPolicy
  readonly #attempts: number
  readonly #cleanupEnabled: boolean
  readonly #active = new Set<Promise<void>>()
  readonly #done = deferred()
  #cleanupNeeded = false
  #cleanupTask: Promise<void> | undefined
  #cleanupTimer: Delay | undefined
  #nextCleanupAt = 0
  #closing = false
  #polling = false

  constructor(
    coordinator: StorageCoordinator,
    queue: string,
    processor: Processor<Data> | ProcessorMany<Data>,
    options: WorkerOptions,
  ) {
    this.#coordinator = coordinator
    this.#queue = queue
    this.#processor = processor
    this.#concurrency = options.concurrency
    this.#batchSize = options.batchSize
    this.#processMany = options.processMany
    this.#retryBackoff = options.retryBackoff
    this.#onError = options.onError
    this.#retention = options.retention
    this.#attempts = options.attempts
    this.#cleanupEnabled = (['completed', 'failed'] as const).some((status) => {
      const rule = options.retention[status]
      return rule.count !== null || rule.maxAge !== null
    })
  }

  /** Start maintenance after the worker is registered with the coordinator. */
  start(): void {
    // A restart may find terminal rows that the previous process left behind.
    this.#scheduleCleanup()
  }

  async poll(): Promise<number> {
    if (this.#closing || this.#polling) return 0

    this.#polling = true
    try {
      try {
        const materialization = this.#coordinator.materializeSchedules({
          queue: this.#queue,
          now: Date.now(),
          attempts: this.#attempts,
        })
        if (materialization !== undefined) await materialization
      } catch (error) {
        this.#report(error, { queue: this.#queue, operation: 'schedule' })
      }

      const availableBatches = this.#concurrency - this.#active.size
      if (availableBatches <= 0) return 0
      const limit = availableBatches * this.#batchSize

      let jobs: ClaimedJob[]
      try {
        jobs = await this.#coordinator.claim({
          queue: this.#queue,
          limit,
          now: Date.now(),
          leaseDuration,
        })
      } catch (error) {
        this.#report(error, { queue: this.#queue, operation: 'claim' })
        return 0
      }
      // A claim also recovers expired leases, which can produce terminal rows
      // that no complete() or fail() call in this process observes.
      this.#scheduleCleanup()
      for (let index = 0; index < jobs.length; index += this.#batchSize) {
        this.#start(jobs.slice(index, index + this.#batchSize))
      }
      return jobs.length
    } finally {
      this.#polling = false
      this.#settle()
    }
  }

  /** Stop claiming jobs and wait for active handlers without aborting them. */
  async close(): Promise<void> {
    if (!this.#closing) {
      this.#closing = true
      this.#coordinator.unregister(this)
      this.#cleanupTimer?.finish()
      this.#settle()
    }
    await this.#done.promise
    // Bounded cleanup batches in flight must finish before close() resolves.
    await this.#cleanupTask
  }

  #settle(): void {
    if (this.#closing && !this.#polling && this.#active.size === 0) this.#done.resolve()
  }

  #start(jobs: ClaimedJob[]): void {
    const task = this.#process(jobs)
    this.#active.add(task)
    void task
      .finally(() => {
        this.#active.delete(task)
        this.#coordinator.wakeWorker(this)
        this.#settle()
      })
      .catch((error: unknown) => {
        safeConsoleLog(`walq queue "${this.#queue}" processing failed unexpectedly`, error)
      })
  }

  async #process(jobs: ClaimedJob[]): Promise<void> {
    const states: ActiveJob[] = jobs.map((job) => ({
      job,
      controller: new AbortController(),
      data: undefined,
      succeeded: false,
      failure: undefined,
      leaseLost: false,
      stopped: false,
      heartbeatDelay: undefined,
    }))
    const heartbeatTasks = states.map((state) =>
      this.#heartbeat(state).catch((error: unknown) => {
        safeConsoleLog(`walq queue "${this.#queue}" heartbeat failed unexpectedly`, error)
      }),
    )
    const validStates: ActiveJob[] = []

    for (const state of states) {
      try {
        state.data = JSON.parse(state.job.data) as Data
        validStates.push(state)
      } catch (error) {
        state.failure = error
      }
    }

    try {
      if (this.#processMany) {
        const batch = validStates.map((state): ProcessManyJob<Data> => ({
          data: state.data as Data,
          context: {
            signal: state.controller.signal,
            jobId: state.job.id,
            attempt: state.job.attemptsMade,
          },
        }))
        if (batch.length > 0) {
          try {
            await (this.#processor as ProcessorMany<Data>)(batch)
            for (const state of validStates) state.succeeded = true
          } catch (error) {
            for (const state of validStates) state.failure = error
          }
        }
      } else {
        const state = validStates[0]
        if (state !== undefined) {
          try {
            await (this.#processor as Processor<Data>)(state.data as Data, {
              signal: state.controller.signal,
              jobId: state.job.id,
              attempt: state.job.attemptsMade,
            })
            state.succeeded = true
          } catch (error) {
            state.failure = error
          }
        }
      }
    } finally {
      for (const state of states) {
        state.stopped = true
        state.heartbeatDelay?.finish()
      }
      await Promise.all(heartbeatTasks)
    }

    for (const state of states) {
      if (!state.succeeded) {
        this.#report(state.failure, {
          queue: this.#queue,
          operation: 'handler',
          jobId: state.job.id,
          attempt: state.job.attemptsMade,
          attemptsExhausted: state.job.attemptsMade >= state.job.attempts,
        })
      }
    }

    await Promise.all(states.map((state) => this.#finish(state)))
  }

  async #heartbeat(state: ActiveJob): Promise<void> {
    while (!state.stopped) {
      state.heartbeatDelay = delay(heartbeatInterval)
      await state.heartbeatDelay.promise
      if (state.stopped) return

      try {
        const result = await this.#coordinator.heartbeat({
          id: state.job.id,
          leaseToken: state.job.leaseToken,
          now: Date.now(),
          leaseDuration,
        })
        if (result === 'lease_lost') {
          state.leaseLost = true
          state.controller.abort()
          return
        }
      } catch (error) {
        // A later heartbeat or lease mutation can still establish the outcome.
        this.#report(error, {
          queue: this.#queue,
          operation: 'heartbeat',
          jobId: state.job.id,
          attempt: state.job.attemptsMade,
        })
      }
    }
  }

  async #finish(state: ActiveJob): Promise<void> {
    if (state.leaseLost) return

    const { job } = state
    try {
      if (state.succeeded) {
        const context = { id: job.id, leaseToken: job.leaseToken, now: Date.now() }
        if ((await this.#coordinator.complete(context)) === 'applied') this.#scheduleCleanup()
      } else {
        const now = Date.now()
        const retryTimestamp =
          job.attemptsMade < job.attempts ? retryAt(now, job.attemptsMade, this.#retryBackoff) : now
        const result = await this.#coordinator.fail({
          id: job.id,
          leaseToken: job.leaseToken,
          now,
          error: errorMessage(state.failure),
          retryAt: retryTimestamp,
        })
        // The adapter schedules a retry while attempts remain, so only an
        // exhausted attempt budget produces a terminal row.
        if (result === 'applied' && job.attemptsMade >= job.attempts) this.#scheduleCleanup()
      }
    } catch (error) {
      // The lease will be recovered if the final mutation did not commit.
      this.#report(error, {
        queue: this.#queue,
        operation: state.succeeded ? 'complete' : 'fail',
        jobId: job.id,
        attempt: job.attemptsMade,
      })
    }
  }

  #report(error: unknown, context: ProcessErrorContext): void {
    const onError = this.#onError
    if (onError === undefined) {
      safeConsoleError(error, context)
      return
    }

    try {
      void Promise.resolve(onError(error, context)).catch((callbackError: unknown) => {
        safeConsoleCallbackError(error, context, callbackError)
      })
    } catch (callbackError) {
      safeConsoleCallbackError(error, context, callbackError)
    }
  }

  /** Coalesce terminal transitions into at most one cleanup pass per interval. */
  #scheduleCleanup(): void {
    if (!this.#cleanupEnabled || this.#closing) return
    this.#cleanupNeeded = true
    if (this.#cleanupTask !== undefined) return
    this.#cleanupTask = this.#runCleanup().catch((error: unknown) => {
      this.#report(error, { queue: this.#queue, operation: 'cleanup' })
    })
  }

  async #runCleanup(): Promise<void> {
    try {
      // Never run database work in the caller's continuation: defer the first
      // batch so process() and terminal transitions stay off the cleanup path.
      await this.#pauseCleanup(0)
      if (this.#closing) return
      await this.#drainCleanup()
    } finally {
      this.#cleanupTimer = undefined
      this.#cleanupTask = undefined
      if (this.#cleanupNeeded && !this.#closing) this.#scheduleCleanup()
    }
  }

  /** Wait through a tracked timer so close() can interrupt the pause. */
  async #pauseCleanup(duration: number): Promise<void> {
    const timer = delay(duration)
    this.#cleanupTimer = timer
    await timer.promise
    this.#cleanupTimer = undefined
  }

  /** Delete bounded batches until nothing is left, work is coalesced, or close. */
  async #drainCleanup(): Promise<void> {
    let throttled = false
    while (!this.#closing && this.#cleanupNeeded) {
      this.#cleanupNeeded = false
      if (!throttled) {
        throttled = true
        const wait = this.#nextCleanupAt - Date.now()
        if (wait > 0) {
          await this.#pauseCleanup(wait)
          if (this.#closing) return
        }
      }

      // Record the throttle before the call so a failing adapter is retried at
      // the same bounded rate as a successful one.
      this.#nextCleanupAt = Date.now() + cleanupInterval
      const result = await this.#cleanupBatch()
      if (result === undefined || !result.more) return

      // Keep draining one bounded batch at a time, yielding between batches so
      // polling, heartbeats, and handler work are not starved.
      this.#cleanupNeeded = true
      await this.#pauseCleanup(0)
    }
  }

  /** Run one bounded batch; adapter errors are reported and end the pass. */
  async #cleanupBatch(): Promise<CleanupResult | undefined> {
    try {
      return await this.#coordinator.cleanup({
        queue: this.#queue,
        retention: this.#retention,
        now: Date.now(),
        limit: cleanupBatch,
      })
    } catch (error) {
      this.#report(error, { queue: this.#queue, operation: 'cleanup' })
      return undefined
    }
  }
}
