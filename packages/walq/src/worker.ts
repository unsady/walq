import type { ClaimedJob } from '@walq/core/storage'

import type { CoordinatedWorker, StorageCoordinator } from './coordinator.js'
import { deferred, delay, type Delay } from './delay.js'
import type { Processor } from './types.js'

const leaseDuration = 30_000
const heartbeatInterval = 10_000

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.stack ?? error.message
  try {
    return String(error)
  } catch {
    return 'Unknown handler error'
  }
}

export class QueueWorker<Data> implements CoordinatedWorker {
  readonly #coordinator: StorageCoordinator
  readonly #queue: string
  readonly #processor: Processor<Data>
  readonly #concurrency: number
  readonly #active = new Set<Promise<void>>()
  readonly #done = deferred()
  #closing = false
  #polling = false

  constructor(
    coordinator: StorageCoordinator,
    queue: string,
    processor: Processor<Data>,
    concurrency: number,
  ) {
    this.#coordinator = coordinator
    this.#queue = queue
    this.#processor = processor
    this.#concurrency = concurrency
  }

  /** Claim jobs for free slots and start their handlers. Returns their count. */
  async poll(): Promise<number> {
    if (this.#closing || this.#polling) return 0

    const limit = this.#concurrency - this.#active.size
    if (limit <= 0) return 0

    this.#polling = true
    try {
      const jobs = await this.#coordinator.claim({
        queue: this.#queue,
        limit,
        now: Date.now(),
        leaseDuration,
      })
      for (const job of jobs) this.#start(job)
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
      this.#settle()
    }
    await this.#done.promise
  }

  #settle(): void {
    if (this.#closing && !this.#polling && this.#active.size === 0) this.#done.resolve()
  }

  #start(job: ClaimedJob): void {
    const task = this.#process(job)
    this.#active.add(task)
    void task.finally(() => {
      this.#active.delete(task)
      this.#coordinator.wakeWorker(this)
      this.#settle()
    })
  }

  async #process(job: ClaimedJob): Promise<void> {
    const controller = new AbortController()
    let stopped = false
    let leaseLost = false
    let heartbeatDelay: Delay | undefined

    const heartbeat = async (): Promise<void> => {
      while (!stopped) {
        heartbeatDelay = delay(heartbeatInterval)
        await heartbeatDelay.promise
        if (stopped) return

        try {
          const result = await this.#coordinator.heartbeat({
            id: job.id,
            leaseToken: job.leaseToken,
            now: Date.now(),
            leaseDuration,
          })
          if (result === 'lease_lost') {
            leaseLost = true
            controller.abort()
            return
          }
        } catch {
          // A later heartbeat or lease mutation can still establish the outcome.
        }
      }
    }

    const heartbeatTask = heartbeat()
    let succeeded = false
    let failure: unknown
    try {
      const data = JSON.parse(job.data) as Data
      await this.#processor(data, {
        signal: controller.signal,
        jobId: job.id,
        attempt: job.attemptsMade,
      })
      succeeded = true
    } catch (error) {
      failure = error
    } finally {
      stopped = true
      heartbeatDelay?.finish()
      await heartbeatTask
    }

    if (leaseLost) return

    try {
      if (succeeded) {
        await this.#coordinator.complete({
          id: job.id,
          leaseToken: job.leaseToken,
          now: Date.now(),
        })
      } else {
        const now = Date.now()
        await this.#coordinator.fail({
          id: job.id,
          leaseToken: job.leaseToken,
          now,
          error: errorMessage(failure),
          retryAt: now,
        })
      }
    } catch {
      // The lease will be recovered if the final mutation did not commit.
    }
  }
}
