import type { ClaimedJob, Storage } from '@walq/core/storage'

import type { Processor, WorkerHandle } from './types.js'

const pollInterval = 1_000
const leaseDuration = 30_000
const heartbeatInterval = 10_000

interface Delay {
  promise: Promise<void>
  finish(): void
}

function delay(duration: number): Delay {
  let settled = false
  let resolvePromise: () => void
  const promise = new Promise<void>((resolve) => {
    resolvePromise = resolve
  })
  const timer = setTimeout(finish, duration)

  function finish(): void {
    if (settled) return
    settled = true
    clearTimeout(timer)
    resolvePromise()
  }

  return { promise, finish }
}

function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.stack ?? error.message
  try {
    return String(error)
  } catch {
    return 'Unknown handler error'
  }
}

export class QueueWorker<Payload> implements WorkerHandle {
  readonly #storage: Storage
  readonly #queue: string
  readonly #processor: Processor<Payload>
  readonly #concurrency: number
  readonly #active = new Set<Promise<void>>()
  readonly #loop: Promise<void>
  readonly #onClose: () => void
  #pollDelay: Delay | undefined
  #closing = false

  constructor(
    storage: Storage,
    queue: string,
    processor: Processor<Payload>,
    concurrency: number,
    onClose: () => void,
  ) {
    this.#storage = storage
    this.#queue = queue
    this.#processor = processor
    this.#concurrency = concurrency
    this.#onClose = onClose
    this.#loop = this.#run()
  }

  async close(): Promise<void> {
    if (!this.#closing) {
      this.#closing = true
      this.#wakePoller()
    }
    await this.#loop
  }

  wake(): void {
    this.#wakePoller()
  }

  async #run(): Promise<void> {
    try {
      while (!this.#closing) {
        const freeSlots = this.#concurrency - this.#active.size
        if (freeSlots === 0) {
          await this.#waitForPoll()
          continue
        }

        let jobs: ClaimedJob[]
        try {
          jobs = await this.#storage.claim({
            queue: this.#queue,
            limit: freeSlots,
            now: Date.now(),
            leaseDuration,
          })
        } catch {
          if (!this.#closing) await this.#waitForPoll()
          continue
        }

        for (const job of jobs) this.#start(job)
        if (jobs.length === 0 && !this.#closing) await this.#waitForPoll()
      }
    } finally {
      await Promise.all(this.#active)
      this.#onClose()
    }
  }

  #start(job: ClaimedJob): void {
    const task = this.#process(job)
    this.#active.add(task)
    void task.finally(() => {
      this.#active.delete(task)
      this.#wakePoller()
    })
  }

  async #process(job: ClaimedJob): Promise<void> {
    const controller = new AbortController()
    let stopped = false
    let leaseLost = false
    let heartbeatDelay: Delay | undefined

    const heartbeat = async () => {
      while (!stopped) {
        heartbeatDelay = delay(heartbeatInterval)
        await heartbeatDelay.promise
        if (stopped) return

        try {
          const result = await this.#storage.heartbeat({
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
      const payload = JSON.parse(job.payload) as Payload
      await this.#processor(payload, {
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
        await this.#storage.complete({
          id: job.id,
          leaseToken: job.leaseToken,
          now: Date.now(),
        })
      } else {
        const now = Date.now()
        await this.#storage.fail({
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

  async #waitForPoll(): Promise<void> {
    this.#pollDelay = delay(pollInterval)
    await this.#pollDelay.promise
    this.#pollDelay = undefined
  }

  #wakePoller(): void {
    this.#pollDelay?.finish()
  }
}
