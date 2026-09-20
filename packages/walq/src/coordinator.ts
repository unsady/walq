import type {
  ClaimInput,
  ClaimedJob,
  CompleteInput,
  EnqueueInput,
  FailInput,
  HeartbeatInput,
  LeaseMutationResult,
  Storage,
  StoredJob,
} from '@walq/core/storage'

import { delay, type Delay } from './delay.js'

const pollInterval = 1_000

/** A queue worker that the coordinator polls for new jobs. */
export type CoordinatedWorker = {
  poll(): Promise<number>
}

/**
 * Single poller and single storage path for every worker of one Storage.
 * Workers register on process() and unregister on close().
 */
type WorkerState = {
  queue: string
  nextPollAt: number
}

/** One claim waiting for the current microtask batch to flush. */
type PendingClaim = {
  input: ClaimInput
  resolve: (jobs: ClaimedJob[]) => void
  reject: (error: unknown) => void
}

export class StorageCoordinator {
  readonly #storage: Storage
  readonly #workers = new Map<CoordinatedWorker, WorkerState>()
  #loop: Promise<void> | undefined
  #pollDelay: Delay | undefined
  #cursor = 0
  #pendingClaims: PendingClaim[] | undefined

  constructor(storage: Storage) {
    this.#storage = storage
  }

  register(queue: string, worker: CoordinatedWorker): void {
    this.#workers.set(worker, { queue, nextPollAt: 0 })
    if (this.#loop === undefined) this.#loop = this.#run()
    else this.#pollDelay?.finish()
  }

  unregister(worker: CoordinatedWorker): void {
    this.#workers.delete(worker)
    this.#pollDelay?.finish()
  }

  /** Wake workers for a queue after new work is committed. */
  wakeQueue(queue: string): void {
    let woken = false
    for (const state of this.#workers.values()) {
      if (state.queue !== queue) continue
      state.nextPollAt = 0
      woken = true
    }
    if (woken) this.#pollDelay?.finish()
  }

  /** Wake one worker after one of its handler slots becomes free. */
  wakeWorker(worker: CoordinatedWorker): void {
    const state = this.#workers.get(worker)
    if (state === undefined) return

    state.nextPollAt = 0
    this.#pollDelay?.finish()
  }

  enqueue(input: EnqueueInput): Promise<StoredJob> {
    return this.#storage.enqueue(input)
  }

  /**
   * Claim from one queue. When the adapter supports grouped claims, requests
   * issued in the same microtask batch are coalesced into one call so a single
   * poller sweep acquires one storage transaction instead of one per queue.
   */
  claim(input: ClaimInput): Promise<ClaimedJob[]> {
    if (this.#storage.claimQueues === undefined) return this.#storage.claim(input)

    return new Promise<ClaimedJob[]>((resolve, reject) => {
      const pending = this.#pendingClaims
      const request: PendingClaim = { input, resolve, reject }
      if (pending === undefined) {
        this.#pendingClaims = [request]
        queueMicrotask(() => {
          void this.#flushClaims()
        })
      } else {
        pending.push(request)
      }
    })
  }

  /** Run one grouped storage call and map results back to request order. */
  async #flushClaims(): Promise<void> {
    const batch = this.#pendingClaims
    this.#pendingClaims = undefined
    if (batch === undefined || batch.length === 0) return

    try {
      const claimQueues = this.#storage.claimQueues
      if (claimQueues === undefined) {
        for (const request of batch) {
          this.#storage.claim(request.input).then(request.resolve, request.reject)
        }
        return
      }

      const results = await claimQueues.call(this.#storage, {
        requests: batch.map((request) => request.input),
      })
      if (results.length !== batch.length) {
        throw new Error(
          `Grouped claim returned ${results.length} results for ${batch.length} requests`,
        )
      }
      for (const [index, request] of batch.entries()) request.resolve(results[index] ?? [])
    } catch (error) {
      for (const request of batch) request.reject(error)
    }
  }

  complete(input: CompleteInput): Promise<LeaseMutationResult> {
    return this.#storage.complete(input)
  }

  fail(input: FailInput): Promise<LeaseMutationResult> {
    return this.#storage.fail(input)
  }

  heartbeat(input: HeartbeatInput): Promise<LeaseMutationResult> {
    return this.#storage.heartbeat(input)
  }

  async #run(): Promise<void> {
    while (this.#workers.size > 0) {
      await this.#sweep()
      if (this.#workers.size > 0) await this.#waitForPoll()
    }
    this.#loop = undefined
  }

  /** Poll ready workers concurrently, rotating their start order for fairness. */
  async #sweep(): Promise<void> {
    const workers = [...this.#workers.keys()]
    const count = workers.length
    const polls: Promise<void>[] = []

    for (let index = 0; index < count; index += 1) {
      const worker = workers[(this.#cursor + index) % count]
      if (worker === undefined) continue

      const state = this.#workers.get(worker)
      if (state === undefined || state.nextPollAt > Date.now()) continue

      // Set the backoff before polling so a wake during poll is not overwritten.
      state.nextPollAt = Date.now() + pollInterval
      polls.push(
        worker.poll().then(
          () => undefined,
          () => undefined,
        ),
      )
    }

    await Promise.all(polls)
    if (count > 0) this.#cursor = (this.#cursor + 1) % count
  }

  async #waitForPoll(): Promise<void> {
    const nextPollAt = Math.min(...[...this.#workers.values()].map((state) => state.nextPollAt))
    const pollDelay = delay(Math.max(0, nextPollAt - Date.now()))
    this.#pollDelay = pollDelay
    await pollDelay.promise
    if (this.#pollDelay === pollDelay) this.#pollDelay = undefined
  }
}

const coordinators = new WeakMap<Storage, StorageCoordinator>()

export function getCoordinator(storage: Storage): StorageCoordinator {
  let coordinator = coordinators.get(storage)
  if (coordinator === undefined) {
    coordinator = new StorageCoordinator(storage)
    coordinators.set(storage, coordinator)
  }
  return coordinator
}
