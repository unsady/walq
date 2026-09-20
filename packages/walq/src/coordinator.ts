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

export class StorageCoordinator {
  readonly #storage: Storage
  readonly #workers = new Map<CoordinatedWorker, WorkerState>()
  #loop: Promise<void> | undefined
  #pollDelay: Delay | undefined
  #cursor = 0

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

  claim(input: ClaimInput): Promise<ClaimedJob[]> {
    return this.#storage.claim(input)
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
