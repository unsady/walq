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
export class StorageCoordinator {
  readonly #storage: Storage
  readonly #workers = new Set<CoordinatedWorker>()
  #operationTail: Promise<void> = Promise.resolve()
  #loop: Promise<void> | undefined
  #pollDelay: Delay | undefined
  #woken = false
  #cursor = 0

  constructor(storage: Storage) {
    this.#storage = storage
  }

  register(worker: CoordinatedWorker): void {
    this.#workers.add(worker)
    if (this.#loop === undefined) this.#loop = this.#run()
    else this.wake()
  }

  unregister(worker: CoordinatedWorker): void {
    this.#workers.delete(worker)
    this.wake()
  }

  /** Interrupt the poll wait so pending work is claimed without extra delay. */
  wake(): void {
    this.#woken = true
    this.#pollDelay?.finish()
  }

  enqueue(input: EnqueueInput): Promise<StoredJob> {
    return this.#operate(() => this.#storage.enqueue(input))
  }

  claim(input: ClaimInput): Promise<ClaimedJob[]> {
    return this.#operate(() => this.#storage.claim(input))
  }

  complete(input: CompleteInput): Promise<LeaseMutationResult> {
    return this.#operate(() => this.#storage.complete(input))
  }

  fail(input: FailInput): Promise<LeaseMutationResult> {
    return this.#operate(() => this.#storage.fail(input))
  }

  heartbeat(input: HeartbeatInput): Promise<LeaseMutationResult> {
    return this.#operate(() => this.#storage.heartbeat(input))
  }

  /** Run one storage operation at a time so writers never overlap in-process. */
  #operate<T>(operation: () => Promise<T>): Promise<T> {
    const result = this.#operationTail.then(operation)
    this.#operationTail = result.then(
      () => undefined,
      () => undefined,
    )
    return result
  }

  async #run(): Promise<void> {
    while (this.#workers.size > 0) {
      const dispatched = await this.#sweep()
      if (!dispatched && this.#workers.size > 0) await this.#waitForPoll()
    }
    this.#loop = undefined
  }

  /** Poll every worker once, rotating the start position for fairness. */
  async #sweep(): Promise<boolean> {
    let dispatched = false
    const workers = [...this.#workers]
    const count = workers.length

    for (let index = 0; index < count; index += 1) {
      const worker = workers[(this.#cursor + index) % count]
      if (worker === undefined || !this.#workers.has(worker)) continue

      let started = 0
      try {
        started = await worker.poll()
      } catch {
        // Polling resumes after the next interval.
      }
      if (started > 0) dispatched = true
    }

    if (count > 0) this.#cursor = (this.#cursor + 1) % count
    return dispatched
  }

  async #waitForPoll(): Promise<void> {
    if (this.#woken) {
      this.#woken = false
      return
    }

    const pollDelay = delay(pollInterval)
    this.#pollDelay = pollDelay
    await pollDelay.promise
    this.#pollDelay = undefined
    this.#woken = false
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
