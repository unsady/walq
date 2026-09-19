import type { Storage } from '@walq/core/storage'

import type { AddedJob, ProcessOptions, Processor, QueueOptions, WorkerHandle } from './types.js'
import { QueueWorker } from './worker.js'

function positiveInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`)
  }
}

export class Queue<Payload> {
  readonly #name: string
  readonly #storage: Storage
  readonly #attempts: number
  #worker: QueueWorker<Payload> | undefined

  constructor(name: string, options: QueueOptions) {
    if (typeof name !== 'string' || name.length === 0) {
      throw new TypeError('Queue name must be a nonempty string')
    }

    const attempts = options.attempts ?? 1
    positiveInteger(attempts, 'attempts')

    this.#name = name
    this.#storage = options.storage
    this.#attempts = attempts
  }

  async add(payload: Payload): Promise<AddedJob> {
    const serialized = JSON.stringify(payload)
    if (serialized === undefined) throw new TypeError('Payload must be JSON serializable')

    const now = Date.now()
    const job = await this.#storage.enqueue({
      queue: this.#name,
      name: this.#name,
      payload: serialized,
      now,
      availableAt: now,
      attempts: this.#attempts,
    })
    this.#worker?.wake()
    return { id: job.id }
  }

  process(processor: Processor<Payload>, options: ProcessOptions = {}): WorkerHandle {
    if (this.#worker) throw new Error(`Queue ${this.#name} is already being processed`)
    if (typeof processor !== 'function') throw new TypeError('processor must be a function')

    const concurrency = options.concurrency ?? 1
    positiveInteger(concurrency, 'concurrency')

    const worker = new QueueWorker(this.#storage, this.#name, processor, concurrency, () => {
      if (this.#worker === worker) this.#worker = undefined
    })
    this.#worker = worker
    return worker
  }
}
