import type { RetentionPolicy, Storage } from '@walq/core/storage'

import { getCoordinator } from './coordinator.js'
import type {
  AddedJob,
  ProcessErrorHandler,
  ProcessOptions,
  Processor,
  QueueOptions,
  WorkerHandle,
} from './types.js'
import { QueueWorker } from './worker.js'

const defaultCompletedRetention = 0
const defaultFailedRetention = 100

function positiveInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`)
  }
}

function retentionValue(
  value: number | null | undefined,
  fallback: number,
  name: string,
): number | null {
  if (value === undefined) return fallback
  if (value === null) return null
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(`${name} must be null or a nonnegative safe integer`)
  }

  return value
}

export class Queue<Data> {
  readonly #name: string
  readonly #storage: Storage
  readonly #attempts: number
  readonly #onError: ProcessErrorHandler | undefined
  readonly #retention: RetentionPolicy
  #worker: QueueWorker<Data> | undefined

  constructor(name: string, options: QueueOptions) {
    if (typeof name !== 'string' || name.length === 0) {
      throw new TypeError('Queue name must be a nonempty string')
    }

    const attempts = options.attempts ?? 1
    positiveInteger(attempts, 'attempts')

    const onError = options.onError
    if (onError !== undefined && typeof onError !== 'function') {
      throw new TypeError('onError must be a function')
    }

    const retention = options.retention
    if (retention !== undefined && (typeof retention !== 'object' || retention === null)) {
      throw new TypeError('retention must be an object')
    }

    this.#name = name
    this.#storage = options.storage
    this.#attempts = attempts
    this.#onError = onError
    this.#retention = {
      completed: retentionValue(
        retention?.completed,
        defaultCompletedRetention,
        'retention.completed',
      ),
      failed: retentionValue(retention?.failed, defaultFailedRetention, 'retention.failed'),
    }
  }

  async add(data: Data): Promise<AddedJob> {
    const serialized = JSON.stringify(data)
    if (serialized === undefined) throw new TypeError('Data must be JSON serializable')

    const now = Date.now()
    const coordinator = getCoordinator(this.#storage)
    const job = await coordinator.enqueue({
      queue: this.#name,
      name: this.#name,
      data: serialized,
      now,
      availableAt: now,
      attempts: this.#attempts,
    })
    coordinator.wakeQueue(this.#name)
    return { id: job.id }
  }

  process(processor: Processor<Data>, options: ProcessOptions = {}): WorkerHandle {
    if (this.#worker) throw new Error(`Queue ${this.#name} is already being processed`)
    if (typeof processor !== 'function') throw new TypeError('processor must be a function')

    const concurrency = options.concurrency ?? 1
    positiveInteger(concurrency, 'concurrency')

    const coordinator = getCoordinator(this.#storage)
    const worker = new QueueWorker(coordinator, this.#name, processor, {
      concurrency,
      onError: this.#onError,
      retention: this.#retention,
    })
    // Registration can reject a second worker for the same queue name, so it
    // must happen before any maintenance or polling starts.
    coordinator.register(this.#name, worker)
    this.#worker = worker
    worker.start()

    return {
      close: async (): Promise<void> => {
        await worker.close()
        if (this.#worker === worker) this.#worker = undefined
      },
    }
  }
}
