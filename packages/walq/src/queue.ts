import type { RetentionPolicy, RetentionRule, Storage } from 'walq/storage'

import { getCoordinator } from './coordinator.js'
import type {
  AddedJob,
  ProcessErrorHandler,
  ProcessOptions,
  Processor,
  QueueOptions,
  RetentionStatus,
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

function retentionCount(
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

function retentionMaxAge(value: number | null | undefined, name: string): number | null {
  if (value === undefined || value === null) return null
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(`${name} must be null or a nonnegative safe integer`)
  }

  return value
}

/** Normalize the public shorthand and rule object into the strict core shape. */
function normalizeRetention(
  value: RetentionStatus | undefined,
  fallbackCount: number,
  name: string,
): RetentionRule {
  if (value === undefined) return { count: fallbackCount, maxAge: null }
  if (value === null) return { count: null, maxAge: null }
  if (typeof value === 'number')
    return { count: retentionCount(value, fallbackCount, name), maxAge: null }
  if (typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError(`${name} must be a number, null, or a retention rule`)
  }

  return {
    count: retentionCount(value.count, fallbackCount, `${name}.count`),
    maxAge: retentionMaxAge(value.maxAge, `${name}.maxAge`),
  }
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
    if (
      retention !== undefined &&
      (typeof retention !== 'object' || retention === null || Array.isArray(retention))
    ) {
      throw new TypeError('retention must be an object')
    }

    this.#name = name
    this.#storage = options.storage
    this.#attempts = attempts
    this.#onError = onError
    this.#retention = {
      completed: normalizeRetention(
        retention?.completed,
        defaultCompletedRetention,
        'retention.completed',
      ),
      failed: normalizeRetention(retention?.failed, defaultFailedRetention, 'retention.failed'),
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
