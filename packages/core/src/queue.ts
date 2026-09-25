import type { EnqueueInput, RetentionPolicy, RetentionRule, Storage } from '@walq/core/storage'

import { getCoordinator } from './coordinator.js'
import type {
  AddOptions,
  AddedJob,
  ProcessErrorHandler,
  ProcessOptions,
  Processor,
  QueueOptions,
  RetentionStatus,
  RetryBackoff,
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

function normalizeAddOptions(value: AddOptions | undefined): AddOptions {
  if (value === undefined) return {}
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new TypeError('add options must be an object')
  }

  const { delay, runAt } = value
  if (delay !== undefined && runAt !== undefined) {
    throw new TypeError('delay and runAt cannot be used together')
  }
  if (delay !== undefined && (!Number.isSafeInteger(delay) || delay < 0)) {
    throw new TypeError('delay must be a nonnegative safe integer')
  }
  if (runAt !== undefined && (!Number.isSafeInteger(runAt) || runAt < 0)) {
    throw new TypeError('runAt must be a nonnegative safe integer')
  }

  return {
    ...(delay !== undefined ? { delay } : {}),
    ...(runAt !== undefined ? { runAt } : {}),
  }
}

function prepareAdd(
  data: unknown,
  options: AddOptions | undefined,
): {
  data: string
  options: AddOptions
} {
  const normalizedOptions = normalizeAddOptions(options)
  const serialized = JSON.stringify(data)
  if (serialized === undefined) throw new TypeError('Data must be JSON serializable')

  return { data: serialized, options: normalizedOptions }
}

function availability(now: number, options: AddOptions): number {
  const availableAt = options.runAt ?? (options.delay === undefined ? now : now + options.delay)
  if (!Number.isSafeInteger(availableAt)) {
    throw new TypeError('availableAt must be a safe integer')
  }

  return availableAt
}

function buildEnqueueInput(
  queue: string,
  attempts: number,
  now: number,
  prepared: ReturnType<typeof prepareAdd>,
): EnqueueInput {
  return {
    queue,
    name: queue,
    data: prepared.data,
    now,
    availableAt: availability(now, prepared.options),
    attempts,
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

function normalizeRetry(value: QueueOptions['retry']): RetryBackoff | undefined {
  if (value === undefined) return undefined
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new TypeError('retry must be an object')
  }

  const backoff = value.backoff
  if (typeof backoff !== 'object' || backoff === null || Array.isArray(backoff)) {
    throw new TypeError('retry.backoff must be an object')
  }
  if (backoff.type !== 'fixed' && backoff.type !== 'exponential') {
    throw new TypeError('retry.backoff.type must be fixed or exponential')
  }
  if (!Number.isSafeInteger(backoff.delay) || backoff.delay < 0) {
    throw new TypeError('retry.backoff.delay must be a nonnegative safe integer')
  }

  const jitter = backoff.jitter === undefined ? 0 : backoff.jitter
  if (typeof jitter !== 'number' || !Number.isFinite(jitter) || jitter < 0 || jitter > 1) {
    throw new TypeError('retry.backoff.jitter must be a number between 0 and 1')
  }

  return { type: backoff.type, delay: backoff.delay, jitter }
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
  readonly #retryBackoff: RetryBackoff | undefined
  readonly #onError: ProcessErrorHandler | undefined
  readonly #retention: RetentionPolicy
  #worker: QueueWorker<Data> | undefined

  constructor(name: string, options: QueueOptions) {
    if (typeof name !== 'string' || name.length === 0) {
      throw new TypeError('Queue name must be a nonempty string')
    }

    const attempts = options.attempts ?? 1
    positiveInteger(attempts, 'attempts')
    const retryBackoff = normalizeRetry(options.retry)

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
    this.#retryBackoff = retryBackoff
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

  async add(data: Data, options?: AddOptions): Promise<AddedJob> {
    const prepared = prepareAdd(data, options)
    const now = Date.now()

    const coordinator = getCoordinator(this.#storage)
    const job = await coordinator.enqueue(
      buildEnqueueInput(this.#name, this.#attempts, now, prepared),
    )
    coordinator.wakeQueue(this.#name)
    return { id: job.id }
  }

  async addMany(items: Array<{ data: Data; options?: AddOptions }>): Promise<AddedJob[]> {
    if (!Array.isArray(items)) throw new TypeError('addMany items must be an array')
    if (items.length === 0) return []

    const prepared = Array.from(items, (item) => {
      if (typeof item !== 'object' || item === null || Array.isArray(item)) {
        throw new TypeError('addMany items must be objects')
      }

      return prepareAdd(item.data, item.options)
    })

    const now = Date.now()
    const inputs = prepared.map((item) => buildEnqueueInput(this.#name, this.#attempts, now, item))

    const coordinator = getCoordinator(this.#storage)
    const jobs = await coordinator.enqueueMany(inputs)
    coordinator.wakeQueue(this.#name)
    return jobs.map(({ id }) => ({ id }))
  }

  process(processor: Processor<Data>, options: ProcessOptions = {}): WorkerHandle {
    if (this.#worker) throw new Error(`Queue ${this.#name} is already being processed`)
    if (typeof processor !== 'function') throw new TypeError('processor must be a function')

    const concurrency = options.concurrency ?? 1
    positiveInteger(concurrency, 'concurrency')

    const coordinator = getCoordinator(this.#storage)
    const worker = new QueueWorker(coordinator, this.#name, processor, {
      concurrency,
      retryBackoff: this.#retryBackoff,
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
