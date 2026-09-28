import type {
  EnqueueInput,
  JobSnapshot,
  RetentionPolicy,
  RetentionRule,
  Storage,
} from '@walq/core/storage'
import { CronExpressionParser } from 'cron-parser'

import { getCoordinator } from './coordinator.js'
import type {
  AddOptions,
  AddedJob,
  Job,
  JobStatus,
  ListOptions,
  ProcessErrorHandler,
  ProcessManyOptions,
  ProcessOptions,
  Processor,
  QueueStats,
  ProcessorMany,
  QueueOptions,
  RetentionStatus,
  RetryBackoff,
  Schedule,
  ScheduleOptions,
  WorkerHandle,
} from './types.js'
import { QueueWorker } from './worker.js'

const defaultCompletedRetention = 0
const defaultFailedRetention = 100
const defaultListLimit = 100
const defaultProcessManyBatchSize = 10
const maxListLimit = 1_000
const jobStatuses: readonly JobStatus[] = ['pending', 'active', 'completed', 'failed', 'cancelled']

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function validateJobId(id: unknown): asserts id is string {
  if (typeof id !== 'string' || id.length === 0) {
    throw new TypeError('id must be a nonempty string')
  }
}

function normalizeScheduleRegistration(value: unknown): ScheduleOptions {
  if (!isRecord(value)) throw new TypeError('schedule options must be an object')
  validateJobId(value.id)

  const hasEvery = value.every !== undefined
  const hasCron = value.cron !== undefined
  if (hasEvery === hasCron) throw new TypeError('exactly one of every or cron must be provided')

  if (hasEvery) {
    positiveInteger(value.every as number, 'every')
    return { id: value.id, every: value.every as number }
  }
  if (typeof value.cron !== 'string' || value.cron.length === 0) {
    throw new TypeError('cron must be a nonempty string')
  }
  try {
    CronExpressionParser.parse(value.cron, { tz: 'UTC' })
  } catch (error) {
    throw new TypeError(`Invalid cron expression: ${String(error)}`)
  }

  return { id: value.id, cron: value.cron }
}

function normalizeListOptions(value: unknown): Required<ListOptions> {
  if (!isRecord(value)) {
    throw new TypeError('list options must be an object')
  }

  const status = jobStatuses.find((candidate) => candidate === value.status)
  const limit = value.limit === undefined ? defaultListLimit : value.limit
  if (status === undefined) {
    throw new TypeError('status must be a supported job status')
  }
  if (
    typeof limit !== 'number' ||
    !Number.isSafeInteger(limit) ||
    limit <= 0 ||
    limit > maxListLimit
  ) {
    throw new TypeError(`limit must be a positive safe integer no greater than ${maxListLimit}`)
  }

  return { status, limit }
}

function normalizeScheduleOptions(value: unknown, operation: 'add' | 'reschedule'): AddOptions {
  if (value === undefined && operation === 'add') return {}
  if (!isRecord(value)) {
    throw new TypeError(`${operation} options must be an object`)
  }

  const { delay, runAt } = value
  if (delay !== undefined && runAt !== undefined) {
    throw new TypeError(
      operation === 'add'
        ? 'delay and runAt cannot be used together'
        : 'exactly one of delay or runAt must be provided',
    )
  }
  if (operation === 'reschedule' && delay === undefined && runAt === undefined) {
    throw new TypeError('exactly one of delay or runAt must be provided')
  }
  if (
    delay !== undefined &&
    (typeof delay !== 'number' || !Number.isSafeInteger(delay) || delay < 0)
  ) {
    throw new TypeError('delay must be a nonnegative safe integer')
  }
  if (
    runAt !== undefined &&
    (typeof runAt !== 'number' || !Number.isSafeInteger(runAt) || runAt < 0)
  ) {
    throw new TypeError('runAt must be a nonnegative safe integer')
  }

  if (delay !== undefined) return { delay }
  if (runAt !== undefined) return { runAt }
  return {}
}

function publicJob<Data>(snapshot: JobSnapshot): Job<Data> {
  return {
    id: snapshot.id,
    data: JSON.parse(snapshot.data) as Data,
    status: snapshot.status,
    attempt: snapshot.attemptsMade,
    attempts: snapshot.attempts,
    createdAt: snapshot.createdAt,
    availableAt: snapshot.availableAt,
    priority: snapshot.priority,
    finishedAt: snapshot.finishedAt,
    error: snapshot.error,
  }
}

function positiveInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`)
  }
}

function availability(now: number, options: AddOptions): number {
  const availableAt = options.runAt ?? (options.delay === undefined ? now : now + options.delay)
  if (!Number.isSafeInteger(availableAt)) {
    throw new TypeError('availableAt must be a safe integer')
  }

  return availableAt
}

function normalizeGroup(value: unknown): { id: string; concurrency: number } | undefined {
  if (value === undefined) return undefined

  const group = typeof value === 'string' ? { id: value } : value
  if (!isRecord(group)) {
    throw new TypeError('group must be a nonempty string or a group object')
  }

  const { id, concurrency = 1 } = group
  if (typeof id !== 'string' || id.length === 0) {
    throw new TypeError('group.id must be a nonempty string')
  }
  if (typeof concurrency !== 'number') {
    throw new TypeError('group.concurrency must be a positive safe integer')
  }
  positiveInteger(concurrency, 'group.concurrency')

  return { id, concurrency }
}

function buildEnqueueInput(
  queue: string,
  attempts: number,
  now: number,
  data: unknown,
  options: AddOptions | undefined,
): EnqueueInput {
  const schedule = normalizeScheduleOptions(options, 'add')
  const priority = options?.priority === undefined ? 0 : options.priority
  if (!Number.isSafeInteger(priority)) {
    throw new TypeError('priority must be a safe integer')
  }

  const dedupe = options?.dedupe
  if (dedupe !== undefined && (typeof dedupe !== 'string' || dedupe.length === 0)) {
    throw new TypeError('dedupe must be a nonempty string')
  }

  const group = normalizeGroup(options?.group)
  const serialized = JSON.stringify(data)
  if (serialized === undefined) throw new TypeError('Data must be JSON serializable')

  const input: EnqueueInput = {
    queue,
    name: queue,
    data: serialized,
    now,
    availableAt: availability(now, schedule),
    priority,
    attempts,
  }
  if (dedupe !== undefined) input.dedupe = dedupe
  if (group !== undefined) input.group = group

  return input
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

function normalizeBackoff(value: QueueOptions['backoff']): RetryBackoff | undefined {
  if (value === undefined) return undefined
  if (!isRecord(value)) {
    throw new TypeError('backoff must be an object')
  }
  if (value.type !== 'fixed' && value.type !== 'exponential') {
    throw new TypeError('backoff.type must be fixed or exponential')
  }
  if (!Number.isSafeInteger(value.delay) || value.delay < 0) {
    throw new TypeError('backoff.delay must be a nonnegative safe integer')
  }

  const jitter = value.jitter === undefined ? 0 : value.jitter
  if (typeof jitter !== 'number' || !Number.isFinite(jitter) || jitter < 0 || jitter > 1) {
    throw new TypeError('backoff.jitter must be a number between 0 and 1')
  }

  return { type: value.type, delay: value.delay, jitter }
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
  if (!isRecord(value)) {
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
    const retryBackoff = normalizeBackoff(options.backoff)

    const onError = options.onError
    if (onError !== undefined && typeof onError !== 'function') {
      throw new TypeError('onError must be a function')
    }

    const retention = options.retention
    if (retention !== undefined && !isRecord(retention)) {
      throw new TypeError('retention must be an object')
    }

    const retentionOptions: QueueOptions['retention'] = retention

    this.#name = name
    this.#storage = options.storage
    this.#attempts = attempts
    this.#retryBackoff = retryBackoff
    this.#onError = onError
    this.#retention = {
      completed: normalizeRetention(
        retentionOptions?.completed,
        defaultCompletedRetention,
        'retention.completed',
      ),
      failed: normalizeRetention(
        retentionOptions?.failed,
        defaultFailedRetention,
        'retention.failed',
      ),
    }
  }

  async add(data: Data, options?: AddOptions): Promise<AddedJob> {
    const input = buildEnqueueInput(this.#name, this.#attempts, Date.now(), data, options)
    const coordinator = getCoordinator(this.#storage)
    const job = await coordinator.enqueue(input)
    coordinator.wakeQueue(this.#name)
    return { id: job.id }
  }

  async addMany(items: Array<{ data: Data; options?: AddOptions }>): Promise<AddedJob[]> {
    if (!Array.isArray(items)) throw new TypeError('addMany items must be an array')
    if (items.length === 0) return []

    const now = Date.now()
    const inputs = Array.from(items, (item) => {
      if (!isRecord(item)) {
        throw new TypeError('addMany items must be objects')
      }

      return buildEnqueueInput(this.#name, this.#attempts, now, item.data, item.options)
    })

    const coordinator = getCoordinator(this.#storage)
    const jobs = await coordinator.enqueueMany(inputs)
    coordinator.wakeQueue(this.#name)
    return jobs.map(({ id }) => ({ id }))
  }

  async get(id: string): Promise<Job<Data> | null> {
    validateJobId(id)
    const snapshot = await this.#storage.inspect({ queue: this.#name, id })
    return snapshot === null ? null : publicJob<Data>(snapshot)
  }

  async schedule(data: Data, options: ScheduleOptions): Promise<void> {
    const normalized = normalizeScheduleRegistration(options)
    const serialized = JSON.stringify(data)
    if (serialized === undefined) throw new TypeError('Data must be JSON serializable')
    const upsertSchedule = this.#storage.upsertSchedule
    if (upsertSchedule === undefined) {
      throw new Error('Storage adapter does not support durable schedules')
    }

    await upsertSchedule.call(this.#storage, {
      queue: this.#name,
      id: normalized.id,
      data: serialized,
      now: Date.now(),
      ...('every' in normalized ? { every: normalized.every } : { cron: normalized.cron }),
    })
    getCoordinator(this.#storage).wakeQueue(this.#name)
  }

  async getSchedule(id: string): Promise<Schedule<Data> | null> {
    validateJobId(id)
    const getSchedule = this.#storage.getSchedule
    if (getSchedule === undefined) {
      throw new Error('Storage adapter does not support durable schedules')
    }

    const stored = await getSchedule.call(this.#storage, { queue: this.#name, id })
    if (stored === null) return null

    const common = {
      id: stored.id,
      data: JSON.parse(stored.data) as Data,
      nextRunAt: stored.nextRunAt,
    }
    return stored.every !== undefined
      ? { ...common, every: stored.every }
      : { ...common, cron: stored.cron! }
  }

  async removeSchedule(id: string): Promise<boolean> {
    validateJobId(id)
    const removeSchedule = this.#storage.removeSchedule
    if (removeSchedule === undefined) {
      throw new Error('Storage adapter does not support durable schedules')
    }

    const removed = await removeSchedule.call(this.#storage, { queue: this.#name, id })
    if (removed) getCoordinator(this.#storage).wakeQueue(this.#name)
    return removed
  }

  async list(options: ListOptions): Promise<Job<Data>[]> {
    const { status, limit } = normalizeListOptions(options)
    const snapshots = await this.#storage.list({ queue: this.#name, status, limit })
    return snapshots.map((snapshot) => publicJob<Data>(snapshot))
  }

  async stats(): Promise<QueueStats> {
    return this.#storage.count({ queue: this.#name })
  }

  async pause(): Promise<void> {
    await this.#storage.pause({ queue: this.#name })
  }

  async resume(): Promise<void> {
    await this.#storage.resume({ queue: this.#name })
    getCoordinator(this.#storage).wakeQueue(this.#name)
  }

  async retry(id: string): Promise<boolean> {
    validateJobId(id)
    const retried = await this.#storage.retry({ queue: this.#name, id, now: Date.now() })
    if (retried) getCoordinator(this.#storage).wakeQueue(this.#name)
    return retried
  }

  async cancel(id: string): Promise<boolean> {
    validateJobId(id)
    return this.#storage.cancel({ queue: this.#name, id, now: Date.now() })
  }

  async reschedule(id: string, options: { delay?: number; runAt?: number }): Promise<boolean> {
    validateJobId(id)
    const normalizedOptions = normalizeScheduleOptions(options, 'reschedule')
    const now = Date.now()
    const availableAt = availability(now, normalizedOptions)

    const rescheduled = await this.#storage.reschedule({ queue: this.#name, id, availableAt })
    if (rescheduled) getCoordinator(this.#storage).wakeQueue(this.#name)
    return rescheduled
  }

  async remove(id: string): Promise<boolean> {
    validateJobId(id)
    return this.#storage.remove({ queue: this.#name, id })
  }

  process(processor: Processor<Data>, options: ProcessOptions = {}): WorkerHandle {
    if (this.#worker) throw new Error(`Queue ${this.#name} is already being processed`)
    if (typeof processor !== 'function') throw new TypeError('processor must be a function')

    const concurrency = options.concurrency ?? 1
    positiveInteger(concurrency, 'concurrency')

    return this.#startWorker(processor, {
      concurrency,
      batchSize: 1,
      processMany: false,
    })
  }

  /** Process claimed jobs in batches; concurrency counts simultaneous batch calls. */
  processMany(processor: ProcessorMany<Data>, options: ProcessManyOptions = {}): WorkerHandle {
    if (this.#worker) throw new Error(`Queue ${this.#name} is already being processed`)
    if (typeof processor !== 'function') throw new TypeError('processor must be a function')
    if (!isRecord(options)) {
      throw new TypeError('processMany options must be an object')
    }

    const processOptions: ProcessManyOptions = options
    const concurrency = processOptions.concurrency === undefined ? 1 : processOptions.concurrency
    const batch =
      processOptions.batch === undefined ? defaultProcessManyBatchSize : processOptions.batch
    positiveInteger(concurrency, 'concurrency')
    positiveInteger(batch, 'batch')
    if (batch > Math.floor(Number.MAX_SAFE_INTEGER / concurrency)) {
      throw new TypeError('concurrency times batch must be a safe integer')
    }

    return this.#startWorker(processor, { concurrency, batchSize: batch, processMany: true })
  }

  #startWorker(
    processor: Processor<Data> | ProcessorMany<Data>,
    options: { concurrency: number; batchSize: number; processMany: boolean },
  ): WorkerHandle {
    const coordinator = getCoordinator(this.#storage)
    const worker = new QueueWorker(coordinator, this.#name, processor, {
      ...options,
      retryBackoff: this.#retryBackoff,
      onError: this.#onError,
      retention: this.#retention,
      attempts: this.#attempts,
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
