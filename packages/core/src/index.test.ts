import type {
  ClaimedJob,
  ClaimInput,
  ClaimQueuesInput,
  CleanupInput,
  CleanupResult,
  CompleteInput,
  EnqueueInput,
  FailInput,
  HeartbeatInput,
  LeaseMutationResult,
  Storage,
  StoredJob,
} from '@walq/core/storage'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { deferred } from './delay.js'
import { Queue, type ProcessErrorContext, type ProcessErrorHandler } from './index.js'

const now = 1_000

function claimedJob(id: string, options: { queue?: string; data?: string } = {}): ClaimedJob {
  const queue = options.queue ?? 'email'
  return {
    id,
    queue,
    name: queue,
    data: options.data ?? '{}',
    status: 'active',
    createdAt: now,
    availableAt: now,
    attemptsMade: 1,
    attempts: 3,
    error: null,
    leaseToken: `lease-${id}`,
    expiresAt: now + 30_000,
  }
}

class TestStorage implements Storage {
  readonly enqueues: EnqueueInput[] = []
  readonly enqueueManyCalls: EnqueueInput[][] = []
  readonly enqueueManyErrors: unknown[] = []
  readonly claims: ClaimInput[] = []
  readonly completions: CompleteInput[] = []
  readonly failures: FailInput[] = []
  readonly heartbeats: HeartbeatInput[] = []
  readonly claimErrors: unknown[] = []
  readonly completeErrors: unknown[] = []
  readonly failErrors: unknown[] = []
  readonly heartbeatErrors: unknown[] = []
  heartbeatResult: LeaseMutationResult = 'applied'
  jobs: ClaimedJob[] = []
  maxConcurrentCalls = 0
  #runningCalls = 0
  #nextJobId = 0

  async enqueue(input: EnqueueInput): Promise<StoredJob> {
    this.#enter()
    this.enqueues.push(input)
    const job: StoredJob = {
      id: `job-${++this.#nextJobId}`,
      queue: input.queue,
      name: input.name,
      data: input.data,
      status: 'pending',
      createdAt: input.now,
      availableAt: input.availableAt,
      attemptsMade: 0,
      attempts: input.attempts,
      error: null,
    }
    return this.#leave(job)
  }

  async enqueueMany(inputs: EnqueueInput[]): Promise<StoredJob[]> {
    this.#enter()
    this.enqueueManyCalls.push(inputs)
    this.#maybeThrow(this.enqueueManyErrors)
    const jobs = inputs.map((input) => ({
      id: `job-${++this.#nextJobId}`,
      queue: input.queue,
      name: input.name,
      data: input.data,
      status: 'pending' as const,
      createdAt: input.now,
      availableAt: input.availableAt,
      attemptsMade: 0,
      attempts: input.attempts,
      error: null,
    }))
    return this.#leave(jobs)
  }

  async claim(input: ClaimInput): Promise<ClaimedJob[]> {
    this.#enter()
    this.claims.push(input)
    this.#maybeThrow(this.claimErrors)
    const claimed = this.jobs.filter((job) => job.queue === input.queue).slice(0, input.limit)
    this.jobs = this.jobs.filter((job) => !claimed.includes(job))
    return this.#leave(claimed)
  }

  async inspect() {
    return null
  }

  async list() {
    return []
  }

  async retry() {
    return false
  }

  async cancel() {
    return false
  }

  async reschedule() {
    return false
  }

  async remove() {
    return false
  }

  async complete(input: CompleteInput): Promise<LeaseMutationResult> {
    this.#enter()
    this.completions.push(input)
    this.#maybeThrow(this.completeErrors)
    return this.#leave('applied')
  }

  async fail(input: FailInput): Promise<LeaseMutationResult> {
    this.#enter()
    this.failures.push(input)
    this.#maybeThrow(this.failErrors)
    return this.#leave('applied')
  }

  async heartbeat(input: HeartbeatInput): Promise<LeaseMutationResult> {
    this.#enter()
    this.heartbeats.push(input)
    this.#maybeThrow(this.heartbeatErrors)
    return this.#leave(this.heartbeatResult)
  }

  async cleanup(_input: CleanupInput): Promise<CleanupResult> {
    return { removed: 0, more: false }
  }

  #enter(): void {
    this.#runningCalls += 1
    this.maxConcurrentCalls = Math.max(this.maxConcurrentCalls, this.#runningCalls)
  }

  async #leave<T>(value: T): Promise<T> {
    await Promise.resolve()
    this.#runningCalls -= 1
    return value
  }

  #maybeThrow(errors: unknown[]): void {
    if (errors.length === 0) return
    this.#runningCalls -= 1
    throw errors.shift()
  }
}

/** Adds the optional grouped claim capability routed through `claim`. */
class GroupedTestStorage extends TestStorage {
  groupedError: unknown = undefined

  async claimQueues({ requests }: ClaimQueuesInput): Promise<ClaimedJob[][]> {
    const error = this.groupedError
    if (error !== undefined) {
      this.groupedError = undefined
      throw error
    }
    return Promise.all(requests.map((request) => this.claim(request)))
  }
}

/** Records and controls bounded cleanup calls. */
class CleanupTestStorage extends TestStorage {
  readonly cleanups: CleanupInput[] = []
  readonly cleanupResults: CleanupResult[] = []
  readonly cleanupErrors: unknown[] = []
  cleanupResult: CleanupResult = { removed: 0, more: false }
  cleanupGate: Promise<void> | undefined

  async cleanup(input: CleanupInput): Promise<CleanupResult> {
    this.cleanups.push(input)
    const error = this.cleanupErrors.shift()
    if (error !== undefined) throw error
    await this.cleanupGate
    return this.cleanupResults.shift() ?? this.cleanupResult
  }
}

afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
})

describe('Queue', () => {
  it('adds serialized data with queue defaults', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(now)
    const storage = new TestStorage()
    const queue = new Queue<{ userId: string }>('email', { storage })

    await expect(queue.add({ userId: '123' })).resolves.toEqual({ id: 'job-1' })
    expect(storage.enqueues).toEqual([
      {
        queue: 'email',
        name: 'email',
        data: '{"userId":"123"}',
        now,
        availableAt: now,
        attempts: 1,
      },
    ])
  })

  it('sets availability from a delay or absolute run time', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(10_000)
    const storage = new TestStorage()
    const queue = new Queue('email', { storage })

    await queue.add({}, { delay: 0 })
    await queue.add({}, { delay: 25 })
    await queue.add({}, { runAt: 0 })
    await queue.add({}, { runAt: 9_000 })

    expect(storage.enqueues.map(({ now, availableAt }) => ({ now, availableAt }))).toEqual([
      { now: 10_000, availableAt: 10_000 },
      { now: 10_000, availableAt: 10_025 },
      { now: 10_000, availableAt: 0 },
      { now: 10_000, availableAt: 9_000 },
    ])
  })

  it('accepts safe-integer availability boundaries', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(0)
    const storage = new TestStorage()
    const queue = new Queue('email', { storage })

    await queue.add({}, { delay: Number.MAX_SAFE_INTEGER })
    await queue.add({}, { runAt: Number.MAX_SAFE_INTEGER })

    expect(storage.enqueues.map(({ availableAt }) => availableAt)).toEqual([
      Number.MAX_SAFE_INTEGER,
      Number.MAX_SAFE_INTEGER,
    ])
  })

  it('adds many jobs atomically with one clock reading and ordered results', async () => {
    const clock = vi.spyOn(Date, 'now').mockReturnValue(now)
    const storage = new TestStorage()
    const queue = new Queue('email', { storage, attempts: 4 })

    await expect(
      queue.addMany([
        { data: { index: 0 } },
        { data: { index: 1 }, options: { delay: 25 } },
        { data: { index: 2 }, options: { runAt: 0 } },
      ]),
    ).resolves.toEqual([{ id: 'job-1' }, { id: 'job-2' }, { id: 'job-3' }])

    expect(clock).toHaveBeenCalledTimes(1)
    expect(storage.enqueueManyCalls).toEqual([
      [
        {
          queue: 'email',
          name: 'email',
          data: '{"index":0}',
          now,
          availableAt: now,
          attempts: 4,
        },
        {
          queue: 'email',
          name: 'email',
          data: '{"index":1}',
          now,
          availableAt: now + 25,
          attempts: 4,
        },
        {
          queue: 'email',
          name: 'email',
          data: '{"index":2}',
          now,
          availableAt: 0,
          attempts: 4,
        },
      ],
    ])
    expect(storage.enqueues).toEqual([])
  })

  it('returns an empty batch without reading the clock or calling storage', async () => {
    const clock = vi.spyOn(Date, 'now')
    const storage = new TestStorage()
    const queue = new Queue('email', { storage })

    await expect(queue.addMany([])).resolves.toEqual([])

    expect(clock).not.toHaveBeenCalled()
    expect(storage.enqueueManyCalls).toEqual([])
  })

  it('rejects invalid or unserializable batch items before any storage call', async () => {
    const storage = new TestStorage()
    const queue = new Queue<unknown>('email', { storage })
    const cyclic: { self?: unknown } = {}
    cyclic.self = cyclic

    await expect(queue.addMany(null as never)).rejects.toThrow(TypeError)
    await expect(queue.addMany([{} as never])).rejects.toThrow('JSON serializable')
    await expect(queue.addMany([{ data: {} }, null] as never)).rejects.toThrow(TypeError)
    await expect(queue.addMany(Array(1) as never)).rejects.toThrow(TypeError)
    await expect(queue.addMany([{ data: {} }, { data: cyclic }])).rejects.toThrow('circular')
    await expect(
      queue.addMany([{ data: {} }, { data: {}, options: { delay: 1, runAt: 1 } }]),
    ).rejects.toThrow('cannot be used together')
    await expect(queue.addMany([{ data: {} }, { data: {}, options: [] as never }])).rejects.toThrow(
      'add options',
    )

    expect(storage.enqueueManyCalls).toEqual([])
    expect(storage.enqueues).toEqual([])
  })

  it('accepts safe scheduling boundaries and rejects a later overflowing delay atomically', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(0)
    const storage = new TestStorage()
    const queue = new Queue('email', { storage })

    await queue.addMany([
      { data: {}, options: { delay: Number.MAX_SAFE_INTEGER } },
      { data: {}, options: { runAt: Number.MAX_SAFE_INTEGER } },
    ])
    expect(storage.enqueueManyCalls[0]!.map(({ availableAt }) => availableAt)).toEqual([
      Number.MAX_SAFE_INTEGER,
      Number.MAX_SAFE_INTEGER,
    ])

    storage.enqueueManyCalls.length = 0
    vi.spyOn(Date, 'now').mockReturnValue(Number.MAX_SAFE_INTEGER)
    await expect(
      queue.addMany([{ data: {} }, { data: {}, options: { delay: 1 } }]),
    ).rejects.toThrow('availableAt')
    expect(storage.enqueueManyCalls).toEqual([])
  })

  it('rejects invalid scheduling options before calling storage', async () => {
    const storage = new TestStorage()
    const queue = new Queue('email', { storage })
    const invalidOptions = [
      null,
      [],
      1,
      { delay: -1 },
      { delay: 1.5 },
      { delay: NaN },
      { delay: Infinity },
      { delay: Number.MAX_SAFE_INTEGER + 1 },
      { delay: '1' },
      { runAt: -1 },
      { runAt: 1.5 },
      { runAt: NaN },
      { runAt: Infinity },
      { runAt: Number.MAX_SAFE_INTEGER + 1 },
      { runAt: '1' },
      { delay: 0, runAt: 0 },
    ]

    for (const options of invalidOptions) {
      await expect(queue.add({}, options as never)).rejects.toThrow(TypeError)
    }

    expect(storage.enqueues).toHaveLength(0)
  })

  it('rejects a delay whose computed availability overflows', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(Number.MAX_SAFE_INTEGER)
    const storage = new TestStorage()
    const queue = new Queue('email', { storage })

    await expect(queue.add({}, { delay: 1 })).rejects.toThrow('availableAt')
    expect(storage.enqueues).toHaveLength(0)
  })

  it('uses queue-level attempts and rejects invalid configuration or data', async () => {
    const storage = new TestStorage()
    const queue = new Queue<unknown>('email', { storage, attempts: 3 })

    await queue.add(null)
    expect(storage.enqueues[0]!.attempts).toBe(3)
    expect(() => new Queue('email', { storage, attempts: 0 })).toThrow('attempts')
    expect(() => new Queue('', { storage })).toThrow('name')
    await expect(queue.add(undefined)).rejects.toThrow('JSON serializable')
  })

  it('claims only for free concurrency slots and completes successful jobs', async () => {
    const storage = new TestStorage()
    storage.jobs.push(claimedJob('1'), claimedJob('2'), claimedJob('3'))
    const gates = [deferred(), deferred(), deferred()]
    let active = 0
    let started = 0
    let maximumActive = 0
    const queue = new Queue('email', { storage })
    const worker = queue.process(
      async () => {
        const index = started
        started += 1
        active += 1
        maximumActive = Math.max(maximumActive, active)
        await gates[index]!.promise
        active -= 1
      },
      { concurrency: 2 },
    )

    await vi.waitFor(() => expect(active).toBe(2))
    expect(storage.claims[0]!.limit).toBe(2)
    gates[0]!.resolve()
    await vi.waitFor(() => expect(storage.completions).toHaveLength(1))
    await vi.waitFor(() => expect(storage.claims.some((claim) => claim.limit === 1)).toBe(true))
    gates[1]!.resolve()
    gates[2]!.resolve()
    await vi.waitFor(() => expect(storage.completions).toHaveLength(3))
    await worker.close()

    expect(maximumActive).toBe(2)
    expect(storage.completions.map(({ id }) => id).sort()).toEqual(['1', '2', '3'])
  })

  it('records handler errors for immediate retry', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {})
    vi.spyOn(Date, 'now').mockReturnValue(now)
    const storage = new TestStorage()
    storage.jobs.push(claimedJob('1', { data: '{"userId":"123"}' }))
    const queue = new Queue<{ userId: string }>('email', { storage })
    const worker = queue.process(async () => {
      throw new Error('send failed')
    })

    await vi.waitFor(() => expect(storage.failures).toHaveLength(1))
    await worker.close()
    expect(storage.failures[0]).toMatchObject({
      id: '1',
      leaseToken: 'lease-1',
      now,
      retryAt: now,
      error: expect.stringContaining('send failed'),
    })
  })

  it('applies downward jitter to fixed retry backoff', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(now)
    const random = vi.spyOn(Math, 'random').mockReturnValueOnce(0).mockReturnValueOnce(0.999)
    const storage = new TestStorage()
    storage.jobs.push(claimedJob('minimum'), claimedJob('jittered'))
    const queue = new Queue('email', {
      storage,
      attempts: 3,
      retry: { backoff: { type: 'fixed', delay: 1_000, jitter: 0.2 } },
      onError: () => {},
    })
    const worker = queue.process(
      async () => {
        throw new Error('send failed')
      },
      { concurrency: 2 },
    )

    await vi.waitFor(() => expect(storage.failures).toHaveLength(2))
    await worker.close()

    expect(storage.failures.map(({ id, retryAt }) => ({ id, retryAt }))).toEqual([
      { id: 'minimum', retryAt: now + 1_000 },
      { id: 'jittered', retryAt: now + 801 },
    ])
    expect(random).toHaveBeenCalledTimes(2)
  })

  it('uses full jitter over the base backoff when jitter is one', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(now)
    vi.spyOn(Math, 'random').mockReturnValueOnce(0).mockReturnValueOnce(0.9999)
    const storage = new TestStorage()
    storage.jobs.push(claimedJob('base'), claimedJob('minimum'))
    const queue = new Queue('email', {
      storage,
      attempts: 3,
      retry: { backoff: { type: 'fixed', delay: 1_000, jitter: 1 } },
      onError: () => {},
    })
    const worker = queue.process(
      async () => {
        throw new Error('send failed')
      },
      { concurrency: 2 },
    )

    await vi.waitFor(() => expect(storage.failures).toHaveLength(2))
    await worker.close()

    expect(storage.failures.map(({ id, retryAt }) => ({ id, retryAt }))).toEqual([
      { id: 'base', retryAt: now + 1_000 },
      { id: 'minimum', retryAt: now + 1 },
    ])
  })

  it('doubles exponential retry delay after each failed attempt', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(now)
    const random = vi.spyOn(Math, 'random')
    const storage = new TestStorage()
    storage.jobs.push(
      { ...claimedJob('first'), attemptsMade: 1, attempts: 3 },
      { ...claimedJob('second'), attemptsMade: 2, attempts: 3 },
    )
    const queue = new Queue('email', {
      storage,
      retry: { backoff: { type: 'exponential', delay: 250 } },
      onError: () => {},
    })
    const worker = queue.process(
      async () => {
        throw new Error('send failed')
      },
      { concurrency: 2 },
    )

    await vi.waitFor(() => expect(storage.failures).toHaveLength(2))
    await worker.close()

    expect(storage.failures.map(({ id, retryAt }) => ({ id, retryAt }))).toEqual([
      { id: 'first', retryAt: now + 250 },
      { id: 'second', retryAt: now + 500 },
    ])
    expect(random).not.toHaveBeenCalled()
  })

  it('caps overflowing retry timestamps and does not back off exhausted attempts', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {})
    const nearLimit = Number.MAX_SAFE_INTEGER - 3
    vi.spyOn(Date, 'now').mockReturnValue(nearLimit)
    const random = vi.spyOn(Math, 'random')
    const storage = new TestStorage()
    storage.jobs.push(
      { ...claimedJob('overflow'), attemptsMade: 2, attempts: 3 },
      { ...claimedJob('exhausted'), attemptsMade: 3, attempts: 3 },
    )
    const queue = new Queue('email', {
      storage,
      retry: { backoff: { type: 'exponential', delay: 10, jitter: 0.5 } },
      onError: () => {},
    })
    const worker = queue.process(
      async () => {
        throw new Error('send failed')
      },
      { concurrency: 2 },
    )

    await vi.waitFor(() => expect(storage.failures).toHaveLength(2))
    await worker.close()

    expect(storage.failures.map(({ id, retryAt }) => ({ id, retryAt }))).toEqual([
      { id: 'overflow', retryAt: Number.MAX_SAFE_INTEGER },
      { id: 'exhausted', retryAt: nearLimit },
    ])
    expect(random).not.toHaveBeenCalled()
  })

  it.each([
    null,
    'invalid',
    [],
    {},
    { backoff: null },
    { backoff: [] },
    { backoff: {} },
    { backoff: { type: 'linear', delay: 1 } },
    { backoff: { type: 'fixed', delay: -1 } },
    { backoff: { type: 'fixed', delay: 1.5 } },
    { backoff: { type: 'fixed', delay: Number.POSITIVE_INFINITY } },
    { backoff: { type: 'fixed', delay: Number.MAX_SAFE_INTEGER + 1 } },
    { backoff: { type: 'fixed', delay: 1, jitter: Number.NaN } },
    { backoff: { type: 'fixed', delay: 1, jitter: null } },
    { backoff: { type: 'fixed', delay: 1, jitter: '0.2' } },
    { backoff: { type: 'fixed', delay: 1, jitter: -0.01 } },
    { backoff: { type: 'fixed', delay: 1, jitter: 1.01 } },
  ])('rejects invalid retry configuration %j', (retry) => {
    const storage = new TestStorage()

    expect(() => new Queue('email', { storage, retry: retry as never })).toThrow('retry')
  })

  it('accepts zero delay and jitter boundary values', () => {
    const storage = new TestStorage()

    expect(
      () =>
        new Queue('email', {
          storage,
          retry: { backoff: { type: 'fixed', delay: 0, jitter: 1 } },
        }),
    ).not.toThrow()
  })

  it('polls immediately and then once per second while empty', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(now)
    const storage = new TestStorage()
    const queue = new Queue('email', { storage })
    const worker = queue.process(async () => {})

    await vi.advanceTimersByTimeAsync(0)
    expect(storage.claims).toHaveLength(1)
    await vi.advanceTimersByTimeAsync(999)
    await vi.advanceTimersByTimeAsync(0)
    expect(storage.claims).toHaveLength(1)
    await vi.advanceTimersByTimeAsync(1)
    await vi.advanceTimersByTimeAsync(0)
    expect(storage.claims).toHaveLength(2)
    await worker.close()
  })

  it('wakes the poller when a job is added', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(now)
    const storage = new TestStorage()
    const handled: string[] = []
    const queue = new Queue('email', { storage })
    const worker = queue.process(async (_data, context) => {
      handled.push(context.jobId)
    })

    await vi.advanceTimersByTimeAsync(0)
    expect(storage.claims).toHaveLength(1)
    storage.jobs.push(claimedJob('job-1'))
    await queue.add({ userId: '123' })
    await vi.advanceTimersByTimeAsync(0)

    expect(handled).toEqual(['job-1'])
    await worker.close()
  })

  it('does not poll an idle queue when another queue is woken', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(now)
    const storage = new TestStorage()
    const email = new Queue('email', { storage })
    const sms = new Queue('sms', { storage })
    const emailWorker = email.process(async () => {})
    const smsWorker = sms.process(async () => {})

    await vi.advanceTimersByTimeAsync(0)
    expect(storage.claims.filter((claim) => claim.queue === 'sms')).toHaveLength(1)

    storage.jobs.push(claimedJob('email-1'))
    await email.add({})
    await vi.advanceTimersByTimeAsync(0)
    await vi.waitFor(() => expect(storage.completions).toHaveLength(1))

    expect(storage.claims.filter((claim) => claim.queue === 'sms')).toHaveLength(1)
    await emailWorker.close()
    await smsWorker.close()
  })

  it('passes job context and heartbeats during graceful close', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(now)
    const storage = new TestStorage()
    storage.jobs.push(claimedJob('1'))
    const gate = deferred()
    let receivedContext: { signal: AbortSignal; jobId: string; attempt: number } | undefined
    const queue = new Queue('email', { storage })
    const worker = queue.process(async (_data, context) => {
      receivedContext = context
      await gate.promise
    })

    await vi.advanceTimersByTimeAsync(0)
    expect(receivedContext).toMatchObject({ jobId: '1', attempt: 1 })
    const closing = worker.close()
    await vi.advanceTimersByTimeAsync(10_000)
    await vi.advanceTimersByTimeAsync(0)
    expect(storage.heartbeats).toHaveLength(1)
    expect(receivedContext!.signal.aborted).toBe(false)
    let closed = false
    void closing.then(() => {
      closed = true
    })
    await vi.advanceTimersByTimeAsync(0)
    expect(closed).toBe(false)

    gate.resolve()
    await closing
    expect(storage.completions).toHaveLength(1)
  })

  it('aborts the job signal and ignores its result after lease loss', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(now)
    const storage = new TestStorage()
    storage.heartbeatResult = 'lease_lost'
    storage.jobs.push(claimedJob('1'))
    const gate = deferred()
    let signal: AbortSignal | undefined
    const queue = new Queue('email', { storage })
    const worker = queue.process(async (_data, context) => {
      signal = context.signal
      await gate.promise
    })

    await vi.advanceTimersByTimeAsync(0)
    await vi.advanceTimersByTimeAsync(10_000)
    await vi.advanceTimersByTimeAsync(0)
    expect(signal!.aborted).toBe(true)
    gate.resolve()
    await vi.advanceTimersByTimeAsync(0)
    await worker.close()

    expect(storage.completions).toEqual([])
    expect(storage.failures).toEqual([])
  })

  it('processes several queues through one coordinator', async () => {
    const storage = new TestStorage()
    storage.jobs.push(claimedJob('email-1'), claimedJob('sms-1', { queue: 'sms' }))
    const handled: string[] = []
    const email = new Queue('email', { storage })
    const sms = new Queue('sms', { storage })
    const emailWorker = email.process(async (_data, context) => {
      handled.push(context.jobId)
    })
    const smsWorker = sms.process(async (_data, context) => {
      handled.push(context.jobId)
    })

    await vi.waitFor(() => expect(handled).toHaveLength(2))
    await emailWorker.close()
    await smsWorker.close()

    expect(handled.sort()).toEqual(['email-1', 'sms-1'])
    expect(new Set(storage.claims.map((claim) => claim.queue))).toEqual(new Set(['email', 'sms']))
    expect(storage.maxConcurrentCalls).toBe(2)
  })

  it('keeps other queues polling after one worker closes', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(now)
    const storage = new TestStorage()
    const handled: string[] = []
    const email = new Queue('email', { storage })
    const sms = new Queue('sms', { storage })
    const emailWorker = email.process(async (_data, context) => {
      handled.push(context.jobId)
    })
    const smsWorker = sms.process(async (_data, context) => {
      handled.push(context.jobId)
    })

    await vi.advanceTimersByTimeAsync(0)
    await emailWorker.close()
    storage.jobs.push(claimedJob('sms-1', { queue: 'sms' }))
    await vi.advanceTimersByTimeAsync(1_000)
    await vi.advanceTimersByTimeAsync(0)

    expect(handled).toEqual(['sms-1'])
    await smsWorker.close()
  })

  it('allows processing again after close but rejects concurrent processors', async () => {
    const storage = new TestStorage()
    const queue = new Queue('email', { storage })
    const first = queue.process(async () => {})

    expect(() => queue.process(async () => {})).toThrow('already being processed')
    await first.close()
    const second = queue.process(async () => {})
    await second.close()
  })

  it('rejects a non-function onError', () => {
    const storage = new TestStorage()

    expect(
      () => new Queue('email', { storage, onError: 'log' as unknown as ProcessErrorHandler }),
    ).toThrow('onError must be a function')
  })

  it('reports claim errors to onError and keeps polling', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(now)
    const storage = new TestStorage()
    storage.claimErrors.push(new Error('claim failed'))
    const reports: ProcessErrorContext[] = []
    const queue = new Queue('email', {
      storage,
      onError: (err, ctx) => {
        reports.push(ctx)
      },
    })
    const worker = queue.process(async () => {})

    await vi.advanceTimersByTimeAsync(0)
    expect(reports).toEqual([{ queue: 'email', operation: 'claim' }])

    storage.jobs.push(claimedJob('1'))
    await queue.add({})
    await vi.advanceTimersByTimeAsync(0)
    expect(storage.completions).toHaveLength(1)

    await worker.close()
  })

  it('reports heartbeat errors without losing the lease', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(now)
    const storage = new TestStorage()
    storage.heartbeatErrors.push(new Error('heartbeat failed'))
    storage.jobs.push(claimedJob('1'))
    const reports: ProcessErrorContext[] = []
    const gate = deferred()
    const queue = new Queue('email', {
      storage,
      onError: (err, ctx) => {
        reports.push(ctx)
      },
    })
    const worker = queue.process(async () => {
      await gate.promise
    })

    await vi.advanceTimersByTimeAsync(0)
    await vi.advanceTimersByTimeAsync(10_000)
    await vi.advanceTimersByTimeAsync(0)
    expect(reports).toEqual([{ queue: 'email', operation: 'heartbeat', jobId: '1', attempt: 1 }])

    gate.resolve()
    await worker.close()
    expect(storage.completions).toHaveLength(1)
  })

  it('reports completion errors', async () => {
    const storage = new TestStorage()
    storage.completeErrors.push(new Error('complete failed'))
    storage.jobs.push(claimedJob('1'))
    const reports: ProcessErrorContext[] = []
    const queue = new Queue('email', {
      storage,
      onError: (err, ctx) => {
        reports.push(ctx)
      },
    })
    const worker = queue.process(async () => {})

    await vi.waitFor(() => expect(reports).toHaveLength(1))
    expect(reports[0]).toEqual({
      queue: 'email',
      operation: 'complete',
      jobId: '1',
      attempt: 1,
    })

    await worker.close()
    expect(storage.completions).toHaveLength(1)
  })

  it('reports handler and fail errors with attempt-budget state', async () => {
    const storage = new TestStorage()
    storage.jobs.push(
      { ...claimedJob('exhausted'), attemptsMade: 1, attempts: 1 },
      { ...claimedJob('retryable'), attemptsMade: 1, attempts: 2 },
    )
    storage.failErrors.push(new Error('fail failed'))
    const reports: ProcessErrorContext[] = []
    const queue = new Queue('email', {
      storage,
      onError: (err, ctx) => {
        reports.push(ctx)
      },
    })
    const worker = queue.process(async () => {
      throw new Error('send failed')
    })

    await vi.waitFor(() => expect(reports).toHaveLength(3))
    await worker.close()

    expect(reports).toEqual([
      {
        queue: 'email',
        operation: 'handler',
        jobId: 'exhausted',
        attempt: 1,
        attemptsExhausted: true,
      },
      { queue: 'email', operation: 'fail', jobId: 'exhausted', attempt: 1 },
      {
        queue: 'email',
        operation: 'handler',
        jobId: 'retryable',
        attempt: 1,
        attemptsExhausted: false,
      },
    ])
  })

  it('logs errors to console when onError is absent', async () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
    const storage = new TestStorage()
    storage.jobs.push(claimedJob('1'))
    const queue = new Queue('email', { storage })
    const worker = queue.process(async () => {
      throw new Error('send failed')
    })

    await vi.waitFor(() => expect(storage.failures).toHaveLength(1))
    await worker.close()

    const call = consoleError.mock.calls.find(
      ([, error]) => error instanceof Error && error.message === 'send failed',
    )
    expect(call?.[0]).toContain('queue "email"')
    expect(call?.[0]).toContain('handler')
    expect(call?.[2]).toMatchObject({ operation: 'handler', jobId: '1', attempt: 1 })
  })

  it('isolates thrown and rejected onError callbacks from queue execution', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {})
    const storage = new TestStorage()
    storage.claimErrors.push(new Error('claim failed'))
    let calls = 0
    const queue = new Queue('email', {
      storage,
      onError: () => {
        calls += 1
        if (calls === 1) throw new Error('sync callback failure')
        return Promise.reject(new Error('async callback failure'))
      },
    })
    const worker = queue.process(async (_data, context) => {
      if (context.jobId === '2') throw new Error('send failed')
    })

    await vi.waitFor(() => expect(calls).toBe(1))

    storage.jobs.push(claimedJob('2'))
    await queue.add({})
    await vi.waitFor(() => expect(calls).toBe(2))

    storage.jobs.push(claimedJob('3'))
    await queue.add({})
    await vi.waitFor(() => expect(storage.completions).toHaveLength(1))

    await worker.close()
    expect(storage.failures).toHaveLength(1)
    expect(storage.failures[0]!.id).toBe('2')
  })

  it('reports grouped claim failures per queue and keeps polling', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(now)
    const storage = new GroupedTestStorage()
    const reports: ProcessErrorContext[] = []
    const email = new Queue('email', {
      storage,
      onError: (err, ctx) => {
        reports.push(ctx)
      },
    })
    const sms = new Queue('sms', {
      storage,
      onError: (err, ctx) => {
        reports.push(ctx)
      },
    })
    const emailWorker = email.process(async () => {})
    const smsWorker = sms.process(async () => {})

    await vi.advanceTimersByTimeAsync(0)

    storage.groupedError = new Error('grouped claim failed')
    await Promise.all([email.add({}), sms.add({})])
    await vi.advanceTimersByTimeAsync(0)

    expect(reports.map((context) => context.queue).sort()).toEqual(['email', 'sms'])
    expect(reports.every((context) => context.operation === 'claim')).toBe(true)

    storage.jobs.push(
      claimedJob('email-1', { queue: 'email' }),
      claimedJob('sms-1', { queue: 'sms' }),
    )
    await vi.advanceTimersByTimeAsync(1_000)
    await vi.advanceTimersByTimeAsync(0)

    expect(storage.completions.map(({ id }) => id).sort()).toEqual(['email-1', 'sms-1'])

    await emailWorker.close()
    await smsWorker.close()
  })
})

describe('terminal-job retention', () => {
  it('defers the first pass and coalesces transitions within the interval', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(now)
    const storage = new CleanupTestStorage()
    const queue = new Queue('email', { storage })
    const worker = queue.process(async () => {})

    // The startup pass runs after process() returns rather than inside it.
    expect(storage.cleanups).toEqual([])
    await vi.advanceTimersByTimeAsync(1)
    expect(storage.cleanups).toEqual([
      {
        queue: 'email',
        retention: {
          completed: { count: 0, maxAge: null },
          failed: { count: 100, maxAge: null },
        },
        now,
        limit: expect.any(Number),
      },
    ])

    storage.jobs.push(claimedJob('1'), claimedJob('2'))
    await queue.add({})
    await vi.advanceTimersByTimeAsync(0)
    expect(storage.completions).toHaveLength(2)
    // Both completions are inside the throttle window of the startup pass.
    expect(storage.cleanups).toHaveLength(1)

    await vi.advanceTimersByTimeAsync(1_000)
    expect(storage.cleanups).toHaveLength(2)
    await worker.close()

    const afterClose = storage.cleanups.length
    await vi.advanceTimersByTimeAsync(5_000)
    expect(storage.cleanups).toHaveLength(afterClose)
  })

  it('passes queue retention to cleanup and skips disabled policies', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(now)
    const storage = new CleanupTestStorage()
    const queue = new Queue('email', { storage, retention: { completed: 3, failed: null } })
    const worker = queue.process(async () => {})

    await vi.advanceTimersByTimeAsync(1)
    expect(storage.cleanups[0]!.retention).toEqual({
      completed: { count: 3, maxAge: null },
      failed: { count: null, maxAge: null },
    })
    expect(storage.cleanups[0]!.now).toBe(now)
    await worker.close()

    const disabled = new CleanupTestStorage()
    const second = new Queue('email', {
      storage: disabled,
      retention: { completed: null, failed: null },
    })
    const secondWorker = second.process(async () => {})
    await vi.advanceTimersByTimeAsync(1)
    expect(disabled.cleanups).toEqual([])
    await secondWorker.close()
  })

  it('normalizes rule objects with defaults and age bounds', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(now)
    const storage = new CleanupTestStorage()
    const queue = new Queue('email', {
      storage,
      retention: { completed: { maxAge: 500 }, failed: { count: 5, maxAge: 1_000 } },
    })
    const worker = queue.process(async () => {})

    await vi.advanceTimersByTimeAsync(1)
    // An omitted count falls back to the status default and an omitted maxAge
    // disables the age bound.
    expect(storage.cleanups[0]!.retention).toEqual({
      completed: { count: 0, maxAge: 500 },
      failed: { count: 5, maxAge: 1_000 },
    })
    await worker.close()
  })

  it('keeps running cleanup for an age-only policy', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(now)
    const storage = new CleanupTestStorage()
    const queue = new Queue('email', {
      storage,
      retention: { completed: { count: null, maxAge: 1_000 }, failed: null },
    })
    const worker = queue.process(async () => {})

    await vi.advanceTimersByTimeAsync(1)
    expect(storage.cleanups).toHaveLength(1)
    await worker.close()
  })

  it('disables cleanup when both bounds are null in rule objects', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(now)
    const storage = new CleanupTestStorage()
    const queue = new Queue('email', {
      storage,
      retention: {
        completed: { count: null, maxAge: null },
        failed: { count: null, maxAge: null },
      },
    })
    const worker = queue.process(async () => {})

    await vi.advanceTimersByTimeAsync(1)
    expect(storage.cleanups).toEqual([])
    await worker.close()
  })

  it('cleans after claim passes that may have recovered expired jobs', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(now)
    const storage = new CleanupTestStorage()
    const queue = new Queue('email', { storage, retention: { completed: 0, failed: 0 } })
    const worker = queue.process(async () => {})

    await vi.advanceTimersByTimeAsync(1)
    expect(storage.cleanups).toHaveLength(1)

    // An idle claim can still recover an expired job into a terminal row even
    // though it returns no jobs and no handler completes or fails.
    storage.claims.length = 0
    await vi.advanceTimersByTimeAsync(1_000)
    expect(storage.claims.length).toBeGreaterThan(0)
    expect(storage.completions).toEqual([])
    expect(storage.cleanups).toHaveLength(2)
    await worker.close()
  })

  it('reports cleanup failures and retries on the next claim pass', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(now)
    const storage = new CleanupTestStorage()
    const reports: ProcessErrorContext[] = []
    const queue = new Queue('email', {
      storage,
      onError: (_error, context) => {
        reports.push(context)
      },
    })
    const worker = queue.process(async () => {})
    await vi.advanceTimersByTimeAsync(1)
    expect(storage.cleanups).toHaveLength(1)

    storage.cleanupErrors.push(new Error('cleanup failed'))
    storage.jobs.push(claimedJob('1'))
    await queue.add({})
    await vi.advanceTimersByTimeAsync(1_000)
    expect(reports).toEqual([{ queue: 'email', operation: 'cleanup' }])
    expect(storage.cleanups).toHaveLength(2)

    storage.jobs.push(claimedJob('2'))
    await queue.add({})
    await vi.advanceTimersByTimeAsync(0)
    expect(storage.completions).toHaveLength(2)
    await vi.advanceTimersByTimeAsync(1_000)
    expect(storage.cleanups).toHaveLength(3)
    expect(reports).toHaveLength(1)
    await worker.close()
  })

  it('drains bounded batches while cleanup reports remaining work', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(now)
    const storage = new CleanupTestStorage()
    const queue = new Queue('email', { storage })
    const worker = queue.process(async () => {})
    await vi.advanceTimersByTimeAsync(1)
    expect(storage.cleanups).toHaveLength(1)

    storage.cleanupResults.push(
      { removed: 2, more: true },
      { removed: 2, more: true },
      { removed: 1, more: false },
    )
    storage.jobs.push(claimedJob('1'))
    await queue.add({})
    await vi.advanceTimersByTimeAsync(1_000)
    // Each drain batch yields through its own zero-delay timer.
    for (let turn = 0; turn < 5; turn += 1) await vi.advanceTimersByTimeAsync(1)
    expect(storage.cleanups).toHaveLength(4)

    await worker.close()
    const afterClose = storage.cleanups.length
    await vi.advanceTimersByTimeAsync(5_000)
    expect(storage.cleanups).toHaveLength(afterClose)
  })

  it('waits for an in-flight cleanup pass during close', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(now)
    const storage = new CleanupTestStorage()
    const gate = deferred()
    storage.cleanupGate = gate.promise
    const queue = new Queue('email', { storage })
    const worker = queue.process(async () => {})
    await vi.advanceTimersByTimeAsync(1)
    expect(storage.cleanups).toHaveLength(1)

    let closed = false
    const closing = worker.close().then(() => {
      closed = true
    })
    await vi.advanceTimersByTimeAsync(1)
    expect(closed).toBe(false)

    gate.resolve()
    await closing
    expect(closed).toBe(true)
  })

  it('rejects a second worker for the same queue name on one storage', async () => {
    const storage = new TestStorage()
    const first = new Queue('email', { storage })
    const second = new Queue('email', { storage })
    const worker = first.process(async () => {})

    expect(() => second.process(async () => {})).toThrow('already being processed')
    await worker.close()

    // The queue can be processed again once the first worker closes.
    const replacement = second.process(async () => {})
    await replacement.close()
  })

  it('rejects invalid retention configuration', () => {
    const storage = new TestStorage()

    expect(() => new Queue('email', { storage, retention: { completed: -1 } })).toThrow(
      'retention.completed',
    )
    expect(() => new Queue('email', { storage, retention: { failed: 1.5 } })).toThrow(
      'retention.failed',
    )
    expect(() => new Queue('email', { storage, retention: 'all' as never })).toThrow(
      'retention must be an object',
    )
    expect(() => new Queue('email', { storage, retention: [] as never })).toThrow(
      'retention must be an object',
    )
    expect(() => new Queue('email', { storage, retention: { completed: { count: -1 } } })).toThrow(
      'retention.completed.count',
    )
    expect(() => new Queue('email', { storage, retention: { failed: { maxAge: 1.5 } } })).toThrow(
      'retention.failed.maxAge',
    )
    expect(
      () =>
        new Queue('email', {
          storage,
          retention: { completed: { maxAge: 'soon' as never } },
        }),
    ).toThrow('retention.completed.maxAge')
    expect(() => new Queue('email', { storage, retention: { completed: [] as never } })).toThrow(
      'retention.completed',
    )
    expect(() => new Queue('email', { storage, retention: { failed: true as never } })).toThrow(
      'retention.failed',
    )
  })
})
