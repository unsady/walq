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

  async enqueue(input: EnqueueInput): Promise<StoredJob> {
    this.#enter()
    this.enqueues.push(input)
    const job: StoredJob = {
      id: `job-${this.enqueues.length}`,
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

  async claim(input: ClaimInput): Promise<ClaimedJob[]> {
    this.#enter()
    this.claims.push(input)
    this.#maybeThrow(this.claimErrors)
    const claimed = this.jobs.filter((job) => job.queue === input.queue).slice(0, input.limit)
    this.jobs = this.jobs.filter((job) => !claimed.includes(job))
    return this.#leave(claimed)
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
