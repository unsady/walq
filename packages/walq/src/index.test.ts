import type {
  ClaimedJob,
  ClaimInput,
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
import { Queue } from './index.js'

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
    const claimed = this.jobs.filter((job) => job.queue === input.queue).slice(0, input.limit)
    this.jobs = this.jobs.filter((job) => !claimed.includes(job))
    return this.#leave(claimed)
  }

  async complete(input: CompleteInput): Promise<LeaseMutationResult> {
    this.#enter()
    this.completions.push(input)
    return this.#leave('applied')
  }

  async fail(input: FailInput): Promise<LeaseMutationResult> {
    this.#enter()
    this.failures.push(input)
    return this.#leave('applied')
  }

  async heartbeat(input: HeartbeatInput): Promise<LeaseMutationResult> {
    this.#enter()
    this.heartbeats.push(input)
    return this.#leave(this.heartbeatResult)
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
    expect(storage.maxConcurrentCalls).toBe(1)
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
})
