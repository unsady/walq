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

import { Queue } from './index.js'

const now = 1_000

function claimedJob(id: string, payload = '{}'): ClaimedJob {
  return {
    id,
    queue: 'email',
    name: 'email',
    payload,
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

function deferred(): { promise: Promise<void>; resolve(): void } {
  let resolvePromise: () => void
  const promise = new Promise<void>((resolve) => {
    resolvePromise = resolve
  })
  return { promise, resolve: () => resolvePromise() }
}

async function waitFor(assertion: () => void): Promise<void> {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    try {
      assertion()
      return
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 0))
    }
  }
  assertion()
}

class TestStorage implements Storage {
  readonly enqueues: EnqueueInput[] = []
  readonly claims: ClaimInput[] = []
  readonly completions: CompleteInput[] = []
  readonly failures: FailInput[] = []
  readonly heartbeats: HeartbeatInput[] = []
  readonly jobs: ClaimedJob[] = []

  async enqueue(input: EnqueueInput): Promise<StoredJob> {
    this.enqueues.push(input)
    return {
      id: `job-${this.enqueues.length}`,
      queue: input.queue,
      name: input.name,
      payload: input.payload,
      status: 'pending',
      createdAt: input.now,
      availableAt: input.availableAt,
      attemptsMade: 0,
      attempts: input.attempts,
      error: null,
    }
  }

  async claim(input: ClaimInput): Promise<ClaimedJob[]> {
    this.claims.push(input)
    return this.jobs.splice(0, input.limit)
  }

  async complete(input: CompleteInput): Promise<LeaseMutationResult> {
    this.completions.push(input)
    return 'applied'
  }

  async fail(input: FailInput): Promise<LeaseMutationResult> {
    this.failures.push(input)
    return 'applied'
  }

  async heartbeat(input: HeartbeatInput): Promise<LeaseMutationResult> {
    this.heartbeats.push(input)
    return 'applied'
  }
}

afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
})

describe('Queue', () => {
  it('adds serialized payloads with queue defaults', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(now)
    const storage = new TestStorage()
    const queue = new Queue<{ userId: string }>('email', { storage })

    await expect(queue.add({ userId: '123' })).resolves.toEqual({ id: 'job-1' })
    expect(storage.enqueues).toEqual([
      {
        queue: 'email',
        name: 'email',
        payload: '{"userId":"123"}',
        now,
        availableAt: now,
        attempts: 1,
      },
    ])
  })

  it('uses queue-level attempts and rejects invalid configuration or payloads', async () => {
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

    await waitFor(() => expect(active).toBe(2))
    expect(storage.claims[0]!.limit).toBe(2)
    gates[0]!.resolve()
    await waitFor(() => expect(storage.completions).toHaveLength(1))
    await waitFor(() => expect(storage.claims.some((claim) => claim.limit === 1)).toBe(true))
    gates[1]!.resolve()
    gates[2]!.resolve()
    await waitFor(() => expect(storage.completions).toHaveLength(3))
    await worker.close()

    expect(maximumActive).toBe(2)
    expect(storage.completions.map(({ id }) => id).sort()).toEqual(['1', '2', '3'])
  })

  it('records handler errors for immediate retry', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(now)
    const storage = new TestStorage()
    storage.jobs.push(claimedJob('1', '{"userId":"123"}'))
    const queue = new Queue<{ userId: string }>('email', { storage })
    const worker = queue.process(async () => {
      throw new Error('send failed')
    })

    await waitFor(() => expect(storage.failures).toHaveLength(1))
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
    expect(storage.claims).toHaveLength(1)
    await vi.advanceTimersByTimeAsync(1)
    expect(storage.claims).toHaveLength(2)
    await worker.close()
  })

  it('heartbeats long-running jobs and waits for them during close', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(now)
    const storage = new TestStorage()
    storage.jobs.push(claimedJob('1'))
    const gate = deferred()
    const queue = new Queue('email', { storage })
    const worker = queue.process(async () => gate.promise)

    await vi.advanceTimersByTimeAsync(0)
    const closing = worker.close()
    await vi.advanceTimersByTimeAsync(10_000)
    expect(storage.heartbeats).toHaveLength(1)
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
