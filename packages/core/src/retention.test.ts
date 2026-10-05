import { afterEach, describe, expect, it, vi } from 'vitest'

import { deferred } from './delay.js'
import { CleanupTestStorage, TestStorage, claimedJob, now } from './fixtures/storage.js'
import { Queue, type ProcessErrorContext } from './index.js'

afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
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
  })
})
