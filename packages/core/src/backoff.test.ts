import { afterEach, describe, expect, it, vi } from 'vitest'

import { TestStorage, claimedJob, now } from './fixtures/storage.js'
import { Queue } from './index.js'

afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
})

describe('handler retries and backoff', () => {
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
      backoff: { type: 'fixed', delay: 1_000, jitter: 0.2 },
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
      backoff: { type: 'fixed', delay: 1_000, jitter: 1 },
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
      backoff: { type: 'exponential', delay: 250 },
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
      backoff: { type: 'exponential', delay: 10, jitter: 0.5 },
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
    { type: 'linear', delay: 1 },
    { type: 'fixed', delay: -1 },
    { type: 'fixed', delay: 1.5 },
    { type: 'fixed', delay: Number.POSITIVE_INFINITY },
    { type: 'fixed', delay: Number.MAX_SAFE_INTEGER + 1 },
    { type: 'fixed', delay: 1, jitter: Number.NaN },
    { type: 'fixed', delay: 1, jitter: null },
    { type: 'fixed', delay: 1, jitter: '0.2' },
    { type: 'fixed', delay: 1, jitter: -0.01 },
    { type: 'fixed', delay: 1, jitter: 1.01 },
  ])('rejects invalid backoff configuration %j', (backoff) => {
    const storage = new TestStorage()

    expect(() => new Queue('email', { storage, backoff: backoff as never })).toThrow('backoff')
  })

  it('accepts zero delay and jitter boundary values', () => {
    const storage = new TestStorage()

    expect(
      () =>
        new Queue('email', {
          storage,
          backoff: { type: 'fixed', delay: 0, jitter: 1 },
        }),
    ).not.toThrow()
  })
})
