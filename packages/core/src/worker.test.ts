import { afterEach, describe, expect, it, vi } from 'vitest'

import { deferred } from './delay.js'
import { GroupedTestStorage, TestStorage, claimedJob, now } from './fixtures/storage.js'
import {
  Queue,
  type ProcessErrorContext,
  type ProcessErrorHandler,
  type ProcessManyJob,
} from './index.js'

afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
})

describe('Queue worker lifecycle', () => {
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

  it('runs processMany handlers in bounded batches and completes every job', async () => {
    const storage = new TestStorage()
    storage.jobs.push(
      claimedJob('1', { data: '{"index":1}' }),
      claimedJob('2', { data: '{"index":2}' }),
      claimedJob('3', { data: '{"index":3}' }),
      claimedJob('4', { data: '{"index":4}' }),
    )
    const gates = [deferred(), deferred()]
    const batches: string[][] = []
    let active = 0
    let maximumActive = 0
    const queue = new Queue<{ index: number }>('email', { storage })
    const worker = queue.processMany(
      async (jobs) => {
        const index = batches.length
        batches.push(jobs.map(({ context }) => context.jobId))
        active += 1
        maximumActive = Math.max(maximumActive, active)
        await gates[index]!.promise
        active -= 1
      },
      { batch: 2, concurrency: 2 },
    )

    await vi.waitFor(() => expect(batches).toHaveLength(2))
    expect(storage.claims[0]!.limit).toBe(4)
    expect(batches).toEqual([
      ['1', '2'],
      ['3', '4'],
    ])
    gates[0]!.resolve()
    gates[1]!.resolve()
    await vi.waitFor(() => expect(storage.completions).toHaveLength(4))
    await worker.close()

    expect(maximumActive).toBe(2)
    expect(storage.completions.map(({ id }) => id).sort()).toEqual(['1', '2', '3', '4'])
  })

  it.each([
    null,
    [],
    { concurrency: null },
    { concurrency: 0 },
    { concurrency: 1.5 },
    { concurrency: Number.MAX_SAFE_INTEGER + 1 },
    { batch: 0 },
    { batch: 1.5 },
    { batch: { size: 2 } },
    { concurrency: 2, batch: Number.MAX_SAFE_INTEGER },
  ])('rejects invalid processMany options %o', (options) => {
    const queue = new Queue('email', { storage: new TestStorage() })

    expect(() => queue.processMany(async () => {}, options as never)).toThrow(TypeError)
  })

  it('fails and retries each job when a processMany batch handler rejects', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(now)
    const storage = new TestStorage()
    storage.jobs.push(claimedJob('1'), { ...claimedJob('2'), attemptsMade: 3 })
    const errors: ProcessErrorContext[] = []
    const queue = new Queue('email', {
      storage,
      attempts: 3,
      backoff: { type: 'fixed', delay: 250 },
      onError: (_error, context) => {
        errors.push(context)
      },
    })
    const worker = queue.processMany(async () => {
      throw new Error('batch failed')
    })

    await vi.waitFor(() => expect(storage.failures).toHaveLength(2))
    expect(storage.claims[0]!.limit).toBe(10)
    await worker.close()

    expect(storage.failures.map(({ id, retryAt, error }) => ({ id, retryAt, error }))).toEqual([
      { id: '1', retryAt: now + 250, error: expect.stringContaining('batch failed') },
      { id: '2', retryAt: now, error: expect.stringContaining('batch failed') },
    ])
    expect(errors).toEqual([
      expect.objectContaining({
        operation: 'handler',
        jobId: '1',
        attempt: 1,
        attemptsExhausted: false,
      }),
      expect.objectContaining({
        operation: 'handler',
        jobId: '2',
        attempt: 3,
        attemptsExhausted: true,
      }),
    ])
  })

  it('heartbeats and loses leases independently within a processMany batch', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(now)
    const storage = new TestStorage()
    storage.jobs.push(claimedJob('lost'), claimedJob('owned'))
    storage.heartbeatResults.set('lost', 'lease_lost')
    const gate = deferred()
    let received: ProcessManyJob<unknown>[] | undefined
    const queue = new Queue('email', { storage })
    const worker = queue.processMany(
      async (jobs) => {
        received = jobs
        await gate.promise
      },
      { batch: 2 },
    )

    await vi.advanceTimersByTimeAsync(0)
    expect(received).toHaveLength(2)
    await vi.advanceTimersByTimeAsync(10_000)
    expect(storage.heartbeats.map(({ id }) => id)).toEqual(['lost', 'owned'])
    expect(received?.map(({ context }) => context.signal.aborted)).toEqual([true, false])

    gate.resolve()
    await worker.close()

    expect(storage.completions.map(({ id }) => id)).toEqual(['owned'])
    expect(storage.failures).toHaveLength(0)
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

    const sameQueue = new Queue('email', { storage })
    expect(() => sameQueue.process(async () => {})).toThrow('already being processed')

    const independent = new Queue('email', { storage: new TestStorage() }).process(async () => {})
    await independent.close()
    await first.close()
    const second = sameQueue.process(async () => {})
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
