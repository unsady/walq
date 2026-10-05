import type {
  CancelInput,
  CountInput,
  InspectInput,
  JobSnapshot,
  ListInput,
  QueueStats,
  QueueInput,
  RemoveInput,
  RescheduleInput,
  RetryInput,
  ScheduleInput,
  StoredSchedule,
  Storage,
  UpsertScheduleInput,
} from '@walq/core/storage'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { Queue } from './index.js'

const now = 1_000

function snapshot(overrides: Partial<JobSnapshot> = {}): JobSnapshot {
  return {
    id: 'job-1',
    queue: 'email',
    name: 'email',
    data: '{"userId":"123"}',
    status: 'pending',
    createdAt: 100,
    availableAt: 200,
    priority: 0,
    attemptsMade: 2,
    attempts: 4,
    error: null,
    finishedAt: null,
    ...overrides,
  }
}

function storageMock() {
  const methods = {
    inspect: vi.fn<(input: InspectInput) => Promise<JobSnapshot | null>>(async (_input) => null),
    count: vi.fn<(input: CountInput) => Promise<QueueStats>>(async (_input) => ({
      pending: 0,
      active: 0,
      completed: 0,
      failed: 0,
      cancelled: 0,
    })),
    list: vi.fn<(input: ListInput) => Promise<JobSnapshot[]>>(async (_input) => []),
    retry: vi.fn<(input: RetryInput) => Promise<boolean>>(async (_input) => false),
    cancel: vi.fn<(input: CancelInput) => Promise<boolean>>(async (_input) => false),
    reschedule: vi.fn<(input: RescheduleInput) => Promise<boolean>>(async (_input) => false),
    remove: vi.fn<(input: RemoveInput) => Promise<boolean>>(async (_input) => false),
    pause: vi.fn<(input: QueueInput) => Promise<void>>(async (_input) => {}),
    resume: vi.fn<(input: QueueInput) => Promise<void>>(async (_input) => {}),
    upsertSchedule: vi.fn<(input: UpsertScheduleInput) => Promise<StoredSchedule>>(
      async (input) => ({
        queue: input.queue,
        id: input.id,
        data: input.data,
        nextRunAt: input.now + ('every' in input ? input.every : 1),
        ...('every' in input ? { every: input.every } : { cron: input.cron }),
      }),
    ),
    getSchedule: vi.fn<(input: ScheduleInput) => Promise<StoredSchedule | null>>(
      async (_input) => null,
    ),
    removeSchedule: vi.fn<(input: ScheduleInput) => Promise<boolean>>(async (_input) => false),
  }

  return { storage: methods as unknown as Storage, ...methods }
}

afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
})

describe('Queue public job API', () => {
  it('maps an inspected snapshot to public fields and scopes by the exact queue name', async () => {
    const methods = storageMock()
    methods.inspect.mockResolvedValue(
      snapshot({ status: 'failed', priority: -3, finishedAt: 900, error: 'send failed' }),
    )
    const queue = new Queue(' email ', { storage: methods.storage })

    await expect(queue.get('job-1')).resolves.toEqual({
      id: 'job-1',
      data: { userId: '123' },
      status: 'failed',
      attempt: 2,
      attempts: 4,
      createdAt: 100,
      availableAt: 200,
      priority: -3,
      finishedAt: 900,
      error: 'send failed',
    })
    expect(methods.inspect).toHaveBeenCalledExactlyOnceWith({ queue: ' email ', id: 'job-1' })
  })

  it('returns null when a job is not found', async () => {
    const methods = storageMock()
    const queue = new Queue('email', { storage: methods.storage })

    await expect(queue.get('missing')).resolves.toBeNull()
    expect(methods.inspect).toHaveBeenCalledExactlyOnceWith({ queue: 'email', id: 'missing' })
  })

  it('returns persisted status counts scoped to the exact queue name', async () => {
    const methods = storageMock()
    const stats = { pending: 2, active: 1, completed: 3, failed: 4, cancelled: 5 }
    methods.count.mockResolvedValue(stats)
    const queue = new Queue(' email ', { storage: methods.storage })

    await expect(queue.stats()).resolves.toEqual(stats)
    expect(methods.count).toHaveBeenCalledExactlyOnceWith({ queue: ' email ' })
  })

  it('lists parsed jobs with the default limit and forwards an explicit limit', async () => {
    const methods = storageMock()
    methods.list.mockResolvedValue([snapshot()])
    const queue = new Queue('email', { storage: methods.storage })

    await expect(queue.list({ status: 'pending' })).resolves.toEqual([
      {
        id: 'job-1',
        data: { userId: '123' },
        status: 'pending',
        attempt: 2,
        attempts: 4,
        createdAt: 100,
        availableAt: 200,
        priority: 0,
        finishedAt: null,
        error: null,
      },
    ])
    await queue.list({ status: 'cancelled', limit: 1_000 })

    expect(methods.list.mock.calls).toEqual([
      [{ queue: 'email', status: 'pending', limit: 100 }],
      [{ queue: 'email', status: 'cancelled', limit: 1_000 }],
    ])
  })

  it.each([
    null,
    {},
    { status: 'waiting' },
    { status: 'pending', limit: 0 },
    { status: 'pending', limit: 1.5 },
    { status: 'pending', limit: 1_001 },
  ])('rejects invalid list options %o before storage access', async (options) => {
    const methods = storageMock()
    const queue = new Queue('email', { storage: methods.storage })

    await expect(queue.list(options as never)).rejects.toThrow(TypeError)
    expect(methods.list).not.toHaveBeenCalled()
  })

  it.each([null, '', 1])('rejects invalid job IDs %o before storage access', async (id) => {
    const methods = storageMock()
    const queue = new Queue('email', { storage: methods.storage })

    await expect(queue.get(id as never)).rejects.toThrow(TypeError)
    expect(methods.inspect).not.toHaveBeenCalled()
  })

  it('rejects invalid IDs for every mutation before storage access', async () => {
    const methods = storageMock()
    const queue = new Queue('email', { storage: methods.storage })

    await expect(queue.retry(null as never)).rejects.toThrow(TypeError)
    await expect(queue.cancel('')).rejects.toThrow(TypeError)
    await expect(queue.reschedule(1 as never, { delay: 0 })).rejects.toThrow(TypeError)
    await expect(queue.remove(undefined as never)).rejects.toThrow(TypeError)
    expect(methods.retry).not.toHaveBeenCalled()
    expect(methods.cancel).not.toHaveBeenCalled()
    expect(methods.reschedule).not.toHaveBeenCalled()
    expect(methods.remove).not.toHaveBeenCalled()
  })
})

describe('Queue durable schedules', () => {
  it('upserts queue-scoped interval and cron schedules and parses snapshots', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(now)
    const methods = storageMock()
    methods.getSchedule.mockResolvedValue({
      queue: 'email',
      id: 'daily',
      data: '{"task":"digest"}',
      cron: '0 9 * * *',
      nextRunAt: 2_000,
    })
    const queue = new Queue('email', { storage: methods.storage })

    await queue.schedule({ task: 'poll' }, { id: 'poller', every: 5_000 })
    await expect(queue.getSchedule('daily')).resolves.toEqual({
      id: 'daily',
      data: { task: 'digest' },
      cron: '0 9 * * *',
      nextRunAt: 2_000,
    })

    expect(methods.upsertSchedule).toHaveBeenCalledExactlyOnceWith({
      queue: 'email',
      id: 'poller',
      data: '{"task":"poll"}',
      now,
      every: 5_000,
    })
    expect(methods.getSchedule).toHaveBeenCalledExactlyOnceWith({ queue: 'email', id: 'daily' })
  })

  it('validates schedule IDs, repeat options, and serializable payloads before storage access', async () => {
    const methods = storageMock()
    const queue = new Queue('email', { storage: methods.storage })

    for (const options of [
      null,
      {},
      { id: '', every: 1 },
      { id: 'bad', every: 0 },
      { id: 'bad', every: 1.5 },
      { id: 'bad', cron: '0 0 99 * *' },
      { id: 'bad', cron: '0 0 * * *', every: 1 },
    ]) {
      await expect(queue.schedule({ task: 'x' }, options as never)).rejects.toThrow(TypeError)
    }
    await expect(queue.schedule({}, { id: 'bad', cron: '0 0 99 * *' })).rejects.toMatchObject({
      cause: expect.any(Error),
    })
    await expect(queue.schedule(undefined as never, { id: 'bad', every: 1 })).rejects.toThrow(
      TypeError,
    )
    await expect(queue.getSchedule('')).rejects.toThrow(TypeError)
    await expect(queue.removeSchedule('')).rejects.toThrow(TypeError)
    expect(methods.upsertSchedule).not.toHaveBeenCalled()
    expect(methods.getSchedule).not.toHaveBeenCalled()
    expect(methods.removeSchedule).not.toHaveBeenCalled()
  })

  it('removes schedules without altering their already-created jobs', async () => {
    const methods = storageMock()
    methods.removeSchedule.mockResolvedValueOnce(true).mockResolvedValueOnce(false)
    const queue = new Queue('email', { storage: methods.storage })

    await expect(queue.removeSchedule('repeat')).resolves.toBe(true)
    await expect(queue.removeSchedule('missing')).resolves.toBe(false)

    expect(methods.removeSchedule.mock.calls).toEqual([
      [{ queue: 'email', id: 'repeat' }],
      [{ queue: 'email', id: 'missing' }],
    ])
  })
})

describe('Queue public mutations', () => {
  it('pauses and resumes the exact queue', async () => {
    const methods = storageMock()
    const queue = new Queue(' email ', { storage: methods.storage })

    await queue.pause()
    await queue.resume()

    expect(methods.pause).toHaveBeenCalledExactlyOnceWith({ queue: ' email ' })
    expect(methods.resume).toHaveBeenCalledExactlyOnceWith({ queue: ' email ' })
  })

  it('propagates resume failures', async () => {
    const methods = storageMock()
    const error = new Error('database unavailable')
    methods.resume.mockRejectedValue(error)
    const queue = new Queue('email', { storage: methods.storage })

    await expect(queue.resume()).rejects.toBe(error)
  })

  it('forwards retry, cancel, and remove with exact queue-scoped arguments', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(now)
    const methods = storageMock()
    methods.retry.mockResolvedValue(true)
    methods.cancel.mockResolvedValue(true)
    methods.remove.mockResolvedValue(true)
    const queue = new Queue('email', { storage: methods.storage })

    await expect(queue.retry('job-r')).resolves.toBe(true)
    await expect(queue.cancel('job-c')).resolves.toBe(true)
    await expect(queue.remove('job-d')).resolves.toBe(true)

    expect(methods.retry).toHaveBeenCalledExactlyOnceWith({ queue: 'email', id: 'job-r', now })
    expect(methods.cancel).toHaveBeenCalledExactlyOnceWith({ queue: 'email', id: 'job-c', now })
    expect(methods.remove).toHaveBeenCalledExactlyOnceWith({ queue: 'email', id: 'job-d' })
  })

  it('reschedules from a delay or absolute run time', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(now)
    const methods = storageMock()
    methods.reschedule.mockResolvedValue(true)
    const queue = new Queue('email', { storage: methods.storage })

    await expect(queue.reschedule('job-delay', { delay: 25 })).resolves.toBe(true)
    await expect(queue.reschedule('job-run-at', { runAt: 0 })).resolves.toBe(true)

    expect(methods.reschedule.mock.calls).toEqual([
      [{ queue: 'email', id: 'job-delay', availableAt: 1_025 }],
      [{ queue: 'email', id: 'job-run-at', availableAt: 0 }],
    ])
  })

  it.each([null, {}, { delay: 0, runAt: 0 }, { delay: -1 }, { delay: 1.5 }, { runAt: -1 }])(
    'rejects invalid reschedule options %o before storage access',
    async (options) => {
      vi.spyOn(Date, 'now').mockReturnValue(now)
      const methods = storageMock()
      const queue = new Queue('email', { storage: methods.storage })

      await expect(queue.reschedule('job-1', options as never)).rejects.toThrow(TypeError)
      expect(methods.reschedule).not.toHaveBeenCalled()
    },
  )

  it('rejects a delay whose calculated timestamp overflows before storage access', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(Number.MAX_SAFE_INTEGER - 10)
    const methods = storageMock()
    const queue = new Queue('email', { storage: methods.storage })

    await expect(queue.reschedule('job-1', { delay: 11 })).rejects.toThrow(TypeError)
    expect(methods.reschedule).not.toHaveBeenCalled()
  })

  it('returns storage results for retry and reschedule mutations', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(now)
    const methods = storageMock()
    methods.retry.mockResolvedValueOnce(true).mockResolvedValueOnce(false)
    methods.reschedule.mockResolvedValueOnce(false).mockResolvedValueOnce(true)
    const queue = new Queue('email', { storage: methods.storage })

    await expect(queue.retry('retry-success')).resolves.toBe(true)
    await expect(queue.retry('retry-failure')).resolves.toBe(false)
    await expect(queue.reschedule('reschedule-failure', { delay: 0 })).resolves.toBe(false)
    await expect(queue.reschedule('reschedule-success', { delay: 0 })).resolves.toBe(true)
  })

  it('propagates storage errors without translating them', async () => {
    const methods = storageMock()
    const error = new Error('database unavailable')
    methods.retry.mockRejectedValue(error)
    const queue = new Queue('email', { storage: methods.storage })

    await expect(queue.retry('job-1')).rejects.toBe(error)
  })
})
