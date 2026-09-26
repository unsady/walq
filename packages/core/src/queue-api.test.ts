import type {
  CancelInput,
  InspectInput,
  JobSnapshot,
  ListInput,
  RemoveInput,
  RescheduleInput,
  RetryInput,
  Storage,
} from '@walq/core/storage'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { StorageCoordinator } from './coordinator.js'
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
    list: vi.fn<(input: ListInput) => Promise<JobSnapshot[]>>(async (_input) => []),
    retry: vi.fn<(input: RetryInput) => Promise<boolean>>(async (_input) => false),
    cancel: vi.fn<(input: CancelInput) => Promise<boolean>>(async (_input) => false),
    reschedule: vi.fn<(input: RescheduleInput) => Promise<boolean>>(async (_input) => false),
    remove: vi.fn<(input: RemoveInput) => Promise<boolean>>(async (_input) => false),
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
      snapshot({ status: 'failed', finishedAt: 900, error: 'send failed' }),
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
    undefined,
    [],
    {},
    { status: null },
    { status: 'waiting' },
    { status: 'pending', limit: null },
    { status: 'pending', limit: 0 },
    { status: 'pending', limit: -1 },
    { status: 'pending', limit: 1.5 },
    { status: 'pending', limit: Number.MAX_SAFE_INTEGER + 1 },
    { status: 'pending', limit: 1_001 },
  ])('rejects invalid list options %o before storage access', async (options) => {
    const methods = storageMock()
    const queue = new Queue('email', { storage: methods.storage })

    await expect(queue.list(options as never)).rejects.toThrow(TypeError)
    expect(methods.list).not.toHaveBeenCalled()
  })

  it.each([null, undefined, '', 1, {}])(
    'rejects invalid job IDs %o before storage access',
    async (id) => {
      const methods = storageMock()
      const queue = new Queue('email', { storage: methods.storage })

      await expect(queue.get(id as never)).rejects.toThrow(TypeError)
      expect(methods.inspect).not.toHaveBeenCalled()
    },
  )

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

describe('Queue public mutations', () => {
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

  it.each([
    null,
    undefined,
    [],
    {},
    { delay: undefined },
    { runAt: undefined },
    { delay: 0, runAt: 0 },
    { delay: null },
    { delay: -1 },
    { delay: 1.5 },
    { delay: Number.MAX_SAFE_INTEGER + 1 },
    { runAt: null },
    { runAt: -1 },
    { runAt: 1.5 },
    { runAt: Number.MAX_SAFE_INTEGER + 1 },
  ])('rejects invalid reschedule options %o before storage access', async (options) => {
    vi.spyOn(Date, 'now').mockReturnValue(now)
    const methods = storageMock()
    const queue = new Queue('email', { storage: methods.storage })

    await expect(queue.reschedule('job-1', options as never)).rejects.toThrow(TypeError)
    expect(methods.reschedule).not.toHaveBeenCalled()
  })

  it('rejects a delay whose calculated timestamp overflows before storage access', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(Number.MAX_SAFE_INTEGER - 10)
    const methods = storageMock()
    const queue = new Queue('email', { storage: methods.storage })

    await expect(queue.reschedule('job-1', { delay: 11 })).rejects.toThrow(TypeError)
    expect(methods.reschedule).not.toHaveBeenCalled()
  })

  it('wakes the queue only after a successful retry or reschedule', async () => {
    vi.spyOn(Date, 'now').mockReturnValue(now)
    const methods = storageMock()
    methods.retry.mockResolvedValueOnce(true).mockResolvedValueOnce(false)
    methods.reschedule.mockResolvedValueOnce(false).mockResolvedValueOnce(true)
    const wakeQueue = vi.spyOn(StorageCoordinator.prototype, 'wakeQueue')
    const queue = new Queue('email', { storage: methods.storage })

    await queue.retry('retry-success')
    await queue.retry('retry-failure')
    await queue.reschedule('reschedule-failure', { delay: 0 })
    await queue.reschedule('reschedule-success', { delay: 0 })

    expect(wakeQueue.mock.calls).toEqual([['email'], ['email']])
  })

  it('propagates storage errors without translating them', async () => {
    const methods = storageMock()
    const error = new Error('database unavailable')
    methods.retry.mockRejectedValue(error)
    const queue = new Queue('email', { storage: methods.storage })

    await expect(queue.retry('job-1')).rejects.toBe(error)
  })
})
