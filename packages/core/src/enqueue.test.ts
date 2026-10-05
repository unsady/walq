import { afterEach, describe, expect, it, vi } from 'vitest'

import { TestStorage, now } from './fixtures/storage.js'
import { Queue } from './index.js'

afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
})

describe('Queue enqueue API', () => {
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
        priority: 0,
        attempts: 1,
      },
    ])
  })

  it('accepts signed safe-integer priorities for single and batch enqueue', async () => {
    const storage = new TestStorage()
    const queue = new Queue('email', { storage })

    await queue.add({}, { priority: Number.MAX_SAFE_INTEGER })
    await queue.addMany([{ data: {}, options: { priority: Number.MIN_SAFE_INTEGER } }])

    expect(storage.enqueues[0]!.priority).toBe(Number.MAX_SAFE_INTEGER)
    expect(storage.enqueueManyCalls[0]![0]!.priority).toBe(Number.MIN_SAFE_INTEGER)
  })

  it('normalizes group shorthand and defaults concurrency in single and batch enqueue', async () => {
    const storage = new TestStorage()
    const queue = new Queue('email', { storage })

    await queue.add({}, { group: 'account:1' })
    await queue.addMany([
      { data: {}, options: { group: { id: 'account:2' } } },
      { data: {}, options: { group: { id: 'account:3', concurrency: 4 } } },
    ])

    expect(storage.enqueues[0]!.group).toEqual({ id: 'account:1', concurrency: 1 })
    expect(storage.enqueueManyCalls[0]!.map(({ group }) => group)).toEqual([
      { id: 'account:2', concurrency: 1 },
      { id: 'account:3', concurrency: 4 },
    ])
  })

  it.each([
    '',
    null,
    1,
    { id: '' },
    { id: 'account', concurrency: 0 },
    { id: 'account', concurrency: 1.5 },
    { id: 'account', concurrency: Number.MAX_SAFE_INTEGER + 1 },
  ])('rejects invalid group %o before storage access', async (group) => {
    const storage = new TestStorage()
    const queue = new Queue('email', { storage })

    await expect(queue.add({}, { group: group as never })).rejects.toThrow(TypeError)
    await expect(
      queue.addMany([{ data: {} }, { data: {}, options: { group: group as never } }]),
    ).rejects.toThrow(TypeError)

    expect(storage.enqueues).toEqual([])
    expect(storage.enqueueManyCalls).toEqual([])
  })

  it('forwards dedupe keys and returns duplicate IDs from single and batch enqueue', async () => {
    const storage = new TestStorage()
    const queue = new Queue('email', { storage })

    await expect(queue.add({ id: 1 }, { dedupe: 'user:123' })).resolves.toEqual({ id: 'job-1' })
    await expect(queue.add({ id: 2 }, { dedupe: 'user:123' })).resolves.toEqual({ id: 'job-1' })
    await expect(
      queue.addMany([
        { data: { id: 3 }, options: { dedupe: 'user:456' } },
        { data: { id: 4 }, options: { dedupe: 'user:456' } },
        { data: { id: 5 }, options: { dedupe: 'user:123' } },
      ]),
    ).resolves.toEqual([{ id: 'job-2' }, { id: 'job-2' }, { id: 'job-1' }])

    expect(storage.enqueues[0]).toMatchObject({ dedupe: 'user:123' })
    await expect(
      queue.addMany([
        { data: { id: 6 }, options: { dedupe: 'user:456' } },
        { data: { id: 7 }, options: { dedupe: 'user:123' } },
      ]),
    ).resolves.toEqual([{ id: 'job-2' }, { id: 'job-1' }])
    expect(storage.enqueueManyCalls[0]!.map(({ dedupe }) => dedupe)).toEqual([
      'user:456',
      'user:456',
      'user:123',
    ])
  })

  it.each(['', 1])('rejects invalid dedupe key %o before storage access', async (dedupe) => {
    const storage = new TestStorage()
    const queue = new Queue('email', { storage })

    await expect(queue.add({}, { dedupe: dedupe as never })).rejects.toThrow(TypeError)
    await expect(
      queue.addMany([{ data: {} }, { data: {}, options: { dedupe: dedupe as never } }]),
    ).rejects.toThrow(TypeError)

    expect(storage.enqueues).toEqual([])
    expect(storage.enqueueManyCalls).toEqual([])
  })

  it.each([null, 1.5, Number.MAX_SAFE_INTEGER + 1])(
    'rejects invalid priority %o before storage access',
    async (priority) => {
      const storage = new TestStorage()
      const queue = new Queue('email', { storage })

      await expect(queue.add({}, { priority: priority as never })).rejects.toThrow(TypeError)
      await expect(
        queue.addMany([{ data: {} }, { data: {}, options: { priority: priority as never } }]),
      ).rejects.toThrow(TypeError)
      expect(storage.enqueues).toEqual([])
      expect(storage.enqueueManyCalls).toEqual([])
    },
  )

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
          priority: 0,
          attempts: 4,
        },
        {
          queue: 'email',
          name: 'email',
          data: '{"index":1}',
          now,
          availableAt: now + 25,
          priority: 0,
          attempts: 4,
        },
        {
          queue: 'email',
          name: 'email',
          data: '{"index":2}',
          now,
          availableAt: 0,
          priority: 0,
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
      { delay: '1' },
      { runAt: -1 },
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
})
