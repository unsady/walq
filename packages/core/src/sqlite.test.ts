import { betterSqlite3 } from '@walq/better-sqlite3'
import Database from 'better-sqlite3'
import { afterEach, expect, it, vi } from 'vitest'

import { deferred } from './delay.js'
import { Queue } from './index.js'

const databases: Database.Database[] = []

function openStorage() {
  const db = new Database(':memory:')
  databases.push(db)
  return { db, storage: betterSqlite3(db) }
}

afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
  for (const db of databases.splice(0)) if (db.open) db.close()
})

it('exposes and mutates the job lifecycle through the public Queue API', async () => {
  vi.spyOn(Date, 'now').mockReturnValue(1_000)
  const { storage } = openStorage()
  const queue = new Queue<{ value: number }>('lifecycle', {
    storage,
    attempts: 2,
    retention: { completed: null, failed: null },
  })

  const cancelled = await queue.add({ value: 1 })
  expect(await queue.get(cancelled.id)).toMatchObject({
    id: cancelled.id,
    data: { value: 1 },
    status: 'pending',
    attempt: 0,
    attempts: 2,
    finishedAt: null,
    error: null,
  })
  expect(await queue.list({ status: 'pending' })).toHaveLength(1)
  expect(await queue.cancel(cancelled.id)).toBe(true)
  expect(await queue.get(cancelled.id)).toMatchObject({ status: 'cancelled', finishedAt: 1_000 })
  expect(await queue.list({ status: 'cancelled' })).toHaveLength(1)

  const rescheduled = await queue.add({ value: 2 })
  expect(await queue.reschedule(rescheduled.id, { runAt: 1_025 })).toBe(true)
  expect(await queue.get(rescheduled.id)).toMatchObject({
    status: 'pending',
    availableAt: 1_025,
    finishedAt: null,
  })
  expect(await queue.remove(rescheduled.id)).toBe(true)

  const retried = await queue.add({ value: 3 })
  const [firstClaim] = await storage.claim({
    queue: 'lifecycle',
    limit: 1,
    now: 1_000,
    leaseDuration: 30_000,
  })
  expect(firstClaim?.id).toBe(retried.id)
  await storage.fail({
    id: firstClaim!.id,
    leaseToken: firstClaim!.leaseToken,
    now: 1_000,
    error: 'temporary failure',
    retryAt: null,
  })
  expect(await queue.get(retried.id)).toMatchObject({
    status: 'failed',
    attempt: 1,
    error: 'temporary failure',
    finishedAt: 1_000,
  })

  expect(await queue.retry(retried.id)).toBe(true)
  expect(await queue.get(retried.id)).toMatchObject({
    status: 'pending',
    attempt: 1,
    error: 'temporary failure',
    finishedAt: null,
  })
  const [secondClaim] = await storage.claim({
    queue: 'lifecycle',
    limit: 1,
    now: 1_000,
    leaseDuration: 30_000,
  })
  expect(secondClaim?.id).toBe(retried.id)
  await storage.complete({ id: retried.id, leaseToken: secondClaim!.leaseToken, now: 1_001 })
  expect(await queue.get(retried.id)).toMatchObject({
    status: 'completed',
    attempt: 2,
    finishedAt: 1_001,
  })
  expect(await queue.remove(retried.id)).toBe(true)
  await expect(queue.get(retried.id)).resolves.toBeNull()
})

it('wakes a sleeping worker after a pending job is rescheduled', async () => {
  vi.useFakeTimers()
  vi.setSystemTime(10_000)
  const { storage } = openStorage()
  const queue = new Queue('wake', {
    storage,
    retention: { completed: null, failed: null },
  })
  const handled = deferred()
  async function processJob(): Promise<void> {
    handled.resolve()
  }
  const worker = queue.process(processJob)

  await vi.advanceTimersByTimeAsync(0)
  const added = await queue.add({}, { delay: 60_000 })
  await vi.advanceTimersByTimeAsync(0)
  expect(await queue.reschedule(added.id, { runAt: Date.now() })).toBe(true)
  await vi.advanceTimersByTimeAsync(0)
  await expect(handled.promise).resolves.toBeUndefined()
  await worker.close()
})

it('processes a job through the SQLite storage adapter', async () => {
  const { storage } = openStorage()
  const queue = new Queue<{ userId: string }>('email', { storage })
  let resolveHandled: (userId: string) => void
  const handled = new Promise<string>((resolve) => {
    resolveHandled = resolve
  })
  const worker = queue.process(async ({ userId }) => {
    resolveHandled(userId)
  })

  const added = await queue.add({ userId: '123' })
  await expect(handled).resolves.toBe('123')
  await worker.close()

  expect(added.id).toEqual(expect.any(String))
  expect(
    await storage.claim({ queue: 'email', limit: 1, now: Date.now(), leaseDuration: 30_000 }),
  ).toEqual([])
})

it('adds scheduled jobs as one ordered SQLite batch', async () => {
  vi.spyOn(Date, 'now').mockReturnValue(1_000)
  const { db, storage } = openStorage()
  const queue = new Queue<{ userId: string }>('email', { storage })

  const added = await queue.addMany([
    { data: { userId: 'first' } },
    { data: { userId: 'second' }, options: { delay: 25 } },
  ])

  const rows = db
    .prepare('SELECT id, data, createdAt, availableAt FROM walq_jobs ORDER BY rowid')
    .all()
  expect(added.map(({ id }) => id)).toEqual(rows.map((row) => (row as { id: string }).id))
  expect(rows).toEqual([
    { id: added[0]!.id, data: '{"userId":"first"}', createdAt: 1_000, availableAt: 1_000 },
    { id: added[1]!.id, data: '{"userId":"second"}', createdAt: 1_000, availableAt: 1_025 },
  ])
})

it('immediately retries failed handlers while attempts remain', async () => {
  vi.spyOn(console, 'error').mockImplementation(() => {})
  const { storage } = openStorage()
  const queue = new Queue('email', { storage, attempts: 2 })
  let calls = 0
  let resolveHandled: () => void
  const handled = new Promise<void>((resolve) => {
    resolveHandled = resolve
  })
  const worker = queue.process(async () => {
    calls += 1
    if (calls === 1) throw new Error('temporary')
    resolveHandled()
  })

  await queue.add({ userId: '123' })
  await handled
  await worker.close()

  expect(calls).toBe(2)
})

it('applies terminal-job retention through the queue defaults', async () => {
  vi.spyOn(console, 'error').mockImplementation(() => {})
  const { db, storage } = openStorage()
  const queue = new Queue('email', { storage, retention: { completed: 0, failed: 1 } })
  let handled = 0
  const worker = queue.process(async () => {
    handled += 1
    if (handled <= 2) throw new Error('send failed')
  })

  await queue.add({ n: 1 })
  await queue.add({ n: 2 })
  await queue.add({ n: 3 })

  await vi.waitFor(
    () => {
      expect(
        db.prepare('SELECT status, count(*) AS count FROM walq_jobs GROUP BY status').all(),
      ).toEqual([{ status: 'failed', count: 1 }])
    },
    { timeout: 5_000 },
  )

  await worker.close()
})

it('removes terminal rows created by lease recovery', async () => {
  vi.useFakeTimers()
  const start = 1_000_000
  vi.setSystemTime(start)
  const { db, storage } = openStorage()
  let handled = 0
  const queue = new Queue('email', { storage, retention: { completed: 0, failed: 0 } })
  const worker = queue.process(async () => {
    handled += 1
  })

  // Let the startup pass finish before the stale lease exists.
  await vi.advanceTimersByTimeAsync(1)

  const stale = start - 60_000
  await storage.enqueue({
    queue: 'email',
    name: 'email',
    data: '{}',
    now: stale,
    availableAt: stale,
    attempts: 1,
  })
  // Leave an expired lease with an exhausted attempt budget behind. The next
  // idle claim recovers it into a failed row without returning a job.
  await storage.claim({ queue: 'email', limit: 1, now: stale, leaseDuration: 1 })
  expect(
    db.prepare("SELECT count(*) AS count FROM walq_jobs WHERE status = 'failed'").get(),
  ).toEqual({ count: 0 })

  await vi.advanceTimersByTimeAsync(1_000)
  await vi.advanceTimersByTimeAsync(1)
  expect(db.prepare('SELECT count(*) AS count FROM walq_jobs').get()).toEqual({ count: 0 })
  expect(handled).toBe(0)
  await worker.close()
})
