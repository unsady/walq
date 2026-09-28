import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Worker } from 'node:worker_threads'

import type { ClaimedJob, LeaseMutationResult, StoredJob } from '@walq/core/storage'
import Database from 'better-sqlite3'
import { afterEach, describe, expect, it } from 'vitest'

import { betterSqlite3 } from './index.js'

const databases: Database.Database[] = []
const directories: string[] = []
const input = {
  queue: 'email',
  name: 'send',
  data: '{"to":"a"}',
  now: 10,
  availableAt: 10,
  priority: 0,
  attempts: 2,
}
const claimInput = { queue: 'email', now: 10, limit: 10, leaseDuration: 20 }

function open(filename = ':memory:') {
  const db = new Database(filename)
  databases.push(db)
  return { db, storage: betterSqlite3(db) }
}

function filename() {
  const directory = mkdtempSync(join(tmpdir(), 'walq-'))
  directories.push(directory)
  return join(directory, 'queue.sqlite')
}

function expectPartialDedupeIndex(db: Database.Database): void {
  const index = db
    .prepare("SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'walq_dedupe'")
    .get() as { sql: string } | undefined
  expect(index?.sql).toMatch(/WHERE dedupe IS NOT NULL$/)
}

const raceTimeout = 4000

async function race(path: string, operations: { method: string; input: object }[]) {
  const gate = new SharedArrayBuffer(4)
  const workers: Worker[] = []
  const timers: NodeJS.Timeout[] = []
  try {
    const tasks = operations.map(
      (operation) =>
        new Promise<ClaimedJob[] | StoredJob | LeaseMutationResult | boolean | number>(
          (resolve, reject) => {
            let settled = false
            const timer = setTimeout(() => {
              settled = true
              reject(new Error(`Timed out waiting for worker ${operation.method}`))
            }, raceTimeout)
            timers.push(timer)
            const worker = new Worker(new URL('./fixtures/claim-worker.ts', import.meta.url), {
              workerData: { path, gate, count: operations.length, ...operation },
            })
            workers.push(worker)
            worker.on(
              'message',
              (
                message:
                  | {
                      ready: true
                    }
                  | { result: ClaimedJob[] | StoredJob | LeaseMutationResult | boolean | number },
              ) => {
                if (!('result' in message)) {
                  Atomics.add(new Int32Array(gate), 0, 1)
                  if (Atomics.load(new Int32Array(gate), 0) === operations.length)
                    Atomics.notify(new Int32Array(gate), 0)
                } else if (!settled) {
                  settled = true
                  clearTimeout(timer)
                  resolve(message.result)
                }
              },
            )
            worker.on('error', (error) => {
              if (!settled) {
                settled = true
                clearTimeout(timer)
                reject(error)
              }
            })
            worker.on('exit', (code) => {
              if (!settled) {
                settled = true
                clearTimeout(timer)
                reject(new Error(`Worker exited before result: ${code}`))
              }
            })
          },
        ),
    )
    return await Promise.all(tasks)
  } finally {
    for (const timer of timers) clearTimeout(timer)
    await Promise.all(workers.map((worker) => worker.terminate()))
  }
}

afterEach(() => {
  for (const db of databases.splice(0)) if (db.open) db.close()
  for (const directory of directories.splice(0)) rmSync(directory, { recursive: true, force: true })
})

describe('SQLite job columns', () => {
  it('stores terminal failure and preserves the last error on completion', async () => {
    const { db, storage } = open()
    await storage.enqueue(input)
    let [job] = await storage.claim(claimInput)
    await storage.fail({ ...job!, now: 11, error: 'previous', retryAt: 0 })
    ;[job] = await storage.claim({ ...claimInput, now: 11 })
    await storage.complete({ ...job!, now: 12 })
    expect(db.prepare('SELECT status, error, leaseToken FROM walq_jobs').get()).toEqual({
      status: 'completed',
      error: 'previous',
      leaseToken: null,
    })

    await storage.enqueue(input)
    ;[job] = await storage.claim(claimInput)
    await storage.fail({ ...job!, now: 11, error: 'final', retryAt: null })
    expect(
      db.prepare("SELECT status, error, leaseToken FROM walq_jobs WHERE status = 'failed'").get(),
    ).toEqual({ status: 'failed', error: 'final', leaseToken: null })
  })

  it('recovers expired leases with expiry availability and per-queue failed counts', async () => {
    const { db, storage } = open()
    for (let index = 0; index < 3; index += 1) await storage.enqueue({ ...input, attempts: 1 })
    await storage.enqueue({ ...input, queue: 'other', attempts: 1 })
    await storage.claim(claimInput)
    await storage.claim({ ...claimInput, queue: 'other' })
    expect(await storage.claim({ ...claimInput, now: 30, limit: 1 })).toEqual([])
    expect(
      db.prepare("SELECT count(*) AS count FROM walq_jobs WHERE status = 'failed'").get(),
    ).toEqual({ count: 3 })
    expect(db.prepare("SELECT status FROM walq_jobs WHERE queue = 'other'").get()).toEqual({
      status: 'active',
    })
  })

  it('tracks heartbeat expiry in the row and leaves it unchanged on invalid inputs', async () => {
    const { db, storage } = open()
    await storage.enqueue(input)
    const [job] = await storage.claim(claimInput)
    expect(await storage.heartbeat({ ...job!, now: 11, leaseDuration: 1 })).toBe('applied')
    expect(db.prepare('SELECT expiresAt FROM walq_jobs').get()).toEqual({ expiresAt: 30 })

    await expect(
      storage.heartbeat({ ...job!, now: 11, leaseDuration: Number.MAX_SAFE_INTEGER }),
    ).rejects.toThrow('expiresAt')
    await expect(storage.complete({ ...job!, now: Number.NaN })).rejects.toThrow('now')
    expect(db.prepare('SELECT status, attemptsMade, expiresAt FROM walq_jobs').get()).toEqual({
      status: 'active',
      attemptsMade: 1,
      expiresAt: 30,
    })
  })
})

describe('SQLite retention', () => {
  const cleanupNow = 1_000_000
  function rule(count: number | null, maxAge: number | null = null) {
    return { count, maxAge }
  }
  const cleanupInput = {
    queue: 'email',
    retention: { completed: rule(0), failed: rule(0) },
    now: cleanupNow,
    limit: 10,
  }

  it('records finishedAt on terminal transitions and clears it on retry', async () => {
    const { db, storage } = open()
    await storage.enqueue({ ...input, attempts: 2 })
    let [job] = await storage.claim(claimInput)
    await storage.fail({ ...job!, now: 11, error: 'retry', retryAt: 11 })
    expect(db.prepare('SELECT status, finishedAt FROM walq_jobs').get()).toEqual({
      status: 'pending',
      finishedAt: null,
    })

    ;[job] = await storage.claim({ ...claimInput, now: 11 })
    await storage.complete({ ...job!, now: 12 })
    expect(db.prepare('SELECT status, finishedAt FROM walq_jobs').get()).toEqual({
      status: 'completed',
      finishedAt: 12,
    })

    await storage.enqueue({ ...input, attempts: 1 })
    ;[job] = await storage.claim(claimInput)
    await storage.fail({ ...job!, now: 13, error: 'terminal', retryAt: null })
    expect(
      db.prepare('SELECT status, finishedAt FROM walq_jobs WHERE id = ?').get(job!.id),
    ).toEqual({ status: 'failed', finishedAt: 13 })
  })

  it('stamps recovered terminal failures with the recovery time', async () => {
    const { db, storage } = open()
    await storage.enqueue({ ...input, attempts: 1 })
    await storage.claim(claimInput)
    await storage.claim({ ...claimInput, now: 30 })
    expect(db.prepare('SELECT status, finishedAt FROM walq_jobs').get()).toEqual({
      status: 'failed',
      finishedAt: 30,
    })
  })

  it('keeps the newest terminal rows per status and queue', async () => {
    const { db, storage } = open()
    const completed: string[] = []
    for (const now of [11, 12, 13, 14]) {
      const stored = await storage.enqueue({ ...input, now, availableAt: now, attempts: 1 })
      const [job] = await storage.claim({ ...claimInput, now })
      await storage.complete({ ...job!, now })
      completed.push(stored.id)
    }
    const failed: string[] = []
    for (const now of [21, 22]) {
      const stored = await storage.enqueue({ ...input, now, availableAt: now, attempts: 1 })
      const [job] = await storage.claim({ ...claimInput, now })
      await storage.fail({ ...job!, now, error: 'terminal', retryAt: null })
      failed.push(stored.id)
    }
    const other = await storage.enqueue({
      ...input,
      queue: 'other',
      now: 30,
      availableAt: 30,
      attempts: 1,
    })
    const [otherJob] = await storage.claim({ ...claimInput, queue: 'other', now: 30 })
    await storage.complete({ ...otherJob!, now: 30 })

    expect(
      await storage.cleanup({
        queue: 'email',
        retention: { completed: rule(2), failed: rule(1) },
        now: cleanupNow,
        limit: 10,
      }),
    ).toEqual({ removed: 3, more: false })

    const remaining = db.prepare('SELECT id FROM walq_jobs ORDER BY finishedAt').all() as {
      id: string
    }[]
    expect(remaining.map((row) => row.id)).toEqual([
      completed[2],
      completed[3],
      failed[1],
      other.id,
    ])
  })

  it('deletes eligible rows from the oldest end and reports remaining work', async () => {
    const { db, storage } = open()
    const completed: string[] = []
    for (const now of [11, 12, 13, 14]) {
      const stored = await storage.enqueue({ ...input, now, availableAt: now, attempts: 1 })
      const [job] = await storage.claim({ ...claimInput, now })
      await storage.complete({ ...job!, now })
      completed.push(stored.id)
    }

    function remaining(): unknown[] {
      return db.prepare('SELECT id FROM walq_jobs ORDER BY finishedAt').all()
    }
    const batch = {
      ...cleanupInput,
      retention: { completed: rule(2), failed: rule(0) },
      limit: 1,
    }

    // Only the two oldest rows are eligible; the oldest goes first.
    expect(await storage.cleanup(batch)).toEqual({ removed: 1, more: true })
    expect(remaining()).toEqual([{ id: completed[1] }, { id: completed[2] }, { id: completed[3] }])

    expect(await storage.cleanup(batch)).toEqual({ removed: 1, more: false })
    expect(remaining()).toEqual([{ id: completed[2] }, { id: completed[3] }])
    expect(await storage.cleanup(batch)).toEqual({ removed: 0, more: false })
    expect(remaining()).toEqual([{ id: completed[2] }, { id: completed[3] }])
  })

  it('exhausts the shared budget across statuses before reporting more', async () => {
    const { db, storage } = open()
    for (const now of [11, 12, 13]) {
      await storage.enqueue({ ...input, now, availableAt: now, attempts: 1 })
      const [job] = await storage.claim({ ...claimInput, now })
      await storage.complete({ ...job!, now })
    }
    const failed: string[] = []
    for (const now of [21, 22]) {
      const stored = await storage.enqueue({ ...input, now, availableAt: now, attempts: 1 })
      const [job] = await storage.claim({ ...claimInput, now })
      await storage.fail({ ...job!, now, error: 'terminal', retryAt: null })
      failed.push(stored.id)
    }
    const batch = {
      queue: 'email',
      retention: { completed: rule(0), failed: rule(1) },
      now: cleanupNow,
      limit: 1,
    }

    // Delete the oldest completed rows first; only the failed verdict remains.
    expect(await storage.cleanup(batch)).toEqual({ removed: 1, more: true })
    expect(await storage.cleanup(batch)).toEqual({ removed: 1, more: true })
    expect(await storage.cleanup(batch)).toEqual({ removed: 1, more: true })
    expect(await storage.cleanup(batch)).toEqual({ removed: 1, more: false })

    const remaining = db.prepare('SELECT id FROM walq_jobs').all() as { id: string }[]
    expect(remaining.map((row) => row.id)).toEqual([failed[1]])
  })

  it('breaks finish-time ties by id so the newest rows survive', async () => {
    const { db, storage } = open()
    const ids: string[] = []
    for (let index = 0; index < 4; index += 1) {
      const stored = await storage.enqueue({ ...input, now: 11, availableAt: 11, attempts: 1 })
      const [job] = await storage.claim({ ...claimInput, now: 11 })
      await storage.complete({ ...job!, now: 11 })
      ids.push(stored.id)
    }

    expect(
      await storage.cleanup({
        ...cleanupInput,
        retention: { completed: rule(2), failed: rule(0) },
      }),
    ).toEqual({ removed: 2, more: false })

    const remaining = db.prepare('SELECT id FROM walq_jobs').all() as { id: string }[]
    expect(remaining.map((row) => row.id).sort()).toEqual([...ids].sort().slice(-2))
  })

  it('removes only rows strictly older than the max-age cutoff', async () => {
    const { db, storage } = open()
    const seeded: string[] = []
    for (const finishedAt of [cleanupNow - 102, cleanupNow - 101, cleanupNow - 100]) {
      const stored = await storage.enqueue({
        ...input,
        now: finishedAt,
        availableAt: finishedAt,
        attempts: 1,
      })
      const [job] = await storage.claim({ ...claimInput, now: finishedAt })
      await storage.complete({ ...job!, now: finishedAt })
      seeded.push(stored.id)
    }

    const ageHundred = { completed: rule(null, 100), failed: rule(0) }
    expect(await storage.cleanup({ ...cleanupInput, retention: ageHundred })).toEqual({
      removed: 2,
      more: false,
    })
    // The row finished exactly at the cutoff survives a repeated pass.
    expect(await storage.cleanup({ ...cleanupInput, retention: ageHundred })).toEqual({
      removed: 0,
      more: false,
    })
    expect(db.prepare('SELECT id FROM walq_jobs ORDER BY finishedAt').all()).toEqual([
      { id: seeded[2] },
    ])

    const ageNinetyNine = { completed: rule(null, 99), failed: rule(0) }
    expect(await storage.cleanup({ ...cleanupInput, retention: ageNinetyNine })).toEqual({
      removed: 1,
      more: false,
    })
    expect(db.prepare('SELECT count(*) AS count FROM walq_jobs').get()).toEqual({ count: 0 })
  })

  it('combines count and age as the union of the two oldest tails', async () => {
    const { db, storage } = open()
    for (const finishedAt of [
      cleanupNow - 500,
      cleanupNow - 400,
      cleanupNow - 200,
      cleanupNow - 100,
    ]) {
      await storage.enqueue({ ...input, now: finishedAt, availableAt: finishedAt, attempts: 1 })
      const [job] = await storage.claim({ ...claimInput, now: finishedAt })
      await storage.complete({ ...job!, now: finishedAt })
    }

    // The age bound reaches further than the count bound, so it wins the union.
    const ageBound = { completed: rule(100, 200), failed: rule(0) }
    expect(await storage.cleanup({ ...cleanupInput, retention: ageBound })).toEqual({
      removed: 2,
      more: false,
    })
    expect(db.prepare('SELECT finishedAt FROM walq_jobs ORDER BY finishedAt').all()).toEqual([
      { finishedAt: cleanupNow - 200 },
      { finishedAt: cleanupNow - 100 },
    ])

    // A small count bound reaches further than an ineffective age bound.
    const countBound = { completed: rule(1, 10_000_000), failed: rule(0) }
    expect(await storage.cleanup({ ...cleanupInput, retention: countBound })).toEqual({
      removed: 1,
      more: false,
    })
    expect(db.prepare('SELECT finishedAt FROM walq_jobs ORDER BY finishedAt').all()).toEqual([
      { finishedAt: cleanupNow - 100 },
    ])
  })

  it('decides retention through bounded index queries', () => {
    const { db } = open()
    const queries = [
      `
        SELECT finishedAt, id FROM (
          SELECT finishedAt, id FROM walq_jobs
          WHERE queue = @queue AND status = @status AND finishedAt IS NOT NULL
          ORDER BY finishedAt DESC, id DESC
          LIMIT 1 OFFSET @offset
        )
        UNION ALL
        SELECT finishedAt, id FROM (
          SELECT finishedAt, id FROM walq_jobs
          WHERE queue = @queue AND status = @status AND finishedAt IS NOT NULL
            AND finishedAt < @cutoff
          ORDER BY finishedAt DESC, id DESC
          LIMIT 1
        )
        ORDER BY finishedAt DESC, id DESC
        LIMIT 1
      `,
      `
        SELECT finishedAt, id FROM walq_jobs
        WHERE queue = @queue AND status = @status AND finishedAt IS NOT NULL
          AND finishedAt < @cutoff
        ORDER BY finishedAt DESC, id DESC
        LIMIT 1
      `,
      `
        SELECT rowid FROM walq_jobs
        WHERE queue = @queue AND status = @status AND finishedAt IS NOT NULL
          AND (finishedAt, id) <= (@finishedAt, @id)
        ORDER BY finishedAt, id
        LIMIT @limit
      `,
      `
        SELECT 1 AS found FROM walq_jobs
        WHERE queue = @queue AND status = @status AND finishedAt IS NOT NULL
          AND (finishedAt, id) <= (@finishedAt, @id)
        LIMIT 1
      `,
    ]

    for (const query of queries) {
      const plan = db.prepare(`EXPLAIN QUERY PLAN ${query}`).all({
        queue: 'email',
        status: 'completed',
        limit: 10,
        offset: 0,
        cutoff: 5,
        finishedAt: 5,
        id: 'x',
      }) as { detail: string }[]
      const detail = plan.map((row) => row.detail).join('; ')

      // Searching the partial terminal index keeps the work proportional to
      // the batch instead of scanning the whole terminal history.
      expect(detail).toContain('SEARCH walq_jobs USING')
      expect(detail).toContain('walq_terminal')
      expect(detail).not.toContain('SCAN walq_jobs')
    }
  })

  it('rejects cleanup inside a caller transaction and invalid retention', async () => {
    const { db, storage } = open()
    db.exec('BEGIN')
    await expect(storage.cleanup(cleanupInput)).rejects.toThrow('transaction')
    db.exec('ROLLBACK')

    await expect(storage.cleanup({ ...cleanupInput, limit: 0 })).rejects.toThrow('limit')
    await expect(storage.cleanup({ ...cleanupInput, now: -1 })).rejects.toThrow('now')
    await expect(storage.cleanup({ ...cleanupInput, now: 1.5 })).rejects.toThrow('now')
    await expect(storage.cleanup({ ...cleanupInput, now: Number.NaN })).rejects.toThrow('now')
    await expect(
      storage.cleanup({ ...cleanupInput, retention: { completed: rule(-1), failed: rule(0) } }),
    ).rejects.toThrow('retention.completed')
    await expect(
      storage.cleanup({ ...cleanupInput, retention: { completed: rule(0, -1), failed: rule(0) } }),
    ).rejects.toThrow('retention.completed.maxAge')
    await expect(
      storage.cleanup({ ...cleanupInput, retention: { completed: rule(0), failed: rule(0, 1.5) } }),
    ).rejects.toThrow('retention.failed.maxAge')
    await expect(
      storage.cleanup({
        ...cleanupInput,
        retention: { completed: null, failed: rule(0) } as never,
      }),
    ).rejects.toThrow('retention.completed')
    await expect(storage.cleanup({ ...cleanupInput, retention: null as never })).rejects.toThrow(
      'retention',
    )
  })
})

describe('SQLite groups', () => {
  it('validates groups and rejects conflicting concurrency without mutation', async () => {
    const { db, storage } = open()

    for (const group of [
      { id: '', concurrency: 1 },
      { id: 'g', concurrency: 0 },
      { id: 'g', concurrency: 1.5 },
      { id: 'g', concurrency: Number.MAX_SAFE_INTEGER + 1 },
    ]) {
      await expect(storage.enqueue({ ...input, group } as never)).rejects.toThrow(TypeError)
    }
    expect(db.prepare('SELECT count(*) AS count FROM walq_jobs').get()).toEqual({ count: 0 })

    const first = await storage.enqueue({ ...input, group: { id: 'g', concurrency: 2 } })
    await expect(storage.enqueue({ ...input, group: { id: 'g', concurrency: 3 } })).rejects.toThrow(
      'already uses concurrency 2',
    )
    await expect(
      storage.enqueueMany([
        { ...input, group: { id: 'batch-conflict', concurrency: 1 } },
        { ...input, group: { id: 'batch-conflict', concurrency: 2 } },
      ]),
    ).rejects.toThrow('already uses concurrency 1')
    expect(await storage.inspect({ queue: input.queue, id: first.id })).not.toBeNull()
    expect(db.prepare('SELECT count(*) AS count FROM walq_jobs').get()).toEqual({ count: 1 })
    expect(db.prepare('SELECT count(*) AS count FROM walq_groups').get()).toEqual({ count: 1 })
  })

  it('deduplicates grouped jobs without changing membership and keeps group config stable', async () => {
    const { db, storage } = open()
    const first = await storage.enqueue({
      ...input,
      data: '{"original":true}',
      dedupe: 'same',
      group: { id: 'g', concurrency: 2 },
    })
    const duplicate = await storage.enqueue({
      ...input,
      data: '{"replacement":true}',
      dedupe: 'same',
      group: { id: 'g', concurrency: 2 },
    })
    expect(duplicate).toMatchObject({ id: first.id, data: '{"original":true}' })
    const differentGroupDuplicate = await storage.enqueue({
      ...input,
      dedupe: 'same',
      group: { id: 'unused', concurrency: 3 },
    })
    expect(differentGroupDuplicate.id).toBe(first.id)
    expect(db.prepare('SELECT id FROM walq_groups WHERE id = ?').get('unused')).toBeUndefined()
    await expect(
      storage.enqueue({ ...input, dedupe: 'same', group: { id: 'g', concurrency: 1 } }),
    ).rejects.toThrow('already uses concurrency 2')

    await storage.remove({ queue: input.queue, id: first.id })
    await expect(storage.enqueue({ ...input, group: { id: 'g', concurrency: 1 } })).rejects.toThrow(
      'already uses concurrency 2',
    )
    expect(db.prepare('SELECT queue, id, concurrency FROM walq_groups').all()).toEqual([
      { queue: 'email', id: 'g', concurrency: 2 },
    ])
  })

  it('enforces limits without head-of-line blocking and scopes groups to queues', async () => {
    const { storage } = open()
    const first = await storage.enqueue({
      ...input,
      priority: 100,
      group: { id: 'shared', concurrency: 1 },
    })
    const blocked = await storage.enqueue({
      ...input,
      priority: 99,
      group: { id: 'shared', concurrency: 1 },
    })
    const otherGroup = await storage.enqueue({
      ...input,
      priority: 98,
      group: { id: 'other', concurrency: 1 },
    })
    const ungrouped = await storage.enqueue({ ...input, priority: 97 })

    expect((await storage.claim({ ...claimInput, limit: 4 })).map(({ id }) => id)).toEqual([
      first.id,
      otherGroup.id,
      ungrouped.id,
    ])
    expect(await storage.inspect({ queue: input.queue, id: blocked.id })).toMatchObject({
      status: 'pending',
    })

    const isolated = await storage.enqueueMany([
      { ...input, queue: 'other-queue', group: { id: 'shared', concurrency: 2 } },
      { ...input, queue: 'other-queue', group: { id: 'shared', concurrency: 2 } },
    ])
    expect(
      (await storage.claim({ ...claimInput, queue: 'other-queue', limit: 2 })).map(({ id }) => id),
    ).toEqual(isolated.map(({ id }) => id))
  })

  it('scans past many saturated groups in global priority order', async () => {
    const { db, storage } = open()
    const groupCount = 70
    const saturated = await storage.enqueueMany(
      Array.from({ length: groupCount }, (_, index) => ({
        ...input,
        priority: 10_000 - index,
        group: { id: `saturated-${index}`, concurrency: 1 },
      })),
    )
    expect(await storage.claim({ ...claimInput, limit: groupCount })).toHaveLength(groupCount)

    const blocked = await storage.enqueueMany(
      saturated.map((job, index) => ({
        ...input,
        priority: 1_000 - index,
        group: { id: `saturated-${index}`, concurrency: 1 },
      })),
    )
    const available = await storage.enqueueMany([
      { ...input, priority: 3, availableAt: 10 },
      { ...input, priority: 2, availableAt: 10 },
      { ...input, priority: 3, availableAt: 9 },
    ])

    expect((await storage.claim({ ...claimInput, limit: 3 })).map(({ id }) => id)).toEqual([
      available[2]!.id,
      available[0]!.id,
      available[1]!.id,
    ])
    expect(
      db.prepare("SELECT count(*) AS count FROM walq_jobs WHERE status = 'pending'").get(),
    ).toEqual({ count: groupCount })
    expect(blocked).toHaveLength(groupCount)
  })

  it('uses the pending and active-group indexes for incremental claims', () => {
    const { db } = open()
    const pendingPlan = db
      .prepare(`
        EXPLAIN QUERY PLAN SELECT id, groupId, priority, availableAt, seq
        FROM walq_jobs INDEXED BY walq_pending
        WHERE queue = 'email' AND status = 'pending' AND availableAt <= 10
          AND attemptsMade < attempts
        ORDER BY priority DESC, availableAt, seq LIMIT 10
      `)
      .all() as { detail: string }[]
    const samePriorityPlan = db
      .prepare(`
        EXPLAIN QUERY PLAN SELECT id FROM walq_jobs INDEXED BY walq_pending
        WHERE queue = 'email' AND status = 'pending' AND availableAt <= 10
          AND attemptsMade < attempts AND priority = 5
          AND (availableAt, seq) > (10, 12)
        ORDER BY priority DESC, availableAt, seq LIMIT 10
      `)
      .all() as { detail: string }[]
    const lowerPriorityPlan = db
      .prepare(`
        EXPLAIN QUERY PLAN SELECT id FROM walq_jobs INDEXED BY walq_pending
        WHERE queue = 'email' AND status = 'pending' AND availableAt <= 10
          AND attemptsMade < attempts AND priority < 5
        ORDER BY priority DESC, availableAt, seq LIMIT 10
      `)
      .all() as { detail: string }[]
    const activeGroupPlan = db
      .prepare(`
        EXPLAIN QUERY PLAN SELECT count(*) FROM walq_jobs AS active INDEXED BY walq_active_group
        WHERE active.queue = 'email' AND active.groupId = 'group-1'
          AND active.status = 'active' AND active.groupId IS NOT NULL
      `)
      .all() as { detail: string }[]

    for (const plan of [pendingPlan, samePriorityPlan, lowerPriorityPlan]) {
      const details = plan.map(({ detail }) => detail).join('; ')
      expect(details).toContain('walq_pending')
      expect(details).not.toContain('TEMP B-TREE')
      expect(details).not.toContain('SCAN walq_jobs')
    }

    const activeGroupDetails = activeGroupPlan.map(({ detail }) => detail).join('; ')
    expect(activeGroupDetails).toContain('walq_active_group')
    expect(activeGroupDetails).toContain('queue=? AND groupId=?')
  })

  it('applies group capacity across claimQueues requests and releases it on state transitions', async () => {
    const { storage } = open()
    const jobs = await storage.enqueueMany(
      Array.from({ length: 3 }, () => ({
        ...input,
        group: { id: 'batch', concurrency: 2 },
      })),
    )
    const results = await storage.claimQueues!({
      requests: [claimInput, claimInput],
    })
    expect(results.map((result) => result.length)).toEqual([2, 0])
    await storage.complete({ ...results[0]![0]!, now: 11 })
    expect((await storage.claim(claimInput)).map(({ id }) => id)).toEqual([jobs[2]!.id])

    const failureJobs = await storage.enqueueMany([
      { ...input, group: { id: 'failure', concurrency: 1 } },
      { ...input, group: { id: 'failure', concurrency: 1 } },
    ])
    const [activeFailure] = await storage.claim(claimInput)
    expect(activeFailure?.id).toBe(failureJobs[0]!.id)
    expect(await storage.claim(claimInput)).toEqual([])
    await storage.fail({ ...activeFailure!, now: 11, error: 'retry later', retryAt: 100 })
    expect((await storage.claim(claimInput)).map(({ id }) => id)).toEqual([failureJobs[1]!.id])

    const recoveryJobs = await storage.enqueueMany([
      { ...input, attempts: 1, group: { id: 'recovery', concurrency: 1 } },
      { ...input, group: { id: 'recovery', concurrency: 1 } },
    ])
    const [expired] = await storage.claim({ ...claimInput, now: 10, leaseDuration: 10 })
    expect(expired?.id).toBe(recoveryJobs[0]!.id)
    const [afterRecovery] = await storage.claim({ ...claimInput, now: 20 })
    expect(afterRecovery?.id).toBe(recoveryJobs[1]!.id)
    expect(await storage.inspect({ queue: input.queue, id: recoveryJobs[0]!.id })).toMatchObject({
      status: 'failed',
    })
  })

  it('enforces group limits atomically across SQLite connections', async () => {
    const path = filename()
    const { storage } = open(path)
    await storage.enqueueMany(
      Array.from({ length: 8 }, () => ({
        ...input,
        group: { id: 'global', concurrency: 3 },
      })),
    )

    const results = await race(
      path,
      Array.from({ length: 5 }, () => ({
        method: 'claim',
        input: { ...claimInput, limit: 4 },
      })),
    )
    const claimed = (results as ClaimedJob[][]).flat()
    expect(claimed).toHaveLength(3)
    expect(new Set(claimed.map(({ id }) => id)).size).toBe(3)
  })

  it('materializes a due schedule only once across SQLite connections', async () => {
    const path = filename()
    const { db, storage } = open(path)
    await storage.upsertSchedule!({
      queue: 'email',
      id: 'concurrent',
      data: '{}',
      now: 100,
      every: 10,
    })
    db.close()

    const results = await race(
      path,
      Array.from({ length: 4 }, () => ({
        method: 'materializeSchedules',
        input: { queue: 'email', now: 150, attempts: 1 },
      })),
    )
    expect(results.filter((result) => result === 1)).toHaveLength(1)

    const reopened = open(path)
    expect(reopened.db.prepare('SELECT count(*) AS count FROM walq_jobs').get()).toEqual({
      count: 1,
    })
    expect(await reopened.storage.getSchedule!({ queue: 'email', id: 'concurrent' })).toMatchObject(
      {
        nextRunAt: 160,
      },
    )
  })
})

describe('SQLite queue pause', () => {
  it('persists queue pause across connections and reopen, including grouped claims', async () => {
    const path = filename()
    const first = open(path)
    const second = open(path)
    const pending = await first.storage.enqueue(input)
    const independent = await first.storage.enqueue({ ...input, queue: 'Email' })

    await first.storage.pause({ queue: 'email' })
    expect(await second.storage.claim(claimInput)).toEqual([])
    expect(
      await second.storage.claimQueues!({
        requests: [claimInput, { ...claimInput, queue: 'Email' }],
      }),
    ).toMatchObject([[], [{ id: independent.id }]])

    first.db.close()
    const reopened = open(path)
    expect(await reopened.storage.claim(claimInput)).toEqual([])
    await second.storage.resume({ queue: 'email' })
    expect(await reopened.storage.claim(claimInput)).toMatchObject([{ id: pending.id }])
  })

  it('does not materialize due schedules while paused and resumes them afterward', async () => {
    const { db, storage } = open()
    await storage.upsertSchedule!({ queue: 'email', id: 'repeat', data: '{}', now: 10, every: 5 })
    await storage.pause({ queue: 'email' })

    expect(await storage.materializeSchedules!({ queue: 'email', now: 20, attempts: 1 })).toBe(0)
    expect(await storage.getSchedule!({ queue: 'email', id: 'repeat' })).toMatchObject({
      nextRunAt: 15,
    })
    expect(db.prepare('SELECT count(*) AS count FROM walq_jobs').get()).toEqual({ count: 0 })

    await storage.resume({ queue: 'email' })
    expect(await storage.materializeSchedules!({ queue: 'email', now: 20, attempts: 1 })).toBe(1)
    expect(db.prepare('SELECT availableAt FROM walq_jobs').get()).toEqual({ availableAt: 15 })
  })
})

describe('SQLite integration', () => {
  it('persists, coalesces missed schedule runs, and leaves materialized jobs unchanged', async () => {
    const { db, storage } = open()
    const registration = {
      queue: 'email',
      id: 'repeat',
      data: '{"version":1}',
      now: 100,
      every: 10,
    } as const

    await expect(storage.upsertSchedule!(registration)).resolves.toMatchObject({ nextRunAt: 110 })
    await storage.upsertSchedule!({ ...registration, now: 120 })
    expect(await storage.getSchedule!({ queue: 'email', id: 'repeat' })).toMatchObject({
      data: '{"version":1}',
      nextRunAt: 110,
    })

    expect(await storage.materializeSchedules!({ queue: 'email', now: 135, attempts: 3 })).toBe(1)
    expect(await storage.materializeSchedules!({ queue: 'email', now: 135, attempts: 3 })).toBe(0)
    await storage.upsertSchedule!({ ...registration, data: '{"version":2}', now: 150 })
    expect(await storage.list({ queue: 'email', status: 'pending', limit: 10 })).toMatchObject([
      { data: '{"version":1}', availableAt: 110, createdAt: 135, attempts: 3 },
    ])
    expect(await storage.getSchedule!({ queue: 'email', id: 'repeat' })).toMatchObject({
      data: '{"version":2}',
      nextRunAt: 160,
    })
    await storage.upsertSchedule!({
      ...registration,
      data: '{"version":2}',
      every: 20,
      now: 170,
    })
    expect(await storage.getSchedule!({ queue: 'email', id: 'repeat' })).toMatchObject({
      every: 20,
      nextRunAt: 190,
    })

    expect(await storage.materializeSchedules!({ queue: 'email', now: 200, attempts: 1 })).toBe(1)
    expect(await storage.removeSchedule!({ queue: 'email', id: 'repeat' })).toBe(true)
    expect(await storage.materializeSchedules!({ queue: 'email', now: 1_000, attempts: 1 })).toBe(0)
    expect(db.prepare('SELECT data, availableAt FROM walq_jobs ORDER BY seq').all()).toEqual([
      { data: '{"version":1}', availableAt: 110 },
      { data: '{"version":2}', availableAt: 190 },
    ])
  })

  it('materializes overdue schedules in bounded batches', async () => {
    const { db, storage } = open()
    for (let index = 0; index < 105; index += 1) {
      await storage.upsertSchedule!({
        queue: 'email',
        id: `schedule-${index}`,
        data: '{}',
        now: 0,
        every: 10,
      })
    }

    expect(await storage.materializeSchedules!({ queue: 'email', now: 10, attempts: 1 })).toBe(100)
    expect(await storage.materializeSchedules!({ queue: 'email', now: 10, attempts: 1 })).toBe(5)
    expect(await storage.materializeSchedules!({ queue: 'email', now: 10, attempts: 1 })).toBe(0)
    expect(db.prepare('SELECT count(*) AS count FROM walq_jobs').get()).toEqual({ count: 105 })
  })

  it('rejects invalid schedule storage inputs and rolls back timestamp overflow', async () => {
    const { db, storage } = open()
    for (const every of [0, -1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
      await expect(
        storage.upsertSchedule!({ queue: 'email', id: 'invalid', data: '{}', now: 0, every }),
      ).rejects.toThrow(TypeError)
    }
    await expect(
      storage.upsertSchedule!({
        queue: 'email',
        id: 'invalid-cron',
        data: '{}',
        now: 0,
        cron: 'not a cron expression',
      }),
    ).rejects.toThrow(TypeError)

    await storage.upsertSchedule!({
      queue: 'email',
      id: 'overflow',
      data: '{}',
      now: 0,
      every: Number.MAX_SAFE_INTEGER,
    })
    await expect(
      storage.materializeSchedules!({
        queue: 'email',
        now: Number.MAX_SAFE_INTEGER,
        attempts: 1,
      }),
    ).rejects.toThrow(TypeError)
    expect(db.prepare('SELECT count(*) AS count FROM walq_jobs').get()).toEqual({ count: 0 })
    expect(await storage.getSchedule!({ queue: 'email', id: 'overflow' })).toMatchObject({
      nextRunAt: Number.MAX_SAFE_INTEGER,
    })
  })

  it('uses UTC cron occurrences and resumes overdue schedules after restart', async () => {
    const path = filename()
    const first = open(path)
    const midnight = Date.UTC(2024, 0, 1)
    await first.storage.upsertSchedule!({
      queue: 'email',
      id: 'daily',
      data: '{}',
      now: midnight,
      cron: '0 0 * * *',
    })
    expect(await first.storage.getSchedule!({ queue: 'email', id: 'daily' })).toMatchObject({
      nextRunAt: midnight + 24 * 60 * 60 * 1_000,
    })
    first.db.close()

    const reopened = open(path)
    const restartNow = midnight + 5 * 24 * 60 * 60 * 1_000
    expect(
      await reopened.storage.materializeSchedules!({
        queue: 'email',
        now: restartNow,
        attempts: 2,
      }),
    ).toBe(1)
    expect(
      await reopened.storage.materializeSchedules!({
        queue: 'email',
        now: restartNow,
        attempts: 2,
      }),
    ).toBe(0)
    expect(await reopened.storage.getSchedule!({ queue: 'email', id: 'daily' })).toMatchObject({
      nextRunAt: midnight + 6 * 24 * 60 * 60 * 1_000,
    })
    expect(
      await reopened.storage.list({ queue: 'email', status: 'pending', limit: 10 }),
    ).toMatchObject([{ availableAt: midnight + 24 * 60 * 60 * 1_000, createdAt: restartNow }])
  })

  it('migrates v2 jobs, leases, and indexes through v10', async () => {
    const db = new Database(':memory:')
    databases.push(db)
    db.exec(`
      CREATE TABLE walq_schema (id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL);
      INSERT INTO walq_schema (id, version) VALUES (1, 2);
      CREATE TABLE walq_jobs (
        id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
        queue TEXT NOT NULL COLLATE BINARY,
        name TEXT NOT NULL,
        data TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('pending', 'active', 'completed', 'failed')),
        createdAt INTEGER NOT NULL CHECK (createdAt >= 0),
        availableAt INTEGER NOT NULL CHECK (availableAt >= 0),
        finishedAt INTEGER CHECK (finishedAt IS NULL OR finishedAt >= 0),
        attemptsMade INTEGER NOT NULL CHECK (attemptsMade >= 0 AND attemptsMade <= attempts),
        attempts INTEGER NOT NULL CHECK (attempts > 0),
        error TEXT,
        leaseToken TEXT,
        expiresAt INTEGER,
        CHECK (
          (status = 'active' AND leaseToken IS NOT NULL AND expiresAt IS NOT NULL AND expiresAt >= 0)
          OR (status != 'active' AND leaseToken IS NULL AND expiresAt IS NULL)
        ),
        CHECK (
          (status IN ('completed', 'failed') AND finishedAt IS NOT NULL)
          OR (status NOT IN ('completed', 'failed') AND finishedAt IS NULL)
        )
      );
      CREATE INDEX walq_pending ON walq_jobs (queue, availableAt, id) WHERE status = 'pending';
      CREATE INDEX walq_active ON walq_jobs (queue, expiresAt) WHERE status = 'active';
      CREATE INDEX walq_terminal ON walq_jobs (queue, status, finishedAt DESC, id DESC)
        WHERE finishedAt IS NOT NULL;
      INSERT INTO walq_jobs VALUES
        ('pending-id', 'email', 'send', '{}', 'pending', 1, 2, NULL, 0, 2, NULL, NULL, NULL),
        ('active-id', 'email', 'send', '{}', 'active', 3, 4, NULL, 1, 3, 'last error', 'lease-token', 50),
        ('completed-id', 'email', 'send', '{}', 'completed', 5, 6, 7, 1, 3, NULL, NULL, NULL),
        ('failed-id', 'other', 'send', '{}', 'failed', 8, 9, 10, 2, 2, 'failed', NULL, NULL);
    `)

    const storage = betterSqlite3(db)
    expect(db.prepare('SELECT version FROM walq_schema').get()).toEqual({ version: 10 })
    expectPartialDedupeIndex(db)
    expect(
      db
        .prepare(
          'SELECT id, queue, status, availableAt, priority, finishedAt, attemptsMade, attempts, error, leaseToken, expiresAt FROM walq_jobs ORDER BY id',
        )
        .all(),
    ).toEqual([
      {
        id: 'active-id',
        queue: 'email',
        status: 'active',
        availableAt: 4,
        priority: 0,
        finishedAt: null,
        attemptsMade: 1,
        attempts: 3,
        error: 'last error',
        leaseToken: 'lease-token',
        expiresAt: 50,
      },
      {
        id: 'completed-id',
        queue: 'email',
        status: 'completed',
        availableAt: 6,
        priority: 0,
        finishedAt: 7,
        attemptsMade: 1,
        attempts: 3,
        error: null,
        leaseToken: null,
        expiresAt: null,
      },
      {
        id: 'failed-id',
        queue: 'other',
        status: 'failed',
        availableAt: 9,
        priority: 0,
        finishedAt: 10,
        attemptsMade: 2,
        attempts: 2,
        error: 'failed',
        leaseToken: null,
        expiresAt: null,
      },
      {
        id: 'pending-id',
        queue: 'email',
        status: 'pending',
        availableAt: 2,
        priority: 0,
        finishedAt: null,
        attemptsMade: 0,
        attempts: 2,
        error: null,
        leaseToken: null,
        expiresAt: null,
      },
    ])
    expect(db.prepare('PRAGMA index_list(walq_jobs)').all()).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: 'walq_pending' }),
        expect.objectContaining({ name: 'walq_active' }),
        expect.objectContaining({ name: 'walq_terminal' }),
      ]),
    )
    expect(db.prepare('PRAGMA index_info(walq_active)').all()).toEqual([
      { seqno: 0, cid: 2, name: 'queue' },
      { seqno: 1, cid: 15, name: 'expiresAt' },
      { seqno: 2, cid: 1, name: 'id' },
    ])
    expect(db.prepare('SELECT id, seq FROM walq_jobs ORDER BY seq').all()).toEqual([
      { id: 'pending-id', seq: 1 },
      { id: 'active-id', seq: 2 },
      { id: 'completed-id', seq: 3 },
      { id: 'failed-id', seq: 4 },
    ])
    expect(
      await storage.heartbeat({
        id: 'active-id',
        leaseToken: 'lease-token',
        now: 20,
        leaseDuration: 10,
      }),
    ).toBe('applied')
  })

  it('migrates v3 jobs through v10 with zero priority', async () => {
    const db = new Database(':memory:')
    databases.push(db)
    db.exec(`
      CREATE TABLE walq_schema (id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL);
      INSERT INTO walq_schema (id, version) VALUES (1, 3);
      CREATE TABLE walq_jobs (
        id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
        queue TEXT NOT NULL COLLATE BINARY,
        name TEXT NOT NULL,
        data TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('pending', 'active', 'completed', 'failed', 'cancelled')),
        createdAt INTEGER NOT NULL CHECK (createdAt >= 0),
        availableAt INTEGER NOT NULL CHECK (availableAt >= 0),
        finishedAt INTEGER CHECK (finishedAt IS NULL OR finishedAt >= 0),
        attemptsMade INTEGER NOT NULL CHECK (attemptsMade >= 0 AND attemptsMade <= attempts),
        attempts INTEGER NOT NULL CHECK (attempts > 0),
        error TEXT,
        leaseToken TEXT,
        expiresAt INTEGER,
        CHECK (
          (status = 'active' AND leaseToken IS NOT NULL AND expiresAt IS NOT NULL AND expiresAt >= 0)
          OR (status != 'active' AND leaseToken IS NULL AND expiresAt IS NULL)
        ),
        CHECK (
          (status IN ('completed', 'failed', 'cancelled') AND finishedAt IS NOT NULL)
          OR (status NOT IN ('completed', 'failed', 'cancelled') AND finishedAt IS NULL)
        )
      );
      CREATE INDEX walq_pending ON walq_jobs (queue, availableAt, id) WHERE status = 'pending';
      INSERT INTO walq_jobs (
        id, queue, name, data, status, createdAt, availableAt, attemptsMade, attempts
      ) VALUES ('legacy', 'email', 'send', '{}', 'pending', 1, 1, 0, 1);
    `)

    const storage = betterSqlite3(db)
    expect(db.prepare('SELECT version FROM walq_schema').get()).toEqual({ version: 10 })
    expectPartialDedupeIndex(db)
    expect(db.prepare('SELECT priority FROM walq_jobs WHERE id = ?').get('legacy')).toEqual({
      priority: 0,
    })

    await storage.enqueue({ ...input, priority: 10 })
    expect((await storage.claim(claimInput)).map(({ priority }) => priority)).toEqual([10, 0])
  })

  it('migrates schema v4 through v10 and preserves existing jobs', async () => {
    const { db } = open()
    const legacy = await betterSqlite3(db).enqueue(input)
    db.exec(`
      DROP INDEX walq_dedupe;
      DROP INDEX walq_active_group;
      DROP INDEX walq_schedules_due;
      DROP TABLE walq_schedules;
      DROP TABLE walq_paused_queues;
      ALTER TABLE walq_jobs DROP COLUMN dedupe;
      DROP TABLE walq_groups;
      ALTER TABLE walq_jobs DROP COLUMN groupId;
      UPDATE walq_schema SET version = 4;
    `)

    const storage = betterSqlite3(db)
    expect(db.prepare('SELECT version FROM walq_schema').get()).toEqual({ version: 10 })
    expectPartialDedupeIndex(db)
    expect(await storage.inspect({ queue: 'email', id: legacy.id })).toMatchObject({
      id: legacy.id,
      data: input.data,
    })

    const first = await storage.enqueue({ ...input, dedupe: 'migrated-key' })
    const duplicate = await storage.enqueue({
      ...input,
      data: '{"replacement":true}',
      dedupe: 'migrated-key',
    })
    expect(duplicate).toMatchObject({ id: first.id, data: input.data })
  })

  it('migrates schema v6 in place and preserves existing ungrouped jobs', async () => {
    const { db, storage } = open()
    const legacy = await storage.enqueue(input)
    db.exec(`
      DROP INDEX walq_active_group;
      DROP INDEX walq_schedules_due;
      DROP TABLE walq_schedules;
      DROP TABLE walq_paused_queues;
      DROP TABLE walq_groups;
      ALTER TABLE walq_jobs DROP COLUMN groupId;
      UPDATE walq_schema SET version = 6;
    `)

    const migrated = betterSqlite3(db)
    expect(db.prepare('SELECT version FROM walq_schema').get()).toEqual({ version: 10 })
    expect(db.prepare('SELECT groupId FROM walq_jobs WHERE id = ?').get(legacy.id)).toEqual({
      groupId: null,
    })
    expect(await migrated.inspect({ queue: input.queue, id: legacy.id })).toMatchObject({
      id: legacy.id,
      data: input.data,
    })
    await migrated.enqueue({ ...input, group: { id: 'new-group', concurrency: 2 } })
  })

  it('migrates the v7 group schema by adding the active-group index', async () => {
    const { db, storage } = open()
    const grouped = await storage.enqueue({ ...input, group: { id: 'v7-group', concurrency: 2 } })
    await storage.claim({ ...claimInput, limit: 1 })
    db.exec(
      'DROP INDEX walq_active_group; DROP INDEX walq_schedules_due; DROP TABLE walq_schedules; DROP TABLE walq_paused_queues; UPDATE walq_schema SET version = 7',
    )

    const migrated = betterSqlite3(db)
    expect(db.prepare('SELECT version FROM walq_schema').get()).toEqual({ version: 10 })
    expect(
      db
        .prepare(
          "SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'walq_active_group'",
        )
        .get(),
    ).toEqual({
      name: 'walq_active_group',
    })
    expect(await migrated.inspect({ queue: input.queue, id: grouped.id })).toMatchObject({
      id: grouped.id,
    })
  })

  it('migrates the v8 schema through v10 with schedules and queue pause state', async () => {
    const { db, storage } = open()
    const job = await storage.enqueue(input)
    db.exec(
      `DROP INDEX walq_schedules_due; DROP TABLE walq_schedules; DROP TABLE walq_paused_queues; UPDATE walq_schema SET version = 8`,
    )

    const migrated = betterSqlite3(db)
    expect(db.prepare('SELECT version FROM walq_schema').get()).toEqual({ version: 10 })
    expect(await migrated.inspect({ queue: input.queue, id: job.id })).toMatchObject({
      id: job.id,
      data: input.data,
    })
    await expect(migrated.getSchedule!({ queue: 'email', id: 'not-created' })).resolves.toBeNull()
  })

  it('migrates the v9 schema by adding durable queue pause state', async () => {
    const { db, storage } = open()
    const job = await storage.enqueue(input)
    db.exec('DROP TABLE walq_paused_queues; UPDATE walq_schema SET version = 9')

    const migrated = betterSqlite3(db)
    expect(db.prepare('SELECT version FROM walq_schema').get()).toEqual({ version: 10 })
    expect(
      db
        .prepare(
          "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'walq_paused_queues'",
        )
        .get(),
    ).toEqual({ name: 'walq_paused_queues' })
    await migrated.pause({ queue: 'email' })
    expect(await migrated.claim(claimInput)).toEqual([])
    expect(await migrated.inspect({ queue: 'email', id: job.id })).toMatchObject({
      status: 'pending',
    })
  })

  it('persists group configuration across storage reopen', async () => {
    const path = filename()
    const { db, storage } = open(path)
    await storage.enqueueMany(
      Array.from({ length: 2 }, () => ({
        ...input,
        group: { id: 'persistent', concurrency: 2 },
      })),
    )
    db.close()

    const reopened = open(path)
    await expect(
      reopened.storage.enqueue({ ...input, group: { id: 'persistent', concurrency: 1 } }),
    ).rejects.toThrow('already uses concurrency 2')
    expect((await reopened.storage.claim({ ...claimInput, limit: 5 })).length).toBe(2)
  })

  it('migrates v5 rows through v10 with deterministic approximate enqueue order', async () => {
    const db = new Database(':memory:')
    databases.push(db)
    db.exec(`
      CREATE TABLE walq_schema (id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL);
      INSERT INTO walq_schema (id, version) VALUES (1, 5);
      CREATE TABLE walq_jobs (
        id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
        queue TEXT NOT NULL COLLATE BINARY,
        name TEXT NOT NULL,
        data TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('pending', 'active', 'completed', 'failed', 'cancelled')),
        createdAt INTEGER NOT NULL CHECK (createdAt >= 0),
        availableAt INTEGER NOT NULL CHECK (availableAt >= 0),
        priority INTEGER NOT NULL CHECK (priority >= -9007199254740991 AND priority <= 9007199254740991),
        dedupe TEXT,
        finishedAt INTEGER CHECK (finishedAt IS NULL OR finishedAt >= 0),
        attemptsMade INTEGER NOT NULL CHECK (attemptsMade >= 0 AND attemptsMade <= attempts),
        attempts INTEGER NOT NULL CHECK (attempts > 0),
        error TEXT,
        leaseToken TEXT,
        expiresAt INTEGER,
        CHECK (
          (status = 'active' AND leaseToken IS NOT NULL AND expiresAt IS NOT NULL AND expiresAt >= 0)
          OR (status != 'active' AND leaseToken IS NULL AND expiresAt IS NULL)
        ),
        CHECK (
          (status IN ('completed', 'failed', 'cancelled') AND finishedAt IS NOT NULL)
          OR (status NOT IN ('completed', 'failed', 'cancelled') AND finishedAt IS NULL)
        )
      );
      CREATE INDEX walq_pending ON walq_jobs (queue, priority DESC, availableAt, id)
        WHERE status = 'pending';
      CREATE INDEX walq_active ON walq_jobs (queue, expiresAt, id) WHERE status = 'active';
      CREATE INDEX walq_terminal ON walq_jobs (queue, status, finishedAt DESC, id DESC)
        WHERE finishedAt IS NOT NULL;
      CREATE UNIQUE INDEX walq_dedupe ON walq_jobs (queue, dedupe) WHERE dedupe IS NOT NULL;
      INSERT INTO walq_jobs (
        id, queue, name, data, status, createdAt, availableAt, priority, dedupe,
        attemptsMade, attempts
      ) VALUES
        ('z-later', 'email', 'send', '{}', 'pending', 20, 10, 4, NULL, 0, 2),
        ('b-earlier', 'email', 'send', '{}', 'pending', 10, 10, 3, NULL, 0, 2),
        ('a-earlier', 'email', 'send', '{}', 'pending', 10, 10, 2, NULL, 0, 2);
    `)

    const storage = betterSqlite3(db)

    expect(db.prepare('SELECT version FROM walq_schema').get()).toEqual({ version: 10 })
    expect(db.prepare('SELECT id, seq FROM walq_jobs ORDER BY seq').all()).toEqual([
      { id: 'a-earlier', seq: 1 },
      { id: 'b-earlier', seq: 2 },
      { id: 'z-later', seq: 3 },
    ])
    expect(
      db.prepare("SELECT sql FROM sqlite_master WHERE name = 'walq_pending'").get(),
    ).toMatchObject({
      sql: expect.stringContaining('availableAt, seq'),
    })
    const claimPlan = db
      .prepare(`
        EXPLAIN QUERY PLAN SELECT id FROM walq_jobs INDEXED BY walq_pending
        WHERE queue = 'email' AND status = 'pending' AND availableAt <= 10
          AND attemptsMade < attempts
        ORDER BY priority DESC, availableAt, seq LIMIT 10
      `)
      .all() as { detail: string }[]
    expect(claimPlan.map(({ detail }) => detail).join('; ')).not.toContain('TEMP B-TREE')
    const next = await storage.enqueue(input)
    expect(db.prepare('SELECT seq FROM walq_jobs WHERE id = ?').get(next.id)).toEqual({ seq: 4 })
  })

  it('uses insertion order for tied claims and pending lists without exposing sequence', async () => {
    const { db, storage } = open()
    const first = await storage.enqueue({ ...input, dedupe: 'first-key' })
    const [deduplicated, second, third] = await storage.enqueueMany([
      { ...input, dedupe: 'first-key' },
      { ...input, dedupe: 'batch-key' },
      input,
    ])

    expect(deduplicated?.id).toBe(first.id)
    expect(db.prepare('SELECT count(*) AS count FROM walq_jobs').get()).toEqual({ count: 3 })

    const rename = db.prepare('UPDATE walq_jobs SET id = ? WHERE id = ?')
    rename.run('z-first', first.id)
    rename.run('a-second', second!.id)
    rename.run('m-third', third!.id)

    expect(db.prepare('SELECT id, seq FROM walq_jobs ORDER BY seq').all()).toEqual([
      { id: 'z-first', seq: 1 },
      { id: 'a-second', seq: 2 },
      { id: 'm-third', seq: 3 },
    ])
    expect(
      (await storage.list({ queue: 'email', status: 'pending', limit: 10 })).map(({ id }) => id),
    ).toEqual(['z-first', 'a-second', 'm-third'])
    expect((await storage.claim(claimInput)).map(({ id }) => id)).toEqual([
      'z-first',
      'a-second',
      'm-third',
    ])
    expect(first).not.toHaveProperty('seq')
    expect(await storage.inspect({ queue: 'email', id: 'z-first' })).not.toHaveProperty('seq')
  })

  it('does not reuse deleted enqueue positions and persists sequence across reopen', async () => {
    const path = filename()
    const { db, storage } = open(path)
    const first = await storage.enqueue(input)
    const deleted = await storage.enqueue(input)
    const deletedSeq = (
      db.prepare('SELECT seq FROM walq_jobs WHERE id = ?').get(deleted.id) as { seq: number }
    ).seq

    expect(await storage.remove({ queue: 'email', id: deleted.id })).toBe(true)
    const replacement = await storage.enqueue(input)
    const replacementSeq = (
      db.prepare('SELECT seq FROM walq_jobs WHERE id = ?').get(replacement.id) as { seq: number }
    ).seq
    expect(replacementSeq).toBeGreaterThan(deletedSeq)
    db.close()

    const reopened = open(path)
    const afterReopen = await reopened.storage.enqueue(input)
    const afterReopenSeq = (
      reopened.db.prepare('SELECT seq FROM walq_jobs WHERE id = ?').get(afterReopen.id) as {
        seq: number
      }
    ).seq
    expect(afterReopenSeq).toBeGreaterThan(replacementSeq)

    const rename = reopened.db.prepare('UPDATE walq_jobs SET id = ? WHERE id = ?')
    rename.run('z-first', first.id)
    rename.run('a-replacement', replacement.id)
    rename.run('m-after-reopen', afterReopen.id)
    expect(
      (await reopened.storage.list({ queue: 'email', status: 'pending', limit: 10 })).map(
        ({ id }) => id,
      ),
    ).toEqual(['z-first', 'a-replacement', 'm-after-reopen'])
  })

  it('persists signed safe-integer priority boundaries', async () => {
    const { db, storage } = open()
    await storage.enqueue({ ...input, priority: Number.MIN_SAFE_INTEGER })
    await storage.enqueue({ ...input, priority: Number.MAX_SAFE_INTEGER })

    expect((await storage.claim(claimInput)).map(({ priority }) => priority)).toEqual([
      Number.MAX_SAFE_INTEGER,
      Number.MIN_SAFE_INTEGER,
    ])
    expect(db.prepare('SELECT priority FROM walq_jobs ORDER BY priority').all()).toEqual([
      { priority: Number.MIN_SAFE_INTEGER },
      { priority: Number.MAX_SAFE_INTEGER },
    ])
  })

  it('persists enqueued jobs across reopen', async () => {
    const path = filename()
    const { db, storage } = open(path)
    const job = await storage.enqueue({ ...input, priority: 11 })
    db.close()
    const reopened = open(path)
    expect(await reopened.storage.claim(claimInput)).toMatchObject([{ id: job.id, priority: 11 }])
    expect(reopened.db.open).toBe(true)
  })

  it('supports repeated initialization on the same connection', async () => {
    const { db, storage } = open()
    expectPartialDedupeIndex(db)
    betterSqlite3(db)
    betterSqlite3(db)
    await storage.enqueue(input)
    expect(await storage.claim(claimInput)).toHaveLength(1)
  })

  it('leaves connection settings alone', async () => {
    const { db } = open()
    const journal = db.pragma('journal_mode', { simple: true })
    betterSqlite3(db)
    expect(db.pragma('journal_mode', { simple: true })).toBe(journal)
  })

  it('rejects unsupported schema versions and external transactions', async () => {
    const { db, storage } = open()
    db.exec('BEGIN')
    expect(() => betterSqlite3(db)).toThrow('transaction')
    await expect(storage.enqueue(input)).rejects.toThrow('transaction')
    await expect(storage.enqueueMany([input])).rejects.toThrow('transaction')
    db.exec('ROLLBACK; UPDATE walq_schema SET version = 11')
    expect(() => betterSqlite3(db)).toThrow('version')
  })

  it('rejects a version 1 database instead of migrating it', () => {
    const db = new Database(':memory:')
    databases.push(db)
    db.exec(`
      CREATE TABLE walq_schema (id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL);
      INSERT INTO walq_schema (id, version) VALUES (1, 1);
    `)
    expect(() => betterSqlite3(db)).toThrow('Unsupported walq schema version: 1')
  })

  it('rolls back every insert in enqueueMany when a later insert fails', async () => {
    const { db, storage } = open()
    db.exec(`CREATE TRIGGER reject_enqueue_many BEFORE INSERT ON walq_jobs
      WHEN NEW.data = '"reject"'
      BEGIN SELECT RAISE(ABORT, 'enqueueMany failed'); END`)

    await expect(
      storage.enqueueMany([
        { ...input, data: '{"first":true}' },
        { ...input, data: '"reject"' },
      ]),
    ).rejects.toThrow('enqueueMany failed')

    expect(db.prepare('SELECT count(*) AS count FROM walq_jobs').get()).toEqual({ count: 0 })
  })

  it('rolls back new dedupe keys when an atomic batch fails', async () => {
    const { db, storage } = open()
    const existing = await storage.enqueue({ ...input, dedupe: 'stored-key' })
    db.exec(`CREATE TRIGGER reject_deduped_enqueue BEFORE INSERT ON walq_jobs
      WHEN NEW.data = '"reject"'
      BEGIN SELECT RAISE(ABORT, 'enqueueMany failed'); END`)

    await expect(
      storage.enqueueMany([
        { ...input, dedupe: 'batch-key', data: '{"first":true}' },
        { ...input, dedupe: 'batch-key', data: '{"second":true}' },
        { ...input, dedupe: 'stored-key', data: '{"replacement":true}' },
        { ...input, data: '"reject"' },
      ]),
    ).rejects.toThrow('enqueueMany failed')

    expect(db.prepare('SELECT count(*) AS count FROM walq_jobs').get()).toEqual({ count: 1 })
    expect(await storage.inspect({ queue: input.queue, id: existing.id })).toMatchObject({
      data: input.data,
      status: 'pending',
    })
    const afterRollback = await storage.enqueue({
      ...input,
      data: '{"after":"rollback"}',
      dedupe: 'batch-key',
    })
    expect(afterRollback.id).not.toBe(existing.id)
    expect(afterRollback.data).toBe('{"after":"rollback"}')
  })

  it('rolls back the whole claim on a database error', async () => {
    const { db, storage } = open()
    await storage.enqueue(input)
    await storage.claim(claimInput)
    db.exec(`CREATE TRIGGER reject_claim BEFORE UPDATE ON walq_jobs
      WHEN NEW.status = 'active' BEGIN SELECT RAISE(ABORT, 'claim failed'); END`)
    await expect(storage.claim({ ...claimInput, now: 30 })).rejects.toThrow('claim failed')
    expect(db.prepare('SELECT status, attemptsMade, expiresAt FROM walq_jobs').get()).toEqual({
      status: 'active',
      attemptsMade: 1,
      expiresAt: 30,
    })
  })

  it('rolls back every request in one chunk when a later queue fails', async () => {
    const { db, storage } = open()
    await storage.enqueue({ ...input, queue: 'a' })
    await storage.enqueue({ ...input, queue: 'b' })
    await storage.claim({ ...claimInput, queue: 'a' })
    await storage.claim({ ...claimInput, queue: 'b' })
    db.exec(`CREATE TRIGGER reject_claim BEFORE UPDATE ON walq_jobs
      WHEN NEW.status = 'active' AND NEW.queue = 'b'
      BEGIN SELECT RAISE(ABORT, 'grouped claim failed'); END`)

    // The default limit keeps both requests in one chunk, so they share a transaction.
    await expect(
      storage.claimQueues!({
        requests: [
          { ...claimInput, queue: 'a', now: 30 },
          { ...claimInput, queue: 'b', now: 30 },
        ],
      }),
    ).rejects.toThrow('grouped claim failed')

    // The successful first request must roll back with the failed second one.
    expect(
      db.prepare('SELECT queue, status, attemptsMade FROM walq_jobs ORDER BY queue').all(),
    ).toEqual([
      { queue: 'a', status: 'active', attemptsMade: 1 },
      { queue: 'b', status: 'active', attemptsMade: 1 },
    ])
  })

  it('keeps earlier chunks committed when a later chunk fails', async () => {
    const { db, storage } = open()
    await storage.enqueue({ ...input, queue: 'a' })
    await storage.enqueue({ ...input, queue: 'b' })
    db.exec(`CREATE TRIGGER reject_claim BEFORE UPDATE ON walq_jobs
      WHEN NEW.status = 'active' AND NEW.queue = 'b'
      BEGIN SELECT RAISE(ABORT, 'grouped claim failed'); END`)

    // A limit above the budget gives every request its own transaction, so the
    // failure cannot reach the chunk that already committed.
    await expect(
      storage.claimQueues!({
        requests: [
          { ...claimInput, queue: 'a', now: 30, limit: 600 },
          { ...claimInput, queue: 'b', now: 30, limit: 600 },
        ],
      }),
    ).rejects.toThrow('grouped claim failed')

    expect(
      db.prepare('SELECT queue, status, attemptsMade FROM walq_jobs ORDER BY queue').all(),
    ).toEqual([
      { queue: 'a', status: 'active', attemptsMade: 1 },
      { queue: 'b', status: 'pending', attemptsMade: 0 },
    ])
  })

  it('claims every queue once when a batch spans several transactions', async () => {
    const { db, storage } = open()
    const queues = Array.from({ length: 70 }, (_, index) => `queue-${index}`)
    for (const queue of queues) {
      await storage.enqueue({ ...input, queue })
      await storage.enqueue({ ...input, queue })
    }

    // limit 16 fits 32 queues per transaction, so 70 queues need three of them.
    const requests = queues.map((queue) => ({ ...claimInput, queue, limit: 16 }))
    const results = await storage.claimQueues!({ requests })

    expect(results).toHaveLength(queues.length)
    expect(results.map((jobs) => jobs.length)).toEqual(queues.map(() => 2))
    expect(new Set(results.flat().map((job) => job.id)).size).toBe(queues.length * 2)
    expect(
      db.prepare("SELECT count(*) AS c FROM walq_jobs WHERE status = 'pending'").get(),
    ).toEqual({ c: 0 })
  })

  it('rejects database lock errors rather than returning lease_lost', async () => {
    const path = filename()
    const first = open(path)
    const second = open(path)
    second.db.pragma('busy_timeout = 0')
    await first.storage.enqueue(input)
    const [job] = await first.storage.claim(claimInput)
    first.db.exec('BEGIN IMMEDIATE')
    try {
      await expect(second.storage.complete({ ...job!, now: 11 })).rejects.toThrow('locked')
    } finally {
      first.db.exec('ROLLBACK')
    }
    expect(await second.storage.complete({ ...job!, now: 11 })).toBe('applied')
  })

  it.each(['complete', 'heartbeat'])(
    'serializes recovery against %s in another worker',
    async (method) => {
      const path = filename()
      const { db, storage } = open(path)
      db.pragma('journal_mode = WAL')
      await storage.enqueue(input)
      const [job] = await storage.claim(claimInput)
      const [mutation, claimed] = await race(path, [
        { method, input: { ...job!, now: 29, leaseDuration: 50 } },
        { method: 'claim', input: { ...claimInput, now: 30 } },
      ])
      const jobs = claimed as ClaimedJob[]
      const outcome = {
        mutation,
        ids: jobs.map((item) => item.id),
        state: db.prepare('SELECT status, attemptsMade FROM walq_jobs').get(),
      }
      expect([
        {
          mutation: 'applied',
          ids: [],
          state: { status: method === 'complete' ? 'completed' : 'active', attemptsMade: 1 },
        },
        { mutation: 'lease_lost', ids: [job!.id], state: { status: 'active', attemptsMade: 2 } },
      ]).toContainEqual(outcome)
      expect(jobs.every((item) => item.leaseToken !== job!.leaseToken)).toBe(true)
    },
  )

  it('atomically serializes concurrent retry and cancellation transitions', async () => {
    const path = filename()
    const { db, storage } = open(path)
    db.pragma('journal_mode = WAL')

    const failed = await storage.enqueue({ ...input, attempts: 1 })
    const [claimed] = await storage.claim(claimInput)
    await storage.fail({
      id: claimed!.id,
      leaseToken: claimed!.leaseToken,
      now: 11,
      error: 'retry me',
      retryAt: null,
    })
    const retryInput = { queue: 'email', id: failed.id, now: 12 }
    const retryResults = await race(path, [
      { method: 'retry', input: retryInput },
      { method: 'retry', input: retryInput },
    ])
    expect(retryResults.sort()).toEqual([false, true])
    expect(
      db.prepare('SELECT status, attemptsMade, attempts, availableAt FROM walq_jobs').get(),
    ).toEqual({ status: 'pending', attemptsMade: 1, attempts: 2, availableAt: 12 })

    const pending = await storage.enqueue(input)
    const cancelInput = { queue: 'email', id: pending.id, now: 13 }
    const cancelResults = await race(path, [
      { method: 'cancel', input: cancelInput },
      { method: 'cancel', input: cancelInput },
    ])
    expect(cancelResults.sort()).toEqual([false, true])
    expect(await storage.inspect({ queue: 'email', id: pending.id })).toMatchObject({
      status: 'cancelled',
      finishedAt: 13,
    })

    const max = Number.MAX_SAFE_INTEGER
    db.prepare(`
      INSERT INTO walq_jobs
        (id, queue, name, data, status, createdAt, availableAt, priority, finishedAt,
         attemptsMade, attempts, error)
      VALUES ('retry-boundary', 'email', 'send', '{}', 'failed', 0, 0, 0, 0, @attempt, @attempt, NULL)
    `).run({ attempt: max - 1 })
    const boundaryRetryInput = { queue: 'email', id: 'retry-boundary', now: 14 }
    const boundaryResults = await race(path, [
      { method: 'retry', input: boundaryRetryInput },
      { method: 'retry', input: boundaryRetryInput },
    ])
    expect(boundaryResults.sort()).toEqual([false, true])
    expect(
      db
        .prepare("SELECT status, attemptsMade, attempts FROM walq_jobs WHERE id = 'retry-boundary'")
        .get(),
    ).toEqual({ status: 'pending', attemptsMade: max - 1, attempts: max })
  })

  it('validates inspection inputs and protects attempt-count overflow', async () => {
    const { db, storage } = open()
    const pending = await storage.enqueue(input)

    await expect(storage.inspect({ queue: '', id: pending.id })).rejects.toThrow('queue')
    await expect(
      storage.list({ queue: 'email', status: undefined as never, limit: 1 }),
    ).rejects.toThrow('status')
    await expect(
      storage.list({ queue: 'email', status: 'pending', limit: Number.MAX_SAFE_INTEGER + 1 }),
    ).rejects.toThrow('limit')
    await expect(
      storage.retry({ queue: 'email', id: pending.id, now: Number.MAX_SAFE_INTEGER + 1 }),
    ).rejects.toThrow('now')
    await expect(storage.cancel({ queue: 'email', id: pending.id, now: -1 })).rejects.toThrow('now')
    await expect(
      storage.reschedule({
        queue: 'email',
        id: pending.id,
        availableAt: Number.MAX_SAFE_INTEGER + 1,
      }),
    ).rejects.toThrow('availableAt')

    const max = Number.MAX_SAFE_INTEGER
    db.prepare(`
      INSERT INTO walq_jobs
        (id, queue, name, data, status, createdAt, availableAt, priority, finishedAt,
         attemptsMade, attempts, error)
      VALUES ('overflow', 'email', 'send', '{}', 'failed', 0, 0, 0, 0, @max, @max, NULL)
    `).run({ max })
    await expect(storage.retry({ queue: 'email', id: 'overflow', now: 1 })).rejects.toThrow(
      RangeError,
    )
    expect(
      db
        .prepare("SELECT status, attemptsMade, attempts FROM walq_jobs WHERE id = 'overflow'")
        .get(),
    ).toEqual({ status: 'failed', attemptsMade: max, attempts: max })
  })

  it('assigns concurrent inserts distinct enqueue positions in serialized order', async () => {
    const path = filename()
    const { db } = open(path)
    db.pragma('journal_mode = WAL')
    const results = await race(
      path,
      Array.from({ length: 4 }, () => ({ method: 'enqueue', input })),
    )
    const jobs = results as StoredJob[]
    const rows = db.prepare('SELECT id, seq FROM walq_jobs ORDER BY seq').all() as {
      id: string
      seq: number
    }[]

    expect(rows.map(({ seq }) => seq)).toEqual([1, 2, 3, 4])
    const rename = db.prepare('UPDATE walq_jobs SET id = ? WHERE id = ?')
    for (const [index, row] of rows.entries()) {
      rename.run(['z-job', 'm-job', 'c-job', 'a-job'][index]!, row.id)
    }
    expect(jobs).toHaveLength(4)
    expect(
      (await betterSqlite3(db).list({ queue: 'email', status: 'pending', limit: 10 })).map(
        ({ id }) => id,
      ),
    ).toEqual(['z-job', 'm-job', 'c-job', 'a-job'])
  })

  it('deduplicates concurrent enqueue calls across database connections', async () => {
    const path = filename()
    const { db } = open(path)
    db.pragma('journal_mode = WAL')
    const results = await race(
      path,
      Array.from({ length: 4 }, () => ({
        method: 'enqueue',
        input: { ...input, dedupe: 'concurrent-key' },
      })),
    )
    const jobs = results as StoredJob[]

    expect(new Set(jobs.map(({ id }) => id)).size).toBe(1)
    expect(db.prepare('SELECT count(*) AS count FROM walq_jobs').get()).toEqual({ count: 1 })
  })

  it('never issues duplicate live leases to concurrent workers', async () => {
    const path = filename()
    const { db, storage } = open(path)
    db.pragma('journal_mode = WAL')
    for (let index = 0; index < 40; index += 1) await storage.enqueue(input)
    const results = await race(
      path,
      Array.from({ length: 4 }, () => ({ method: 'claim', input: claimInput })),
    )
    const ids = (results as ClaimedJob[][]).flat().map((job) => job.id)
    expect(ids).toHaveLength(40)
    expect(new Set(ids).size).toBe(40)
  })
})
