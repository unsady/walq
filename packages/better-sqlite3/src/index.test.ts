import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Worker } from 'node:worker_threads'

import type { ClaimedJob, LeaseMutationResult } from '@walq/core/storage'
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

const raceTimeout = 4000

async function race(path: string, operations: { method: string; input: object }[]) {
  const gate = new SharedArrayBuffer(4)
  const workers: Worker[] = []
  const timers: NodeJS.Timeout[] = []
  try {
    const tasks = operations.map(
      (operation) =>
        new Promise<ClaimedJob[] | LeaseMutationResult | boolean>((resolve, reject) => {
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
              message: { ready: true } | { result: ClaimedJob[] | LeaseMutationResult | boolean },
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
        }),
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

describe('SQLite integration', () => {
  it('migrates v2 jobs, leases, and indexes to the v3 schema', async () => {
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
    expect(db.prepare('SELECT version FROM walq_schema').get()).toEqual({ version: 3 })
    expect(
      db
        .prepare(
          'SELECT id, queue, status, availableAt, finishedAt, attemptsMade, attempts, error, leaseToken, expiresAt FROM walq_jobs ORDER BY id',
        )
        .all(),
    ).toEqual([
      {
        id: 'active-id',
        queue: 'email',
        status: 'active',
        availableAt: 4,
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
      { seqno: 0, cid: 1, name: 'queue' },
      { seqno: 1, cid: 12, name: 'expiresAt' },
      { seqno: 2, cid: 0, name: 'id' },
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

  it('persists enqueued jobs across reopen', async () => {
    const path = filename()
    const { db, storage } = open(path)
    const job = await storage.enqueue(input)
    db.close()
    const reopened = open(path)
    expect((await reopened.storage.claim(claimInput))[0]!.id).toBe(job.id)
    expect(reopened.db.open).toBe(true)
  })

  it('supports repeated initialization on the same connection', async () => {
    const { db, storage } = open()
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
    db.exec('ROLLBACK; UPDATE walq_schema SET version = 4')
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
        (id, queue, name, data, status, createdAt, availableAt, finishedAt,
         attemptsMade, attempts, error)
      VALUES ('overflow', 'email', 'send', '{}', 'failed', 0, 0, 0, @max, @max, NULL)
    `).run({ max })
    await expect(storage.retry({ queue: 'email', id: 'overflow', now: 1 })).rejects.toThrow(
      'safe integer range',
    )
    expect(
      db
        .prepare("SELECT status, attemptsMade, attempts FROM walq_jobs WHERE id = 'overflow'")
        .get(),
    ).toEqual({ status: 'failed', attemptsMade: max, attempts: max })
  })

  it('keeps lifecycle reads and mutations scoped to the exact queue', async () => {
    const { storage } = open()
    const job = await storage.enqueue({ ...input, queue: 'email', attempts: 1 })
    await expect(storage.inspect({ queue: 'other', id: job.id })).resolves.toBeNull()
    await expect(storage.list({ queue: 'other', status: 'pending', limit: 10 })).resolves.toEqual(
      [],
    )
    expect(await storage.cancel({ queue: 'other', id: job.id, now: 11 })).toBe(false)
    expect(await storage.reschedule({ queue: 'other', id: job.id, availableAt: 20 })).toBe(false)
    expect(await storage.remove({ queue: 'other', id: job.id })).toBe(false)
    expect(await storage.inspect({ queue: 'email', id: job.id })).toMatchObject({
      status: 'pending',
    })
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
