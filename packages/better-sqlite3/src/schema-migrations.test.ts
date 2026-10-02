import { DatabaseSync as NativeDatabase } from 'node:sqlite'

import { sqlite } from '@walq/sqlite'
import BetterDatabase from 'better-sqlite3'
import { afterEach, describe, expect, it } from 'vitest'

import {
  claimInput,
  expectPartialDedupeIndex,
  filename,
  input,
  revertGroupScheduling,
  setupCleanup,
} from './fixtures/storage.js'
import { betterSqlite3 as createBetterStorage } from './index.js'

setupCleanup()

describe.each([
  { name: 'better-sqlite3', Database: BetterDatabase },
  { name: 'node:sqlite', Database: NativeDatabase },
])('$name schema', ({ Database }) => {
  const databases: (BetterDatabase.Database | NativeDatabase)[] = []

  afterEach(() => {
    for (const db of databases.splice(0)) {
      if (db instanceof NativeDatabase ? db.isOpen : db.open) db.close()
    }
  })

  function createStorage(db: BetterDatabase.Database | NativeDatabase) {
    return db instanceof NativeDatabase ? sqlite(db) : createBetterStorage(db)
  }

  function journalMode(db: BetterDatabase.Database | NativeDatabase): unknown {
    const row = db.prepare('PRAGMA journal_mode').get() as Record<string, unknown>

    return Object.values(row)[0]
  }

  function open(path = ':memory:') {
    const db = new Database(path)
    databases.push(db)

    return { db, storage: createStorage(db) }
  }

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

  describe('SQLite schema migrations', () => {
    it('migrates v2 jobs, leases, and indexes through v12', async () => {
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

      const storage = createStorage(db)
      expect(db.prepare('SELECT version FROM walq_schema').get()).toEqual({ version: 12 })
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

    it('migrates v3 jobs through v12 with zero priority', async () => {
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

      const storage = createStorage(db)
      expect(db.prepare('SELECT version FROM walq_schema').get()).toEqual({ version: 12 })
      expectPartialDedupeIndex(db)
      expect(db.prepare('SELECT priority FROM walq_jobs WHERE id = ?').get('legacy')).toEqual({
        priority: 0,
      })

      await storage.enqueue({ ...input, priority: 10 })
      expect((await storage.claim(claimInput)).map(({ priority }) => priority)).toEqual([10, 0])
    })

    it('migrates schema v4 through v12 and preserves existing jobs', async () => {
      const { db } = open()
      const legacy = await createStorage(db).enqueue(input)
      revertGroupScheduling(db)
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

      const storage = createStorage(db)
      expect(db.prepare('SELECT version FROM walq_schema').get()).toEqual({ version: 12 })
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
      revertGroupScheduling(db)
      db.exec(`
      DROP INDEX walq_active_group;
      DROP INDEX walq_schedules_due;
      DROP TABLE walq_schedules;
      DROP TABLE walq_paused_queues;
      DROP TABLE walq_groups;
      ALTER TABLE walq_jobs DROP COLUMN groupId;
      UPDATE walq_schema SET version = 6;
    `)

      const migrated = createStorage(db)
      expect(db.prepare('SELECT version FROM walq_schema').get()).toEqual({ version: 12 })
      expect(db.prepare('SELECT groupId FROM walq_jobs WHERE id = ?').get(legacy.id)).toEqual({
        groupId: null,
      })
      expect(await migrated.inspect({ queue: input.queue, id: legacy.id })).toMatchObject({
        id: legacy.id,
        data: input.data,
      })
      await migrated.enqueue({ ...input, group: { id: 'new-group', concurrency: 2 } })
    })

    it('migrates the v7 group schema and removes the obsolete active-group index', async () => {
      const { db, storage } = open()
      const grouped = await storage.enqueue({ ...input, group: { id: 'v7-group', concurrency: 2 } })
      await storage.claim({ ...claimInput, limit: 1 })
      revertGroupScheduling(db)
      db.exec(
        'DROP INDEX walq_active_group; DROP INDEX walq_schedules_due; DROP TABLE walq_schedules; DROP TABLE walq_paused_queues; UPDATE walq_schema SET version = 7',
      )

      const migrated = createStorage(db)
      expect(db.prepare('SELECT version FROM walq_schema').get()).toEqual({ version: 12 })
      expect(
        db
          .prepare(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'walq_active_group'",
          )
          .get(),
      ).toBeUndefined()
      expect(await migrated.inspect({ queue: input.queue, id: grouped.id })).toMatchObject({
        id: grouped.id,
      })
    })

    it('migrates the v8 schema through v12 with schedules and queue pause state', async () => {
      const { db, storage } = open()
      const job = await storage.enqueue(input)
      revertGroupScheduling(db)
      db.exec(
        `DROP INDEX walq_schedules_due; DROP TABLE walq_schedules; DROP TABLE walq_paused_queues; UPDATE walq_schema SET version = 8`,
      )

      const migrated = createStorage(db)
      expect(db.prepare('SELECT version FROM walq_schema').get()).toEqual({ version: 12 })
      expect(await migrated.inspect({ queue: input.queue, id: job.id })).toMatchObject({
        id: job.id,
        data: input.data,
      })
      await expect(migrated.getSchedule!({ queue: 'email', id: 'not-created' })).resolves.toBeNull()
    })

    it('migrates the v9 schema by adding durable queue pause state', async () => {
      const { db, storage } = open()
      const job = await storage.enqueue(input)
      revertGroupScheduling(db)
      db.exec('DROP TABLE walq_paused_queues; UPDATE walq_schema SET version = 9')

      const migrated = createStorage(db)
      expect(db.prepare('SELECT version FROM walq_schema').get()).toEqual({ version: 12 })
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

    it('migrates v10 pending groups, backfills counts and drops the old active index', async () => {
      const { db, storage } = open()
      const jobs = await storage.enqueueMany(
        Array.from({ length: 2 }, () => ({
          ...input,
          group: { id: 'backfill', concurrency: 1 },
        })),
      )
      const [active] = await storage.claim({ ...claimInput, limit: 1 })
      revertGroupScheduling(db)
      db.exec('UPDATE walq_schema SET version = 10')

      const migrated = createStorage(db)
      expect(db.prepare('SELECT version FROM walq_schema').get()).toEqual({ version: 12 })
      expect(
        db.prepare("SELECT 1 FROM sqlite_master WHERE name = 'walq_active_group'").get(),
      ).toBeUndefined()
      expect(
        db.prepare("SELECT pendingCount, activeCount FROM walq_groups WHERE id = 'backfill'").get(),
      ).toEqual({
        pendingCount: 1,
        activeCount: 1,
      })
      expect(await migrated.claim(claimInput)).toEqual([])
      expect(await migrated.complete({ ...active!, now: input.now })).toBe('applied')
      expect((await migrated.claim(claimInput)).map(({ id }) => id)).toEqual([jobs[1]!.id])
      expect(
        db.prepare("SELECT pendingCount, activeCount FROM walq_groups WHERE id = 'backfill'").get(),
      ).toEqual({
        pendingCount: 0,
        activeCount: 1,
      })
    })
  })

  describe('SQLite schema and ordering', () => {
    it('migrates v5 rows through v12 with deterministic approximate enqueue order', async () => {
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

      const storage = createStorage(db)

      expect(db.prepare('SELECT version FROM walq_schema').get()).toEqual({ version: 12 })
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
        WHERE queue = 'email' AND status = 'pending' AND groupId IS NULL
          AND availableAt <= 10 AND attemptsMade < attempts
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
      expect(reopened.db instanceof NativeDatabase ? reopened.db.isOpen : reopened.db.open).toBe(
        true,
      )
    })

    it('supports repeated initialization on the same connection', async () => {
      const { db, storage } = open()
      expectPartialDedupeIndex(db)
      createStorage(db)
      createStorage(db)
      await storage.enqueue(input)
      expect(await storage.claim(claimInput)).toHaveLength(1)
    })

    it('leaves connection settings alone', async () => {
      const { db } = open()
      const journal = journalMode(db)
      createStorage(db)
      expect(journalMode(db)).toBe(journal)
    })

    it('rejects unsupported schema versions and external transactions', async () => {
      const { db, storage } = open()
      db.exec('BEGIN')
      expect(() => createStorage(db)).toThrow('transaction')
      await expect(storage.enqueue(input)).rejects.toThrow('transaction')
      await expect(storage.enqueueMany([input])).rejects.toThrow('transaction')
      db.exec('ROLLBACK; UPDATE walq_schema SET version = 13')
      expect(() => createStorage(db)).toThrow('version')
    })

    it('rejects a version 1 database instead of migrating it', () => {
      const db = new Database(':memory:')
      databases.push(db)
      db.exec(`
      CREATE TABLE walq_schema (id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL);
      INSERT INTO walq_schema (id, version) VALUES (1, 1);
    `)
      expect(() => createStorage(db)).toThrow('Unsupported walq schema version: 1')
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
  })
})
