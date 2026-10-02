import { DatabaseSync } from 'node:sqlite'

import { betterSqlite3 } from '@walq/better-sqlite3'
import Database from 'better-sqlite3'
import { afterEach, describe, expect, it } from 'vitest'

import {
  filename,
  input,
  claimInput,
  race,
  setupCleanup,
} from '../../better-sqlite3/src/fixtures/storage.js'
import { sqlite } from './index.js'

const connections: DatabaseSync[] = []
setupCleanup()
afterEach(() => {
  for (const db of connections.splice(0)) if (db.isOpen) db.close()
})

function open(path = ':memory:') {
  const db = new DatabaseSync(path)
  connections.push(db)
  db.exec('PRAGMA busy_timeout = 5000')

  return { db, storage: sqlite(db) }
}

describe('node:sqlite driver', () => {
  it('returns safe numeric values and ignores extra named parameters', async () => {
    const { db, storage } = open()
    const job = await storage.enqueue({ ...input, priority: Number.MAX_SAFE_INTEGER })
    const rows = await storage.list({ queue: input.queue, status: 'pending', limit: 10 })

    expect(rows[0]?.priority).toBe(Number.MAX_SAFE_INTEGER)
    expect((await storage.count({ queue: input.queue })).pending).toBe(1)
    expect((await storage.claim(claimInput))[0]?.id).toBe(job.id)
    expect(db.isTransaction).toBe(false)
  })

  it('rolls back an enqueue batch on a group configuration conflict', async () => {
    const { db, storage } = open()
    await expect(
      storage.enqueueMany([
        { ...input, group: { id: 'shared', concurrency: 1 } },
        { ...input, group: { id: 'shared', concurrency: 2 } },
      ]),
    ).rejects.toThrow('already uses concurrency')

    expect(db.isTransaction).toBe(false)
    expect((await storage.count({ queue: input.queue })).pending).toBe(0)
    expect(db.prepare('SELECT * FROM walq_groups').all()).toEqual([])
    await expect(storage.enqueue(input)).resolves.toMatchObject({ queue: input.queue })
  })

  it('rejects caller transactions without committing or rolling them back', async () => {
    const { db, storage } = open()
    db.exec('BEGIN')

    expect(() => sqlite(db)).toThrow('inside a transaction')
    await expect(storage.enqueue(input)).rejects.toThrow('inside a transaction')
    expect(db.isTransaction).toBe(true)
    db.exec('ROLLBACK')
  })

  it('leaves no partial schema after an unsupported schema version', () => {
    const db = new DatabaseSync(':memory:')
    connections.push(db)
    db.exec(
      'CREATE TABLE walq_schema (id INTEGER PRIMARY KEY, version INTEGER); INSERT INTO walq_schema VALUES (1, 11)',
    )

    expect(() => sqlite(db)).toThrow('Unsupported walq schema version: 11')
    expect(db.isTransaction).toBe(false)
    expect(db.prepare("SELECT name FROM sqlite_master WHERE type = 'table'").all()).toEqual([
      { name: 'walq_schema' },
    ])
  })

  it('rolls back failed writes on a read-only connection', async () => {
    const path = filename()
    open(path).db.close()
    const db = new DatabaseSync(path, { readOnly: true })
    connections.push(db)

    const storage = sqlite(db)
    await expect(storage.enqueue(input)).rejects.toThrow(/readonly/i)
    expect(db.isTransaction).toBe(false)
    expect((await storage.count({ queue: input.queue })).pending).toBe(0)
  })

  it.each(['builtin', 'better-sqlite3'])(
    'reopens a %s database with the other driver',
    async (first) => {
      const path = filename()
      const builtin = open(path)
      builtin.db.close()
      const db = new Database(path)
      const legacy = betterSqlite3(db)
      const native = open(path)
      const writer = first === 'builtin' ? native.storage : legacy
      const reader = first === 'builtin' ? legacy : native.storage

      try {
        const job = await writer.enqueue({ ...input, group: { id: 'shared', concurrency: 1 } })
        const [claimed] = await reader.claim(claimInput)
        expect(claimed?.id).toBe(job.id)
        expect(
          await writer.complete({
            id: job.id,
            leaseToken: claimed!.leaseToken,
            now: 11,
          }),
        ).toBe('applied')
        expect((await reader.inspect({ queue: input.queue, id: job.id }))?.status).toBe('completed')
      } finally {
        db.close()
      }
    },
  )

  it('serializes concurrent claims without duplicate live leases', async () => {
    const path = filename()
    const { storage } = open(path)
    await storage.enqueueMany(Array.from({ length: 20 }, () => ({ ...input })))

    const results = await race(
      path,
      Array.from({ length: 4 }, () => ({ method: 'claim', input: { ...claimInput, limit: 10 } })),
      'node:sqlite',
    )
    const jobs = results.flat() as { id: string }[]
    expect(jobs).toHaveLength(20)
    expect(new Set(jobs.map((job) => job.id)).size).toBe(20)
  })

  it('deduplicates concurrent inserts and enforces group limits', async () => {
    const path = filename()
    open(path)
    const results = (await race(
      path,
      Array.from({ length: 4 }, () => ({
        method: 'enqueue',
        input: { ...input, dedupe: 'same', group: { id: 'shared', concurrency: 1 } },
      })),
      'node:sqlite',
    )) as { id: string }[]
    expect(new Set(results.map((job) => job.id)).size).toBe(1)

    const claims = await race(
      path,
      Array.from({ length: 4 }, () => ({ method: 'claim', input: claimInput })),
      'node:sqlite',
    )
    expect(claims.flat()).toHaveLength(1)
  })
})
