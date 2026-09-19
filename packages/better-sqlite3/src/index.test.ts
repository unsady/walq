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
  payload: '{"to":"a"}',
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
        new Promise<ClaimedJob[] | LeaseMutationResult>((resolve, reject) => {
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
            (message: { ready: true } | { result: ClaimedJob[] | LeaseMutationResult }) => {
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

describe('SQLite integration', () => {
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
    db.exec('ROLLBACK; UPDATE walq_schema SET version = 2')
    expect(() => betterSqlite3(db)).toThrow('version')
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
