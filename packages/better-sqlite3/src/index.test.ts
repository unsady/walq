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
  maxAttempts: 2,
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

async function race(path: string, operations: { method: string; input: object }[]) {
  const gate = new SharedArrayBuffer(4)
  const workers: Worker[] = []
  const tasks = operations.map(
    (operation) =>
      new Promise<ClaimedJob[] | LeaseMutationResult>((resolve, reject) => {
        const worker = new Worker(new URL('./fixtures/claim-worker.ts', import.meta.url), {
          workerData: { path, gate, count: operations.length, ...operation },
        })
        workers.push(worker)
        worker.on(
          'message',
          (message: { ready: true } | { result: ClaimedJob[] | LeaseMutationResult }) => {
            if ('result' in message) resolve(message.result)
            else {
              Atomics.add(new Int32Array(gate), 0, 1)
              if (Atomics.load(new Int32Array(gate), 0) === operations.length)
                Atomics.notify(new Int32Array(gate), 0)
            }
          },
        )
        worker.on('error', reject)
        worker.on('exit', (code) => {
          if (code !== 0) reject(new Error(`Worker exited: ${code}`))
        })
      }),
  )
  try {
    return await Promise.all(tasks)
  } finally {
    await Promise.all(workers.map((worker) => worker.terminate()))
  }
}

afterEach(() => {
  for (const db of databases.splice(0)) if (db.open) db.close()
  for (const directory of directories.splice(0)) rmSync(directory, { recursive: true, force: true })
})

describe('storage contract', () => {
  it('enqueues independent snapshots and claims due jobs in binary order', async () => {
    const { storage } = open()
    const a = await storage.enqueue(input)
    const b = await storage.enqueue(input)
    expect(a).toMatchObject({ status: 'pending', attempts: 0, createdAt: 10, error: null })
    expect(a.id).not.toBe(b.id)
    await storage.enqueue({ ...input, availableAt: 11 })
    await storage.enqueue({ ...input, queue: 'Email' })
    const early = await storage.enqueue({ ...input, availableAt: 0 })
    const jobs = await storage.claim(claimInput)
    expect(jobs.map((job) => job.id)).toEqual([early.id, ...[a.id, b.id].sort()])
    expect(jobs.every((job) => job.attempts === 1 && job.expiresAt === 30)).toBe(true)
    expect(a.attempts).toBe(0)
    expect(await storage.claim(claimInput)).toEqual([])
  })

  it('checks live tokens and makes completion terminal', async () => {
    const { storage } = open()
    await storage.enqueue(input)
    const [job] = await storage.claim(claimInput)
    const credentials = { id: job!.id, leaseToken: job!.leaseToken, now: 11 }
    expect(await storage.complete({ ...credentials, leaseToken: 'wrong' })).toBe('lease_lost')
    expect(await storage.complete({ ...credentials, id: 'missing' })).toBe('lease_lost')
    expect(await storage.complete(credentials)).toBe('applied')
    expect(await storage.complete(credentials)).toBe('lease_lost')
    expect(await storage.claim({ ...claimInput, now: 100 })).toEqual([])
  })

  it('records retries, preserves errors and enforces the attempt limit', async () => {
    const { db, storage } = open()
    await storage.enqueue(input)
    const [first] = await storage.claim(claimInput)
    expect(await storage.fail({ ...first!, now: 11, error: 'retry', retryAt: 15 })).toBe('applied')
    expect(await storage.claim({ ...claimInput, now: 14 })).toEqual([])
    const [second] = await storage.claim({ ...claimInput, now: 15 })
    expect(second).toMatchObject({ attempts: 2, error: 'retry', availableAt: 15 })
    expect(second!.leaseToken).not.toBe(first!.leaseToken)
    expect(await storage.complete({ ...first!, now: 16 })).toBe('lease_lost')
    await storage.fail({ ...second!, now: 16, error: 'final', retryAt: 16 })
    expect(db.prepare('SELECT status, error, leaseToken FROM walq_jobs').get()).toEqual({
      status: 'failed',
      error: 'final',
      leaseToken: null,
    })
  })

  it('supports terminal failure and completion preserving the last error', async () => {
    const { db, storage } = open()
    await storage.enqueue(input)
    let [job] = await storage.claim(claimInput)
    await storage.fail({ ...job!, now: 11, error: 'previous', retryAt: 0 })
    ;[job] = await storage.claim({ ...claimInput, now: 11 })
    await storage.complete({ ...job!, now: 12 })
    expect(db.prepare('SELECT status, error FROM walq_jobs').get()).toEqual({
      status: 'completed',
      error: 'previous',
    })
    await storage.enqueue(input)
    ;[job] = await storage.claim(claimInput)
    await storage.fail({ ...job!, now: 11, error: '', retryAt: null })
    expect(await storage.claim({ ...claimInput, now: 100 })).toEqual([])
  })

  it('recovers all expired leases independently of the claim limit and only in its queue', async () => {
    const { db, storage } = open()
    for (let i = 0; i < 3; i++) await storage.enqueue({ ...input, maxAttempts: 1 })
    await storage.enqueue({ ...input, queue: 'other', maxAttempts: 1 })
    const jobs = await storage.claim(claimInput)
    await storage.claim({ ...claimInput, queue: 'other' })
    for (const job of jobs) {
      expect(await storage.complete({ ...job, now: 30 })).toBe('lease_lost')
      expect(await storage.fail({ ...job, now: 30, error: 'late', retryAt: null })).toBe(
        'lease_lost',
      )
      expect(await storage.heartbeat({ ...job, now: 30, leaseDuration: 20 })).toBe('lease_lost')
    }
    expect(await storage.claim({ ...claimInput, now: 30, limit: 1 })).toEqual([])
    expect(
      db.prepare("SELECT count(*) AS count FROM walq_jobs WHERE status = 'failed'").get(),
    ).toEqual({ count: 3 })
    expect(db.prepare("SELECT status FROM walq_jobs WHERE queue = 'other'").get()).toEqual({
      status: 'active',
    })
  })

  it('reclaims at expiry with a fresh token and expiry-based availability', async () => {
    const { storage } = open()
    await storage.enqueue(input)
    const [first] = await storage.claim(claimInput)
    const [second] = await storage.claim({ ...claimInput, now: 35 })
    expect(second).toMatchObject({ id: first!.id, availableAt: 30, attempts: 2, error: null })
    expect(second!.leaseToken).not.toBe(first!.leaseToken)
  })

  it('extends but never shortens a lease', async () => {
    const { db, storage } = open()
    await storage.enqueue(input)
    const [job] = await storage.claim(claimInput)
    expect(await storage.heartbeat({ ...job!, now: 11, leaseDuration: 1 })).toBe('applied')
    expect(db.prepare('SELECT expiresAt FROM walq_jobs').get()).toEqual({ expiresAt: 30 })
    expect(await storage.heartbeat({ ...job!, now: 20, leaseDuration: 30 })).toBe('applied')
    expect(await storage.claim({ ...claimInput, now: 30 })).toEqual([])
    expect(await storage.complete({ ...job!, now: 49 })).toBe('applied')
  })

  it('rejects invalid inputs before mutation, including recovery', async () => {
    const { db, storage } = open()
    for (const patch of [
      { now: -1 },
      { maxAttempts: 0 },
      { payload: 'undefined' },
      { queue: '' },
      { availableAt: Infinity },
    ]) {
      await expect(storage.enqueue({ ...input, ...patch })).rejects.toThrow(/must be|JSON/)
    }
    await storage.enqueue(input)
    const [job] = await storage.claim(claimInput)
    for (const patch of [
      { limit: 0 },
      { leaseDuration: 0 },
      { now: Number.MAX_SAFE_INTEGER },
      { now: 1.5 },
    ]) {
      await expect(storage.claim({ ...claimInput, now: 30, ...patch })).rejects.toThrow(
        'safe integer',
      )
    }
    await expect(storage.fail({ ...job!, now: 11, retryAt: -1, error: 'bad' })).rejects.toThrow(
      'retryAt',
    )
    await expect(
      storage.heartbeat({ ...job!, now: 11, leaseDuration: Number.MAX_SAFE_INTEGER }),
    ).rejects.toThrow('expiresAt')
    await expect(storage.complete({ ...job!, now: NaN })).rejects.toThrow('now')
    expect(db.prepare('SELECT status, attempts, expiresAt FROM walq_jobs').get()).toEqual({
      status: 'active',
      attempts: 1,
      expiresAt: 30,
    })
  })
})

describe('SQLite integration', () => {
  it('persists jobs, supports repeated initialization and leaves connection settings alone', async () => {
    const path = filename()
    const { db, storage } = open(path)
    const journal = db.pragma('journal_mode', { simple: true })
    betterSqlite3(db)
    expect(db.pragma('journal_mode', { simple: true })).toBe(journal)
    const job = await storage.enqueue(input)
    db.close()
    const reopened = open(path)
    expect((await reopened.storage.claim(claimInput))[0]!.id).toBe(job.id)
    expect(reopened.db.open).toBe(true)
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
    expect(db.prepare('SELECT status, attempts, expiresAt FROM walq_jobs').get()).toEqual({
      status: 'active',
      attempts: 1,
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
        state: db.prepare('SELECT status, attempts FROM walq_jobs').get(),
      }
      expect([
        {
          mutation: 'applied',
          ids: [],
          state: { status: method === 'complete' ? 'completed' : 'active', attempts: 1 },
        },
        { mutation: 'lease_lost', ids: [job!.id], state: { status: 'active', attempts: 2 } },
      ]).toContainEqual(outcome)
      expect(jobs.every((item) => item.leaseToken !== job!.leaseToken)).toBe(true)
    },
  )

  it('never issues duplicate live leases to concurrent workers', async () => {
    const path = filename()
    const { db, storage } = open(path)
    db.pragma('journal_mode = WAL')
    for (let i = 0; i < 40; i++) await storage.enqueue(input)
    const results = await race(
      path,
      Array.from({ length: 4 }, () => ({ method: 'claim', input: claimInput })),
    )
    const ids = (results as ClaimedJob[][]).flat().map((job) => job.id)
    expect(ids).toHaveLength(40)
    expect(new Set(ids).size).toBe(40)
  })
})
