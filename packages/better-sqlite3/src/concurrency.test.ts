import type { ClaimedJob, StoredJob } from '@walq/core/storage'
import { describe, expect, it } from 'vitest'

import { claimInput, filename, input, open, race, setupCleanup } from './fixtures/storage.js'
import { betterSqlite3 } from './index.js'

setupCleanup()

describe('SQLite concurrency', () => {
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

  it('yields between committed claim chunks and observes an intervening pause', async () => {
    const { db, storage } = open()
    await storage.enqueue({ ...input, queue: 'a' })
    await storage.enqueue({ ...input, queue: 'b' })
    let observed: { transaction: boolean; statuses: unknown } | undefined
    const turn = new Promise<void>((resolve, reject) => {
      setImmediate(() => {
        observed = {
          transaction: db.inTransaction,
          statuses: db.prepare('SELECT queue, status FROM walq_jobs ORDER BY queue').all(),
        }
        storage.pause({ queue: 'b' }).then(resolve, reject)
      })
    })
    const results = await storage.claimQueues!({
      requests: ['a', 'b'].map((queue) => ({ ...claimInput, queue, limit: 512 })),
    })
    await turn

    expect(observed).toEqual({
      transaction: false,
      statuses: [
        { queue: 'a', status: 'active' },
        { queue: 'b', status: 'pending' },
      ],
    })
    expect(results.map((jobs) => jobs.length)).toEqual([1, 0])
  })

  it('does not yield when claims fit in one transaction', async () => {
    const { storage } = open()
    await storage.enqueue(input)
    let yielded = false
    const turn = new Promise<void>((resolve) => {
      setImmediate(() => {
        yielded = true
        resolve()
      })
    })

    const results = await storage.claimQueues!({ requests: [claimInput] })
    expect(results[0]).toHaveLength(1)
    expect(yielded).toBe(false)
    await turn
  })

  it('snapshots validated requests before yielding between chunks', async () => {
    const { storage } = open()
    await storage.enqueue({ ...input, queue: 'a' })
    await storage.enqueue({ ...input, queue: 'b' })
    const requests = ['a', 'b'].map((queue) => ({ ...claimInput, queue, limit: 512 }))
    const second = requests[1]!
    const turn = new Promise<void>((resolve) => {
      setImmediate(() => {
        second.queue = 'changed'
        second.limit = 0
        second.now = -1
        resolve()
      })
    })
    const results = await storage.claimQueues!({ requests })
    await turn

    expect(results.map((jobs) => jobs.map(({ queue }) => queue))).toEqual([['a'], ['b']])
  })

  it('rejects a caller transaction started between chunks without joining it', async () => {
    const { db, storage } = open()
    await storage.enqueue({ ...input, queue: 'a' })
    await storage.enqueue({ ...input, queue: 'b' })
    const turn = new Promise<void>((resolve) => {
      setImmediate(() => {
        db.exec('BEGIN')
        resolve()
      })
    })

    try {
      await expect(
        storage.claimQueues!({
          requests: ['a', 'b'].map((queue) => ({ ...claimInput, queue, limit: 512 })),
        }),
      ).rejects.toThrow('transaction')
      await turn
      expect(db.inTransaction).toBe(true)
      expect(db.prepare('SELECT queue, status FROM walq_jobs ORDER BY queue').all()).toEqual([
        { queue: 'a', status: 'active' },
        { queue: 'b', status: 'pending' },
      ])
    } finally {
      if (db.inTransaction) db.exec('ROLLBACK')
    }
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
