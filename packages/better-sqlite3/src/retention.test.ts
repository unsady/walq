import { describe, expect, it } from 'vitest'

import { claimInput, input, open, setupCleanup } from './fixtures/storage.js'

setupCleanup()

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
