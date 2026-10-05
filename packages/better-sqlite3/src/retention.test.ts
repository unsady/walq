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

  it('records finishedAt on terminal transitions and keeps retries unfinished', async () => {
    const { storage } = open()
    await storage.enqueue({ ...input, attempts: 2 })
    let [job] = await storage.claim(claimInput)
    await storage.fail({ ...job!, now: 11, error: 'retry', retryAt: 11 })
    expect(await storage.inspect({ queue: input.queue, id: job!.id })).toMatchObject({
      status: 'pending',
      finishedAt: null,
    })

    ;[job] = await storage.claim({ ...claimInput, now: 11 })
    await storage.complete({ ...job!, now: 12 })
    expect(await storage.inspect({ queue: input.queue, id: job!.id })).toMatchObject({
      status: 'completed',
      finishedAt: 12,
    })

    await storage.enqueue({ ...input, attempts: 1 })
    ;[job] = await storage.claim(claimInput)
    await storage.fail({ ...job!, now: 13, error: 'terminal', retryAt: null })
    expect(await storage.inspect({ queue: input.queue, id: job!.id })).toMatchObject({
      status: 'failed',
      finishedAt: 13,
    })
  })

  it('keeps the newest terminal rows per status and queue', async () => {
    const { storage } = open()
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

    expect(
      (await storage.list({ queue: input.queue, status: 'completed', limit: 10 })).map(
        ({ id }) => id,
      ),
    ).toEqual([completed[3], completed[2]])
    expect(
      (await storage.list({ queue: input.queue, status: 'failed', limit: 10 })).map(({ id }) => id),
    ).toEqual([failed[1]])
    expect(await storage.inspect({ queue: 'other', id: other.id })).not.toBeNull()
  })

  it('exhausts the shared budget across statuses before reporting more', async () => {
    const { storage } = open()
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

    expect(await storage.count({ queue: input.queue })).toEqual({
      pending: 0,
      active: 0,
      completed: 0,
      failed: 1,
      cancelled: 0,
    })
    expect(
      (await storage.list({ queue: input.queue, status: 'failed', limit: 10 })).map(({ id }) => id),
    ).toEqual([failed[1]])
  })

  it('breaks finish-time ties by id so the newest rows survive', async () => {
    const { storage } = open()
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

    const remaining = await storage.list({ queue: input.queue, status: 'completed', limit: 10 })
    expect(remaining.map(({ id }) => id).sort()).toEqual([...ids].sort().slice(-2))
  })

  it('combines count and age as the union of the two oldest tails', async () => {
    const { storage } = open()
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
    expect(
      (await storage.list({ queue: input.queue, status: 'completed', limit: 10 })).map(
        ({ finishedAt }) => finishedAt,
      ),
    ).toEqual([cleanupNow - 100, cleanupNow - 200])

    // A small count bound reaches further than an ineffective age bound.
    const countBound = { completed: rule(1, 10_000_000), failed: rule(0) }
    expect(await storage.cleanup({ ...cleanupInput, retention: countBound })).toEqual({
      removed: 1,
      more: false,
    })
    expect(
      (await storage.list({ queue: input.queue, status: 'completed', limit: 10 })).map(
        ({ finishedAt }) => finishedAt,
      ),
    ).toEqual([cleanupNow - 100])
  })

  it('rejects cleanup inside a caller transaction', async () => {
    const { db, storage } = open()
    db.exec('BEGIN')
    await expect(storage.cleanup(cleanupInput)).rejects.toThrow('transaction')
    db.exec('ROLLBACK')
  })
})

describe('SQLite retention (performance contract)', () => {
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
})
