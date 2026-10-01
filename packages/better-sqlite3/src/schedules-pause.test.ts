import { describe, expect, it } from 'vitest'

import { claimInput, filename, input, open, setupCleanup } from './fixtures/storage.js'

setupCleanup()

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
})
