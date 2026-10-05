import Database from 'better-sqlite3'
import { describe, expect, it } from 'vitest'

import { claimInput, databases, filename, input, open, setupCleanup } from './fixtures/storage.js'
import { betterSqlite3 } from './index.js'

setupCleanup()

describe('SQLite groups', () => {
  it('validates groups and rejects conflicting concurrency without mutation', async () => {
    const { storage } = open()

    for (const group of [
      { id: '', concurrency: 1 },
      { id: 'g', concurrency: 0 },
    ]) {
      await expect(storage.enqueue({ ...input, group } as never)).rejects.toThrow(TypeError)
    }
    expect(await storage.count({ queue: input.queue })).toMatchObject({ pending: 0 })

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
    expect(await storage.count({ queue: input.queue })).toMatchObject({ pending: 1 })
    // The failed batch must not leave its first group's configuration behind.
    await expect(
      storage.enqueue({ ...input, group: { id: 'batch-conflict', concurrency: 2 } }),
    ).resolves.toBeDefined()
  })

  it('deduplicates grouped jobs without changing membership and keeps group config stable', async () => {
    const { storage } = open()
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
    // A duplicate must not establish the supplied, otherwise-unused group.
    await expect(
      storage.enqueue({ ...input, group: { id: 'unused', concurrency: 1 } }),
    ).resolves.toBeDefined()
    await expect(
      storage.enqueue({ ...input, dedupe: 'same', group: { id: 'g', concurrency: 1 } }),
    ).rejects.toThrow('already uses concurrency 2')

    await storage.remove({ queue: input.queue, id: first.id })
    await expect(storage.enqueue({ ...input, group: { id: 'g', concurrency: 1 } })).rejects.toThrow(
      'already uses concurrency 2',
    )
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
      ungrouped.id,
      otherGroup.id,
      first.id,
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
    const { storage } = open()
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
    expect(await storage.count({ queue: input.queue })).toMatchObject({ pending: groupCount })
    expect(blocked).toHaveLength(groupCount)
  })

  it('round-robins groups across calls and connections, with ungrouped jobs first', async () => {
    const path = filename()
    const { storage } = open(path)
    const grouped = new Map<string, string[]>()

    for (const id of ['a', 'b', 'c']) {
      const jobs = await storage.enqueueMany(
        Array.from({ length: id === 'c' ? 2 : 3 }, (_, index) => ({
          ...input,
          priority: 100 - index,
          group: { id, concurrency: 3 },
        })),
      )
      grouped.set(
        id,
        jobs.map((job) => job.id),
      )
    }
    const plain = await storage.enqueue({ ...input, priority: -1 })
    const other = new Database(path)
    databases.push(other)
    const otherStorage = betterSqlite3(other)

    expect((await storage.claim({ ...claimInput, limit: 1 })).map(({ id }) => id)).toEqual([
      plain.id,
    ])

    const order = ['a', 'b', 'c', 'a', 'b', 'c', 'a', 'b']
    const positions = new Map<string, number>()
    for (const [index, groupId] of order.entries()) {
      const adapter = index % 2 === 0 ? storage : otherStorage
      const [claimed] = await adapter.claim({ ...claimInput, limit: 1 })
      expect(claimed?.id).toBe(grouped.get(groupId)?.[positions.get(groupId) ?? 0])
      positions.set(groupId, (positions.get(groupId) ?? 0) + 1)
    }

    expect(await otherStorage.claim(claimInput)).toEqual([])
  })

  it('keeps claiming groups under a continuous backlog of ungrouped jobs', async () => {
    const { storage } = open()
    const plain = await storage.enqueueMany(
      Array.from({ length: 8 }, () => ({ ...input, priority: 100 })),
    )
    const groups = new Map<string, string[]>()
    for (const id of ['a', 'b']) {
      const jobs = await storage.enqueueMany(
        Array.from({ length: 3 }, () => ({
          ...input,
          priority: -100,
          group: { id, concurrency: 3 },
        })),
      )
      groups.set(
        id,
        jobs.map((job) => job.id),
      )
    }

    const expected = [
      plain[0]!.id,
      groups.get('a')![0]!,
      groups.get('b')![0]!,
      plain[1]!.id,
      groups.get('a')![1]!,
      groups.get('b')![1]!,
    ]
    for (const id of expected) {
      expect((await storage.claim({ ...claimInput, limit: 1 }))[0]?.id).toBe(id)
    }

    expect((await storage.claim({ ...claimInput, limit: 4 })).map(({ id }) => id)).toEqual([
      plain[2]!.id,
      groups.get('a')![2]!,
      groups.get('b')![2]!,
      plain[3]!.id,
    ])
    expect((await storage.claim({ ...claimInput, limit: 1 }))[0]?.id).toBe(plain[4]!.id)
  })

  it('validates duplicate inputs without registering an unused group', async () => {
    const { storage } = open()
    const original = await storage.enqueue({ ...input, dedupe: 'same' })

    await expect(
      storage.enqueue({ ...input, data: 'invalid JSON', dedupe: 'same' }),
    ).rejects.toThrow(SyntaxError)
    await expect(storage.enqueue({ ...input, attempts: 0, dedupe: 'same' })).rejects.toThrow(
      'attempts',
    )
    await expect(
      storage.enqueue({ ...input, dedupe: 'same', group: { id: 'unused', concurrency: 0 } }),
    ).rejects.toThrow('group.concurrency')

    const duplicate = await storage.enqueue({
      ...input,
      dedupe: 'same',
      group: { id: 'unused', concurrency: 2 },
    })
    expect(duplicate).toEqual(original)
    await expect(
      storage.enqueue({ ...input, group: { id: 'unused', concurrency: 1 } }),
    ).resolves.toBeDefined()
  })

  it('validates every batch input before inserting even when its dedupe key already exists', async () => {
    const { storage } = open()
    await storage.enqueue({ ...input, dedupe: 'same' })

    await expect(
      storage.enqueueMany([
        { ...input, dedupe: 'new' },
        { ...input, dedupe: 'same', attempts: 0 },
      ]),
    ).rejects.toThrow('attempts')
    expect(await storage.count({ queue: input.queue })).toMatchObject({ pending: 1 })
  })

  it('round-robins inside a multi-job claim and honors priority within each group', async () => {
    const { storage } = open()
    const a = await storage.enqueueMany([
      { ...input, priority: 1, group: { id: 'a', concurrency: 2 } },
      { ...input, priority: 10, group: { id: 'a', concurrency: 2 } },
    ])
    const b = await storage.enqueueMany([
      { ...input, priority: -1, group: { id: 'b', concurrency: 2 } },
      { ...input, priority: 0, group: { id: 'b', concurrency: 2 } },
    ])

    expect((await storage.claim({ ...claimInput, limit: 4 })).map(({ id }) => id)).toEqual([
      a[1]!.id,
      b[1]!.id,
      a[0]!.id,
      b[0]!.id,
    ])
  })

  it('skips saturated groups without advancing their turn', async () => {
    const { storage } = open()
    const [first, blocked] = await storage.enqueueMany([
      { ...input, group: { id: 'a', concurrency: 1 } },
      { ...input, group: { id: 'a', concurrency: 1 } },
    ])
    const [available] = await storage.enqueueMany([
      { ...input, group: { id: 'b', concurrency: 1 } },
    ])

    const [active] = await storage.claim({ ...claimInput, limit: 1 })
    expect(active?.id).toBe(first!.id)
    expect((await storage.claim({ ...claimInput, limit: 2 })).map(({ id }) => id)).toEqual([
      available!.id,
    ])
    expect(await storage.claim(claimInput)).toEqual([])

    expect(await storage.complete({ ...active!, now: input.now })).toBe('applied')
    expect((await storage.claim(claimInput)).map(({ id }) => id)).toEqual([blocked!.id])
  })

  it('keeps the round-robin turn after batching ungrouped jobs past a full group', async () => {
    const { storage } = open()
    const [first, blocked] = await storage.enqueueMany(
      Array.from({ length: 2 }, () => ({
        ...input,
        group: { id: 'full', concurrency: 1 },
      })),
    )
    const [active] = await storage.claim({ ...claimInput, limit: 1 })
    expect(active?.id).toBe(first!.id)

    const plain = await storage.enqueueMany(Array.from({ length: 2 }, () => ({ ...input })))
    expect((await storage.claim({ ...claimInput, limit: 2 })).map(({ id }) => id)).toEqual(
      plain.map(({ id }) => id),
    )

    expect(await storage.complete({ ...active!, now: input.now })).toBe('applied')
    const later = await storage.enqueue(input)
    expect((await storage.claim({ ...claimInput, limit: 1 }))[0]?.id).toBe(blocked!.id)
    expect((await storage.claim({ ...claimInput, limit: 1 }))[0]?.id).toBe(later.id)
  })

  it('does not advance the turn for groups whose jobs are not yet due', async () => {
    const { storage } = open()
    const due = await storage.enqueue({ ...input, group: { id: 'a', concurrency: 1 } })
    const later = await storage.enqueue({
      ...input,
      availableAt: 20,
      group: { id: 'b', concurrency: 1 },
    })

    expect((await storage.claim({ ...claimInput, limit: 1 }))[0]?.id).toBe(due.id)
    expect(await storage.claim({ ...claimInput, limit: 1 })).toEqual([])
    expect((await storage.claim({ ...claimInput, now: 20, limit: 1 }))[0]?.id).toBe(later.id)
  })

  it('skips future-only groups without losing later claims when now moves backward', async () => {
    const { storage } = open()
    await storage.enqueueMany(
      Array.from({ length: 100 }, (_, index) => ({
        ...input,
        availableAt: 20,
        group: { id: `future-${index}`, concurrency: 1 },
      })),
    )
    const ordinary = await storage.enqueueMany(Array.from({ length: 2 }, () => ({ ...input })))

    expect((await storage.claim({ ...claimInput, limit: 2 })).map(({ id }) => id)).toEqual(
      ordinary.map(({ id }) => id),
    )
    expect(await storage.claim({ ...claimInput, now: 19, limit: 1 })).toEqual([])

    const [first] = await storage.claim({ ...claimInput, now: 20, limit: 1 })
    expect(first).toBeDefined()
    expect(await storage.claim({ ...claimInput, now: 19, limit: 1 })).toEqual([])
    const [second] = await storage.claim({ ...claimInput, now: 20, limit: 1 })
    expect(second?.id).not.toBe(first?.id)
  })

  it('jumps over future-only groups to a ready group without changing turn order', async () => {
    const { storage } = open()
    await storage.enqueueMany(
      Array.from({ length: 100 }, (_, index) => ({
        ...input,
        availableAt: 20,
        group: { id: `future-${index}`, concurrency: 1 },
      })),
    )
    const ready = await storage.enqueue({ ...input, group: { id: 'zz-ready', concurrency: 1 } })
    const ordinary = await storage.enqueue(input)

    expect((await storage.claim({ ...claimInput, limit: 1 }))[0]?.id).toBe(ordinary.id)
    expect((await storage.claim({ ...claimInput, limit: 1 }))[0]?.id).toBe(ready.id)
    expect(await storage.claim({ ...claimInput, limit: 1 })).toEqual([])
  })

  it('claims ready work past future-only and saturated groups', async () => {
    const { storage } = open()
    await storage.enqueueMany(
      Array.from({ length: 8 }, (_, index) => ({
        ...input,
        availableAt: 20,
        group: { id: `future-${index}`, concurrency: 1 },
      })),
    )
    const blocked = await storage.enqueue({ ...input, group: { id: 'z-blocked', concurrency: 1 } })
    await storage.enqueueMany(
      Array.from({ length: 100 }, () => ({
        ...input,
        group: { id: 'z-blocked', concurrency: 1 },
      })),
    )
    expect((await storage.claim({ ...claimInput, limit: 1 }))[0]?.id).toBe(blocked.id)
    const ordinary = await storage.enqueue(input)
    expect((await storage.claim({ ...claimInput, limit: 1 }))[0]?.id).toBe(ordinary.id)

    const ready = await storage.enqueue({ ...input, group: { id: 'zz-ready', concurrency: 1 } })
    expect((await storage.claim({ ...claimInput, limit: 1 }))[0]?.id).toBe(ready.id)
    expect(await storage.claim({ ...claimInput, limit: 1 })).toEqual([])
  })

  it('releases capacity in multiple saturated groups behind future-only groups', async () => {
    const { storage } = open()
    await storage.enqueueMany(
      Array.from({ length: 8 }, (_, index) => ({
        ...input,
        availableAt: 20,
        group: { id: `future-${index}`, concurrency: 1 },
      })),
    )

    const active = []
    const blocked = []
    for (const id of ['group-a', 'group-b']) {
      const jobs = await storage.enqueueMany(
        Array.from({ length: 2 }, () => ({ ...input, group: { id, concurrency: 1 } })),
      )
      active.push(jobs[0]!)
      blocked.push(jobs[1]!)
    }
    const claimed = await storage.claim({ ...claimInput, limit: 2 })
    expect(claimed.map(({ id }) => id)).toEqual(active.map(({ id }) => id))

    const ordinary = await storage.enqueue(input)
    expect((await storage.claim({ ...claimInput, limit: 1 }))[0]?.id).toBe(ordinary.id)

    const ready = await storage.enqueue({ ...input, group: { id: 'zz-ready', concurrency: 1 } })
    expect((await storage.claim({ ...claimInput, limit: 1 }))[0]?.id).toBe(ready.id)
    expect(await storage.claim({ ...claimInput, limit: 1 })).toEqual([])

    await storage.complete({ ...claimed[0]!, now: input.now })
    expect((await storage.claim({ ...claimInput, limit: 1 }))[0]?.id).toBe(blocked[0]!.id)
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
})

describe('SQLite group scheduling (performance contracts)', () => {
  it('writes the round-robin cursor once for a multi-job claim', async () => {
    const { db, storage } = open()
    await storage.enqueueMany(
      ['a', 'b', 'c'].flatMap((id) =>
        Array.from({ length: 2 }, () => ({ ...input, group: { id, concurrency: 2 } })),
      ),
    )
    db.exec(`
      CREATE TABLE cursor_writes (count INTEGER NOT NULL);
      INSERT INTO cursor_writes VALUES (0);
      CREATE TRIGGER count_cursor_insert AFTER INSERT ON walq_group_cursor
      BEGIN UPDATE cursor_writes SET count = count + 1; END;
      CREATE TRIGGER count_cursor_update AFTER UPDATE ON walq_group_cursor
      BEGIN UPDATE cursor_writes SET count = count + 1; END;
    `)

    const claimed = await storage.claim({ ...claimInput, limit: 5 })

    expect(claimed).toHaveLength(5)
    expect(db.prepare('SELECT count FROM cursor_writes').get()).toEqual({ count: 1 })
  })

  it('uses separate pending indexes for grouped and ungrouped claims', () => {
    const { db } = open()
    const queries = [
      `SELECT id FROM walq_jobs INDEXED BY walq_pending
       WHERE queue = 'email' AND status = 'pending' AND groupId IS NULL
         AND availableAt <= 10 AND attemptsMade < attempts
       ORDER BY priority DESC, availableAt, seq LIMIT 10`,
      `SELECT id FROM walq_jobs INDEXED BY walq_pending_grouped
       WHERE queue = 'email' AND groupId = 'one' AND groupId IS NOT NULL
         AND status = 'pending' AND availableAt <= 10 AND attemptsMade < attempts
       ORDER BY priority DESC, availableAt, seq LIMIT 1`,
      `SELECT id FROM walq_groups INDEXED BY walq_groups_eligible
       WHERE queue = 'email' AND pendingCount > 0 AND activeCount < concurrency
         AND id > 'one' ORDER BY id LIMIT 1`,
      `SELECT groupId FROM walq_jobs INDEXED BY walq_pending_grouped
       WHERE queue = 'email' AND status = 'pending' AND groupId IS NOT NULL
         AND groupId > 'one' AND availableAt <= 10 AND attemptsMade < attempts
       ORDER BY groupId LIMIT 1`,
      `SELECT 1 FROM walq_groups INDEXED BY walq_groups_eligible
       WHERE queue = 'email' AND id = 'one'
         AND pendingCount > 0 AND activeCount < concurrency`,
    ]
    const indexes = [
      'walq_pending',
      'walq_pending_grouped',
      'walq_groups_eligible',
      'walq_pending_grouped',
      'walq_groups_eligible',
    ]
    const plans = queries.map((query) => {
      const plan = db.prepare(`EXPLAIN QUERY PLAN ${query}`).all() as { detail: string }[]

      return plan.map(({ detail }) => detail).join('; ')
    })

    for (const [index, details] of plans.entries()) {
      expect(details).toContain(indexes[index])
      expect(details).not.toContain('TEMP B-TREE')
      expect(details).not.toContain('SCAN walq_jobs')
    }
    expect(plans[3]).toContain('groupId>?')
  })
})
