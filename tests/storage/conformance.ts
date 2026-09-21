import { Buffer } from 'node:buffer'

import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import type {
  ClaimedJob,
  ClaimInput,
  CleanupInput,
  CleanupResult,
  EnqueueInput,
  LeaseMutationResult,
  RetentionRule,
  Storage,
} from 'walq/storage'

export type StorageFactory = () => Storage | Promise<Storage>
export type StorageCleanup = () => void | Promise<void>

const queue = 'email'
const otherQueue = 'other'
const jobName = 'send'
const data = '{"to":"a"}'
const now = 10
const leaseDuration = 20
const expiresAt = 30

type LeaseMethod = 'complete' | 'fail' | 'heartbeat'

function enqueueInput(overrides: Partial<EnqueueInput> = {}): EnqueueInput {
  return {
    queue,
    name: jobName,
    data,
    now,
    availableAt: now,
    attempts: 2,
    ...overrides,
  }
}

function claimInput(overrides: Partial<ClaimInput> = {}): ClaimInput {
  return { queue, now, limit: 10, leaseDuration, ...overrides }
}

function mutate(
  storage: Storage,
  method: LeaseMethod,
  job: Pick<ClaimedJob, 'id' | 'leaseToken'>,
  targetNow: number,
): Promise<LeaseMutationResult> {
  switch (method) {
    case 'complete':
      return storage.complete({ id: job.id, leaseToken: job.leaseToken, now: targetNow })
    case 'fail':
      return storage.fail({
        id: job.id,
        leaseToken: job.leaseToken,
        now: targetNow,
        error: 'boom',
        retryAt: 11,
      })
    case 'heartbeat':
      return storage.heartbeat({
        id: job.id,
        leaseToken: job.leaseToken,
        now: targetNow,
        leaseDuration,
      })
  }
}

export function runStorageConformance(
  createStorage: StorageFactory,
  cleanup: StorageCleanup,
): void {
  describe('storage conformance', () => {
    let storage: Storage

    beforeEach(async () => {
      storage = await createStorage()
    })

    afterEach(async () => {
      await cleanup()
    })

    it('enqueues independent jobs with default state', async () => {
      const first = await storage.enqueue(enqueueInput())
      const second = await storage.enqueue(enqueueInput())

      expect(first.id).not.toBe(second.id)
      expect(first).toMatchObject({
        queue,
        name: jobName,
        data,
        status: 'pending',
        attemptsMade: 0,
        createdAt: now,
        availableAt: now,
        attempts: 2,
        error: null,
      })
      expect(second).toMatchObject({ status: 'pending', attemptsMade: 0, error: null })
      expect(first.attemptsMade).toBe(0)

      const early = await storage.enqueue(enqueueInput({ availableAt: 0 }))
      expect(early.availableAt).toBe(0)

      const jobs = await storage.claim(claimInput())
      expect(jobs).toHaveLength(3)
    })

    it('claims due jobs ordered by availableAt then id', async () => {
      const first = await storage.enqueue(enqueueInput())
      const second = await storage.enqueue(enqueueInput())
      const future = await storage.enqueue(enqueueInput({ availableAt: 11 }))
      const early = await storage.enqueue(enqueueInput({ availableAt: 0 }))

      const jobs = await storage.claim(claimInput())
      const orderedIds = [first.id, second.id].sort((left, right) =>
        Buffer.compare(Buffer.from(left), Buffer.from(right)),
      )
      expect(jobs.map((job) => job.id)).toEqual([early.id, ...orderedIds])
      expect(jobs.every((job) => job.attemptsMade === 1 && job.expiresAt === expiresAt)).toBe(true)
      expect(first.attemptsMade).toBe(0)

      expect(await storage.claim(claimInput())).toEqual([])
      const dueFuture = await storage.claim(claimInput({ now: 11 }))
      expect(dueFuture.map((job) => job.id)).toEqual([future.id])
    })

    it('isolates queues by exact name and bounds claims by limit', async () => {
      await storage.enqueue(enqueueInput())
      await storage.enqueue(enqueueInput({ queue: 'Email' }))
      await storage.enqueue(enqueueInput({ queue: otherQueue }))

      const jobs = await storage.claim(claimInput())
      expect(jobs).toHaveLength(1)
      expect(jobs[0]).toMatchObject({ queue })

      expect(await storage.claim(claimInput({ queue: 'email ' }))).toEqual([])
      const exactCase = await storage.claim(claimInput({ queue: 'Email' }))
      expect(exactCase).toHaveLength(1)
      const other = await storage.claim(claimInput({ queue: otherQueue }))
      expect(other).toHaveLength(1)

      await storage.enqueue(enqueueInput({ queue: 'limited' }))
      await storage.enqueue(enqueueInput({ queue: 'limited' }))
      await storage.enqueue(enqueueInput({ queue: 'limited' }))
      const firstBatch = await storage.claim(claimInput({ queue: 'limited', limit: 2 }))
      expect(firstBatch).toHaveLength(2)
      const secondBatch = await storage.claim(claimInput({ queue: 'limited', limit: 2 }))
      expect(secondBatch).toHaveLength(1)
      expect(await storage.claim(claimInput({ queue: 'limited' }))).toEqual([])
    })

    it('completes a retried lease exactly once and keeps terminal jobs unclaimable', async () => {
      await storage.enqueue(enqueueInput())
      const [first] = await storage.claim(claimInput())
      expect(
        await storage.fail({
          id: first!.id,
          leaseToken: first!.leaseToken,
          now: 11,
          error: 'previous',
          retryAt: 11,
        }),
      ).toBe('applied')

      const [second] = await storage.claim(claimInput({ now: 11 }))
      expect(second).toMatchObject({ attemptsMade: 2, error: 'previous' })
      const credentials = { id: second!.id, leaseToken: second!.leaseToken, now: 12 }
      expect(await storage.complete(credentials)).toBe('applied')
      expect(await storage.complete(credentials)).toBe('lease_lost')
      expect(await storage.claim(claimInput({ now: 100 }))).toEqual([])
    })

    it('schedules retries, preserves errors, and enforces attempts', async () => {
      await storage.enqueue(enqueueInput({ attempts: 2 }))
      const [first] = await storage.claim(claimInput())

      expect(
        await storage.fail({
          id: first!.id,
          leaseToken: first!.leaseToken,
          now: 11,
          error: 'retry',
          retryAt: 15,
        }),
      ).toBe('applied')
      expect(await storage.claim(claimInput({ now: 14 }))).toEqual([])

      const [second] = await storage.claim(claimInput({ now: 15 }))
      expect(second).toMatchObject({
        id: first!.id,
        attemptsMade: 2,
        error: 'retry',
        availableAt: 15,
      })
      expect(second!.leaseToken).not.toBe(first!.leaseToken)
      expect(
        await storage.fail({
          id: second!.id,
          leaseToken: second!.leaseToken,
          now: 16,
          error: 'final',
          retryAt: 16,
        }),
      ).toBe('applied')
      expect(await storage.claim(claimInput({ now: 100 }))).toEqual([])
    })

    it('reclaims an expired retry with the preserved error and expiry-based availability', async () => {
      await storage.enqueue(enqueueInput({ attempts: 3 }))
      const [first] = await storage.claim(claimInput())
      expect(
        await storage.fail({
          id: first!.id,
          leaseToken: first!.leaseToken,
          now: 11,
          error: 'retry',
          retryAt: 15,
        }),
      ).toBe('applied')

      const [second] = await storage.claim(claimInput({ now: 15 }))
      expect(second).toMatchObject({ id: first!.id, attemptsMade: 2, error: 'retry' })
      const retryExpiresAt = 15 + leaseDuration

      expect(await storage.claim(claimInput({ now: retryExpiresAt - 1 }))).toEqual([])
      const [third] = await storage.claim(claimInput({ now: retryExpiresAt }))
      expect(third).toMatchObject({
        id: first!.id,
        attemptsMade: 3,
        availableAt: retryExpiresAt,
        error: 'retry',
      })
      expect(third!.leaseToken).not.toBe(second!.leaseToken)
      expect(third!.expiresAt).toBe(retryExpiresAt + leaseDuration)
    })

    it('extends live leases with heartbeat but never shortens them', async () => {
      await storage.enqueue(enqueueInput())
      const [job] = await storage.claim(claimInput())

      expect(
        await storage.heartbeat({
          id: job!.id,
          leaseToken: job!.leaseToken,
          now: 11,
          leaseDuration: 1,
        }),
      ).toBe('applied')
      // The proposed expiry 12 is shorter than the current 30: the lease must not shrink.
      expect(await storage.claim(claimInput({ now: 20 }))).toEqual([])

      expect(
        await storage.heartbeat({
          id: job!.id,
          leaseToken: job!.leaseToken,
          now: 20,
          leaseDuration: 30,
        }),
      ).toBe('applied')
      // Extended to 50, so nothing is claimable at 30 and the original token still completes.
      expect(await storage.claim(claimInput({ now: 30 }))).toEqual([])
      expect(await storage.complete({ id: job!.id, leaseToken: job!.leaseToken, now: 49 })).toBe(
        'applied',
      )
      expect(await storage.claim(claimInput({ now: 100 }))).toEqual([])

      await storage.enqueue(enqueueInput({ queue: otherQueue, attempts: 3 }))
      const [other] = await storage.claim(claimInput({ queue: otherQueue }))
      expect(
        await storage.heartbeat({
          id: other!.id,
          leaseToken: other!.leaseToken,
          now: 11,
          leaseDuration: 1,
        }),
      ).toBe('applied')
      expect(
        await storage.heartbeat({
          id: other!.id,
          leaseToken: other!.leaseToken,
          now: 12,
          leaseDuration: 1,
        }),
      ).toBe('applied')
      // Heartbeats preserve attemptsMade: the next claim is only the second attempt.
      const [reclaimed] = await storage.claim(claimInput({ queue: otherQueue, now: 30 }))
      expect(reclaimed).toMatchObject({ id: other!.id, attemptsMade: 2 })
    })

    it.each(['complete', 'fail', 'heartbeat'] as const)(
      '%s rejects missing, superseded, and exactly-expired leases without touching the live lease',
      async (method) => {
        await storage.enqueue(enqueueInput({ attempts: 3 }))
        const [first] = await storage.claim(claimInput())

        expect(
          await mutate(storage, method, { id: 'missing', leaseToken: first!.leaseToken }, 11),
        ).toBe('lease_lost')
        expect(await mutate(storage, method, { id: first!.id, leaseToken: 'wrong' }, 11)).toBe(
          'lease_lost',
        )
        expect(
          await storage.heartbeat({
            id: first!.id,
            leaseToken: first!.leaseToken,
            now: 11,
            leaseDuration,
          }),
        ).toBe('applied')

        expect(
          await storage.fail({
            id: first!.id,
            leaseToken: first!.leaseToken,
            now: 11,
            error: 'retry',
            retryAt: 11,
          }),
        ).toBe('applied')
        const [second] = await storage.claim(claimInput({ now: 11 }))
        expect(second!.leaseToken).not.toBe(first!.leaseToken)
        expect(await mutate(storage, method, first!, 11)).toBe('lease_lost')
        expect(
          await storage.heartbeat({
            id: second!.id,
            leaseToken: second!.leaseToken,
            now: 11,
            leaseDuration,
          }),
        ).toBe('applied')

        // The second lease expires at 31, so a mutation exactly at 31 is expired.
        expect(await mutate(storage, method, second!, 31)).toBe('lease_lost')
        const [third] = await storage.claim(claimInput({ now: 31 }))
        expect(third!.id).toBe(second!.id)
        expect(third!.leaseToken).not.toBe(second!.leaseToken)
        expect(await mutate(storage, method, second!, 31)).toBe('lease_lost')
      },
    )

    it.each(['complete', 'fail', 'heartbeat'] as const)(
      '%s reports lease_lost on completed and failed jobs',
      async (method) => {
        await storage.enqueue(enqueueInput())
        const [completed] = await storage.claim(claimInput())
        expect(
          await storage.complete({
            id: completed!.id,
            leaseToken: completed!.leaseToken,
            now: 11,
          }),
        ).toBe('applied')

        await storage.enqueue(enqueueInput({ queue: otherQueue }))
        const [failed] = await storage.claim(claimInput({ queue: otherQueue }))
        expect(
          await storage.fail({
            id: failed!.id,
            leaseToken: failed!.leaseToken,
            now: 11,
            error: '',
            retryAt: null,
          }),
        ).toBe('applied')

        expect(await mutate(storage, method, completed!, 12)).toBe('lease_lost')
        expect(await mutate(storage, method, failed!, 12)).toBe('lease_lost')
        expect(await storage.claim(claimInput({ now: 100 }))).toEqual([])
        expect(await storage.claim(claimInput({ queue: otherQueue, now: 100 }))).toEqual([])
      },
    )

    it('recovers all expired leases per queue even when the limit yields nothing', async () => {
      for (let index = 0; index < 3; index += 1) {
        await storage.enqueue(enqueueInput({ attempts: 1 }))
      }
      await storage.enqueue(enqueueInput({ queue: otherQueue, attempts: 1 }))

      expect(await storage.claim(claimInput())).toHaveLength(3)
      const [other] = await storage.claim(claimInput({ queue: otherQueue }))

      expect(await storage.claim(claimInput({ now: 30, limit: 1 }))).toEqual([])
      expect(await storage.claim(claimInput({ now: 100 }))).toEqual([])

      // Recovery of the email queue left the other queue's live lease alone.
      expect(
        await storage.heartbeat({
          id: other!.id,
          leaseToken: other!.leaseToken,
          now: 29,
          leaseDuration,
        }),
      ).toBe('applied')
      expect(await storage.claim(claimInput({ queue: otherQueue, now: 50 }))).toEqual([])
      expect(await storage.claim(claimInput({ queue: otherQueue, now: 100 }))).toEqual([])
    })

    it('rejects invalid inputs without mutation', async () => {
      // Messages are not standardized; only rejection without mutation is asserted.
      const base = enqueueInput()
      for (const patch of [
        { now: -1 },
        { name: '' },
        { attempts: 0 },
        { data: 'undefined' },
        { queue: '' },
        { availableAt: Number.POSITIVE_INFINITY },
        { now: 1.5 },
      ]) {
        await expect(storage.enqueue({ ...base, ...patch })).rejects.toThrow(/.+/)
      }

      const stored = await storage.enqueue(base)
      const jobs = await storage.claim(claimInput())
      expect(jobs).toHaveLength(1)
      const [claimed] = jobs
      expect(claimed!.id).toBe(stored.id)

      for (const patch of [
        { limit: 0 },
        { leaseDuration: 0 },
        { now: Number.MAX_SAFE_INTEGER },
        { now: 1.5 },
      ]) {
        await expect(storage.claim({ ...claimInput({ now: 30 }), ...patch })).rejects.toThrow(/.+/)
      }
      await expect(
        storage.fail({
          id: claimed!.id,
          leaseToken: claimed!.leaseToken,
          now: 11,
          error: 'bad',
          retryAt: -1,
        }),
      ).rejects.toThrow(/.+/)
      await expect(
        storage.heartbeat({
          id: claimed!.id,
          leaseToken: claimed!.leaseToken,
          now: 11,
          leaseDuration: Number.MAX_SAFE_INTEGER,
        }),
      ).rejects.toThrow(/.+/)
      await expect(
        storage.complete({ id: claimed!.id, leaseToken: claimed!.leaseToken, now: Number.NaN }),
      ).rejects.toThrow(/.+/)

      expect(
        await storage.heartbeat({
          id: claimed!.id,
          leaseToken: claimed!.leaseToken,
          now: 11,
          leaseDuration,
        }),
      ).toBe('applied')
      expect(
        await storage.complete({ id: claimed!.id, leaseToken: claimed!.leaseToken, now: 11 }),
      ).toBe('applied')
      expect(await storage.claim(claimInput({ now: 100 }))).toEqual([])
    })
  })
}

/**
 * Conformance for the optional grouped claim capability. Adapters that expose
 * claimQueues call this alongside runStorageConformance; adapters that do not
 * are expected to fall back to claim() through the coordinator instead.
 */ export function runGroupedClaimConformance(
  createStorage: StorageFactory,
  cleanup: StorageCleanup,
): void {
  describe('grouped claim conformance', () => {
    let storage: Storage

    beforeEach(async () => {
      storage = await createStorage()
    })

    afterEach(async () => {
      await cleanup()
    })

    function claimQueues(requests: ClaimInput[]): Promise<ClaimedJob[][]> {
      const grouped = storage.claimQueues
      if (grouped === undefined) throw new Error('claimQueues is not implemented')
      return grouped.call(storage, { requests })
    }

    it('maps every result to its request and bounds each request by its own limit', async () => {
      await storage.enqueue(enqueueInput({ queue: 'a' }))
      await storage.enqueue(enqueueInput({ queue: 'a' }))
      await storage.enqueue(enqueueInput({ queue: 'b' }))

      const results = await claimQueues([
        claimInput({ queue: 'b', limit: 5 }),
        claimInput({ queue: 'a', limit: 1 }),
        claimInput({ queue: 'a', limit: 5 }),
      ])

      expect(results).toHaveLength(3)
      expect(results[0]!.map((job) => job.queue)).toEqual(['b'])
      expect(results[1]).toHaveLength(1)
      expect(results[2]).toHaveLength(1)
      expect([...results[1]!, ...results[2]!].every((job) => job.queue === 'a')).toBe(true)
      const ids = results.flat().map((job) => job.id)
      expect(new Set(ids).size).toBe(ids.length)
    })

    it('keeps repeated requests for one queue sequential without duplicate claims', async () => {
      await storage.enqueue(enqueueInput())
      await storage.enqueue(enqueueInput())
      await storage.enqueue(enqueueInput())

      const results = await claimQueues([
        claimInput({ limit: 2 }),
        claimInput({ limit: 2 }),
        claimInput({ limit: 2 }),
      ])

      expect(results.map((jobs) => jobs.length)).toEqual([2, 1, 0])
      const ids = results.flat().map((job) => job.id)
      expect(new Set(ids).size).toBe(3)
      expect(results.flat().every((job) => job.attemptsMade === 1)).toBe(true)
    })

    it('issues a distinct lease token to every job in one batch', async () => {
      for (let index = 0; index < 4; index += 1) await storage.enqueue(enqueueInput())

      const [jobs] = await claimQueues([claimInput({ limit: 4 })])
      const tokens = jobs!.map((job) => job.leaseToken)

      expect(jobs).toHaveLength(4)
      expect(tokens.every((token) => token.length > 0)).toBe(true)
      expect(new Set(tokens).size).toBe(4)
    })

    it('returns one empty result for a queue with no eligible work', async () => {
      await storage.enqueue(enqueueInput())

      const results = await claimQueues([
        claimInput({ queue: 'missing' }),
        claimInput({ queue: otherQueue }),
      ])

      expect(results).toEqual([[], []])
    })

    it('accepts an empty batch without touching storage', async () => {
      await storage.enqueue(enqueueInput())

      expect(await claimQueues([])).toEqual([])
      expect(await storage.claim(claimInput())).toHaveLength(1)
    })

    it('claims every requested queue exactly once when the batch spans several transactions', async () => {
      // A limit above any internal budget forces the adapter to commit the batch
      // in several transactions; a small limit keeps it in as few as possible.
      for (const limit of [512, 16]) {
        const queues = Array.from({ length: 40 }, (_, index) => `queue-${limit}-${index}`)
        for (const queue of queues) {
          await storage.enqueue(enqueueInput({ queue }))
          await storage.enqueue(enqueueInput({ queue }))
        }

        const requests = queues.map((queue) => claimInput({ queue, limit }))
        const results = await claimQueues(requests)

        expect(results).toHaveLength(queues.length)
        expect(results.map((jobs) => jobs.length)).toEqual(queues.map(() => 2))
        const ids = results.flat().map((job) => job.id)
        expect(new Set(ids).size).toBe(ids.length)
        expect(results.flat().every((job) => job.attemptsMade === 1)).toBe(true)

        // Every job is leased, so a later sweep finds nothing left behind.
        expect((await claimQueues(requests)).flat()).toEqual([])
      }
    })

    it('recovers expired leases per requested queue and leaves other queues alone', async () => {
      await storage.enqueue(enqueueInput({ queue, attempts: 1 }))
      await storage.enqueue(enqueueInput({ queue: otherQueue, attempts: 1 }))
      expect(await storage.claim(claimInput())).toHaveLength(1)
      const [other] = await storage.claim(claimInput({ queue: otherQueue }))

      const results = await claimQueues([claimInput({ now: expiresAt, limit: 1 })])
      expect(results).toEqual([[]])

      // Recovery of the email queue marked its exhausted job failed but must not
      // have recovered the other queue's still-live lease.
      expect(await storage.claim(claimInput({ queue, now: 100 }))).toEqual([])
      expect(
        await storage.heartbeat({
          id: other!.id,
          leaseToken: other!.leaseToken,
          now: expiresAt - 1,
          leaseDuration,
        }),
      ).toBe('applied')
    })

    it('rejects an invalid request before mutating any requested queue', async () => {
      await storage.enqueue(enqueueInput({ queue: 'a' }))
      await storage.enqueue(enqueueInput({ queue: 'b' }))

      await expect(
        claimQueues([claimInput({ queue: 'a' }), claimInput({ queue: 'b', limit: 0 })]),
      ).rejects.toThrow(/.+/)

      // Nothing was claimed, so both queues still hold their jobs.
      expect(await storage.claim(claimInput({ queue: 'a' }))).toHaveLength(1)
      expect(await storage.claim(claimInput({ queue: 'b' }))).toHaveLength(1)
    })
  })
}

/** Conformance for bounded terminal-job cleanup. */
export function runCleanupConformance(
  createStorage: StorageFactory,
  cleanup: StorageCleanup,
): void {
  describe('cleanup conformance', () => {
    let storage: Storage
    const cleanupNow = 1_000_000

    beforeEach(async () => {
      storage = await createStorage()
    })

    afterEach(async () => {
      await cleanup()
    })

    function rule(count: number | null, maxAge: number | null = null): RetentionRule {
      return { count, maxAge }
    }

    function runCleanup(input: CleanupInput): Promise<CleanupResult> {
      return storage.cleanup(input)
    }

    function cleanupInput(overrides: Partial<CleanupInput> = {}): CleanupInput {
      return {
        queue,
        retention: { completed: rule(0), failed: rule(0) },
        now: cleanupNow,
        limit: 10,
        ...overrides,
      }
    }

    /** Create one terminal job for `queue` whose finish time is `finishedAt`. */
    async function finish(
      status: 'completed' | 'failed',
      finishedAt: number,
      targetQueue = queue,
    ): Promise<ClaimedJob> {
      const created = await storage.enqueue(
        enqueueInput({
          queue: targetQueue,
          now: finishedAt,
          availableAt: finishedAt,
          attempts: 1,
        }),
      )
      const [job] = await storage.claim(
        claimInput({ queue: targetQueue, now: finishedAt, limit: 1 }),
      )
      if (job === undefined || job.id !== created.id) {
        throw new Error('cleanup conformance could not claim the seeded job')
      }

      const result =
        status === 'failed'
          ? await storage.fail({
              id: job.id,
              leaseToken: job.leaseToken,
              now: finishedAt,
              error: 'cleanup failure',
              retryAt: null,
            })
          : await storage.complete({ id: job.id, leaseToken: job.leaseToken, now: finishedAt })
      if (result !== 'applied') throw new Error('cleanup conformance could not finish the job')

      return job
    }

    it('removes only terminal rows beyond the retention counts and queue scope', async () => {
      for (const finishedAt of [100, 101, 102, 103]) await finish('completed', finishedAt)
      for (const finishedAt of [200, 201, 202]) await finish('failed', finishedAt)
      for (const finishedAt of [300, 301]) await finish('completed', finishedAt, otherQueue)

      const activeSource = await storage.enqueue(
        enqueueInput({ queue, now: 400, availableAt: 400, attempts: 1 }),
      )
      const [active] = await storage.claim(
        claimInput({ queue, now: 400, limit: 1, leaseDuration: 10_000 }),
      )
      expect(active?.id).toBe(activeSource.id)
      const pending = await storage.enqueue(enqueueInput({ queue, now: 401, availableAt: 401 }))

      // Four completed and three failed rows are eligible: keep 2 and 1.
      const keepTwoAndOne = { completed: rule(2), failed: rule(1) }
      expect(await runCleanup(cleanupInput({ retention: keepTwoAndOne }))).toEqual({
        removed: 4,
        more: false,
      })
      expect(await runCleanup(cleanupInput({ retention: keepTwoAndOne }))).toEqual({
        removed: 0,
        more: false,
      })

      // Tightening the limits removes the retained rows as well.
      expect(await runCleanup(cleanupInput())).toEqual({ removed: 3, more: false })

      // Another queue keeps its rows until it is cleaned with its own policy.
      expect(await runCleanup(cleanupInput({ queue: otherQueue }))).toEqual({
        removed: 2,
        more: false,
      })

      // Pending and active rows were never eligible.
      expect(
        await storage.heartbeat({
          id: active!.id,
          leaseToken: active!.leaseToken,
          now: 402,
          leaseDuration: 10_000,
        }),
      ).toBe('applied')
      const claimed = await storage.claim(claimInput({ queue, now: 402, limit: 10 }))
      expect(claimed.map((job) => job.id)).toEqual([pending.id])
    })

    it('bounds work per call and reports that more rows remain', async () => {
      for (const finishedAt of [100, 101, 102, 103, 104]) await finish('completed', finishedAt)

      expect(await runCleanup(cleanupInput({ limit: 2 }))).toEqual({ removed: 2, more: true })
      expect(await runCleanup(cleanupInput({ limit: 2 }))).toEqual({ removed: 2, more: true })
      expect(await runCleanup(cleanupInput({ limit: 2 }))).toEqual({ removed: 1, more: false })
      expect(await runCleanup(cleanupInput({ limit: 2 }))).toEqual({ removed: 0, more: false })
    })

    it('keeps every terminal row when both bounds are disabled', async () => {
      await finish('completed', cleanupNow - 100)
      await finish('failed', cleanupNow - 101)

      const disabled = { completed: rule(null, null), failed: rule(null, null) }
      expect(await runCleanup(cleanupInput({ retention: disabled }))).toEqual({
        removed: 0,
        more: false,
      })
      expect(await runCleanup(cleanupInput())).toEqual({ removed: 2, more: false })
    })

    it('removes only rows strictly older than the max-age cutoff', async () => {
      await finish('completed', cleanupNow - 102)
      await finish('completed', cleanupNow - 101)
      await finish('completed', cleanupNow - 100)
      await finish('failed', cleanupNow - 200)
      await finish('completed', cleanupNow - 400, otherQueue)

      // Rows strictly older than now - 100 are eligible; the exact-cutoff row is
      // retained.
      const ageHundred = { completed: rule(null, 100), failed: rule(null, 100) }
      expect(await runCleanup(cleanupInput({ retention: ageHundred }))).toEqual({
        removed: 3,
        more: false,
      })
      expect(await runCleanup(cleanupInput({ retention: ageHundred }))).toEqual({
        removed: 0,
        more: false,
      })

      // A smaller maxAge moves the cutoff inside the retained row.
      const ageNinetyNine = { completed: rule(null, 99), failed: rule(null, 99) }
      expect(await runCleanup(cleanupInput({ retention: ageNinetyNine }))).toEqual({
        removed: 1,
        more: false,
      })

      // Age cleanup never crosses queue boundaries.
      expect(await runCleanup(cleanupInput({ queue: otherQueue }))).toEqual({
        removed: 1,
        more: false,
      })
    })

    it('bounds age cleanup per call and deletes from the oldest end', async () => {
      const finishedAt = [
        cleanupNow - 5,
        cleanupNow - 4,
        cleanupNow - 3,
        cleanupNow - 2,
        cleanupNow - 1,
      ]
      for (const time of finishedAt) await finish('completed', time)

      // maxAge 0 removes every row finished strictly before now.
      const retention = { completed: rule(null, 0), failed: rule(0) }
      expect(await runCleanup(cleanupInput({ retention, limit: 2 }))).toEqual({
        removed: 2,
        more: true,
      })
      expect(await runCleanup(cleanupInput({ retention, limit: 2 }))).toEqual({
        removed: 2,
        more: true,
      })
      expect(await runCleanup(cleanupInput({ retention, limit: 2 }))).toEqual({
        removed: 1,
        more: false,
      })
      expect(await runCleanup(cleanupInput({ retention, limit: 2 }))).toEqual({
        removed: 0,
        more: false,
      })
    })

    it('applies the count bound while an age bound is configured', async () => {
      for (const time of [cleanupNow - 400, cleanupNow - 300, cleanupNow - 200, cleanupNow - 100]) {
        await finish('completed', time)
      }

      // The huge maxAge makes the age bound ineffective, so the count keeps only
      // the newest row.
      const retention = { completed: rule(1, 1_000_000), failed: rule(0) }
      expect(await runCleanup(cleanupInput({ retention }))).toEqual({ removed: 3, more: false })
    })

    it('applies the age bound while a count bound is configured', async () => {
      for (const time of [cleanupNow - 500, cleanupNow - 400, cleanupNow - 200, cleanupNow - 100]) {
        await finish('completed', time)
      }

      // The count keeps everything, so only rows older than now - 200 are removed.
      const retention = { completed: rule(100, 200), failed: rule(0) }
      expect(await runCleanup(cleanupInput({ retention }))).toEqual({ removed: 2, more: false })
    })

    it('rejects invalid inputs without mutating terminal rows', async () => {
      await finish('completed', cleanupNow - 100)

      const patches: Partial<CleanupInput>[] = [
        { queue: '' },
        { limit: 0 },
        { limit: 1.5 },
        { now: -1 },
        { now: 1.5 },
        { now: Number.NaN },
        { now: Number.POSITIVE_INFINITY },
        { retention: { completed: rule(-1), failed: rule(0) } },
        { retention: { completed: rule(0, -1), failed: rule(0) } },
        { retention: { completed: rule(1.5, 0), failed: rule(0) } },
        { retention: { completed: rule(0), failed: rule(0, 1.5) } },
        { retention: { completed: null, failed: rule(0) } as never },
        { retention: { completed: rule(0), failed: undefined } as never },
        { retention: null as never },
      ]
      for (const patch of patches) {
        await expect(runCleanup({ ...cleanupInput(), ...patch })).rejects.toThrow(/.+/)
      }

      expect(await runCleanup(cleanupInput())).toEqual({ removed: 1, more: false })
    })
  })
}
