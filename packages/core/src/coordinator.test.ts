import type { ClaimedJob, ClaimInput, EnqueueInput, Storage, StoredJob } from '@walq/core/storage'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { getCoordinator, type CoordinatedWorker } from './coordinator.js'
import { deferred } from './delay.js'

const now = 1_000

const enqueueInput: EnqueueInput = {
  queue: 'email',
  name: 'email',
  data: '{}',
  now: 1_000,
  availableAt: 1_000,
  attempts: 1,
}

const claimInput: ClaimInput = {
  queue: 'email',
  limit: 1,
  now: 1_000,
  leaseDuration: 30_000,
}

const storedJob: StoredJob = {
  id: 'job-1',
  queue: 'email',
  name: 'email',
  data: '{}',
  status: 'pending',
  createdAt: 1_000,
  availableAt: 1_000,
  attemptsMade: 0,
  attempts: 1,
  error: null,
}

interface GatedStorage {
  storage: Storage
  started: string[]
  release(): void
}

interface GroupedStorage {
  storage: Storage
  calls: ClaimInput[][]
}

/** Storage that advertises claimQueues and records each grouped request. */
function groupedStorage(
  handler: (requests: ClaimInput[]) => ClaimedJob[][] = () => [],
): GroupedStorage {
  const calls: ClaimInput[][] = []
  const storage: Storage = {
    async enqueue() {
      return storedJob
    },
    async enqueueMany(inputs) {
      return inputs.map((input, index) => ({
        ...storedJob,
        id: `job-${index + 1}`,
        queue: input.queue,
        name: input.name,
        data: input.data,
        createdAt: input.now,
        availableAt: input.availableAt,
        attempts: input.attempts,
      }))
    },
    async claim() {
      throw new Error('claim must not be called when claimQueues exists')
    },
    async claimQueues({ requests }) {
      calls.push(requests)
      return handler(requests)
    },
    async inspect() {
      return null
    },
    async list() {
      return []
    },
    async retry() {
      return false
    },
    async cancel() {
      return false
    },
    async reschedule() {
      return false
    },
    async remove() {
      return false
    },
    async complete() {
      return 'applied'
    },
    async fail() {
      return 'applied'
    },
    async heartbeat() {
      return 'applied'
    },
    async cleanup() {
      return { removed: 0, more: false }
    },
  }

  return { storage, calls }
}

function claimedJob(queue: string, id = queue): ClaimedJob {
  return {
    ...storedJob,
    id,
    queue,
    status: 'active',
    leaseToken: `lease-${id}`,
    expiresAt: 31_000,
  }
}

function gatedStorage(): GatedStorage {
  const started: string[] = []
  const gate = deferred()

  const storage: Storage = {
    async enqueue() {
      started.push('enqueue')
      await gate.promise
      return storedJob
    },
    async enqueueMany(inputs) {
      started.push('enqueueMany')
      await gate.promise
      return inputs.map((input, index) => ({
        ...storedJob,
        id: `job-${index + 1}`,
        queue: input.queue,
        name: input.name,
        data: input.data,
        createdAt: input.now,
        availableAt: input.availableAt,
        attempts: input.attempts,
      }))
    },
    async claim() {
      started.push('claim')
      await gate.promise
      return []
    },
    async inspect() {
      started.push('inspect')
      await gate.promise
      return null
    },
    async list() {
      started.push('list')
      await gate.promise
      return []
    },
    async retry() {
      started.push('retry')
      await gate.promise
      return false
    },
    async cancel() {
      started.push('cancel')
      await gate.promise
      return false
    },
    async reschedule() {
      started.push('reschedule')
      await gate.promise
      return false
    },
    async remove() {
      started.push('remove')
      await gate.promise
      return false
    },
    async complete() {
      started.push('complete')
      await gate.promise
      return 'applied'
    },
    async fail() {
      started.push('fail')
      await gate.promise
      return 'applied'
    },
    async heartbeat() {
      started.push('heartbeat')
      await gate.promise
      return 'applied'
    },
    async cleanup() {
      started.push('cleanup')
      await gate.promise
      return { removed: 0, more: false }
    },
  }

  return { storage, started, release: () => gate.resolve() }
}

afterEach(() => {
  vi.useRealTimers()
})

describe('StorageCoordinator', () => {
  it('returns one coordinator per storage instance', () => {
    const first = gatedStorage().storage
    const second = gatedStorage().storage

    expect(getCoordinator(first)).toBe(getCoordinator(first))
    expect(getCoordinator(first)).not.toBe(getCoordinator(second))
  })

  it('allows storage operations to overlap', async () => {
    const { storage, started, release } = gatedStorage()
    const coordinator = getCoordinator(storage)

    const first = coordinator.enqueue(enqueueInput)
    const second = coordinator.claim(claimInput)
    expect(started).toEqual(['enqueue', 'claim'])

    release()
    await Promise.all([first, second])
  })

  it('polls ready workers concurrently', async () => {
    vi.useFakeTimers()
    const { storage } = gatedStorage()
    const coordinator = getCoordinator(storage)
    const gate = deferred()
    const started: string[] = []
    let blocking = false
    function worker(name: string): CoordinatedWorker {
      return {
        poll: async () => {
          started.push(name)
          if (blocking) await gate.promise
          return 0
        },
      }
    }

    const first = worker('a')
    const second = worker('b')
    coordinator.register('a', first)
    coordinator.register('b', second)
    await vi.advanceTimersByTimeAsync(0)

    started.length = 0
    blocking = true
    coordinator.wakeQueue('a')
    coordinator.wakeQueue('b')
    await vi.advanceTimersByTimeAsync(0)
    expect(started.sort()).toEqual(['a', 'b'])

    gate.resolve()
    coordinator.unregister(first)
    coordinator.unregister(second)
  })

  it('rotates the poll order between workers', async () => {
    vi.useFakeTimers()
    const { storage } = gatedStorage()
    const coordinator = getCoordinator(storage)
    const order: string[] = []
    function worker(name: string): CoordinatedWorker {
      return {
        poll: async () => {
          order.push(name)
          return 0
        },
      }
    }

    coordinator.register('a', worker('a'))
    coordinator.register('b', worker('b'))
    await vi.advanceTimersByTimeAsync(1_000)

    order.length = 0
    await vi.advanceTimersByTimeAsync(1_000)
    const first = order.slice()
    order.length = 0
    await vi.advanceTimersByTimeAsync(1_000)

    expect(first).toHaveLength(2)
    expect(order).toHaveLength(2)
    expect(order[0]).not.toBe(first[0])
  })

  it('polls only while workers are registered', async () => {
    vi.useFakeTimers()
    const { storage } = gatedStorage()
    const coordinator = getCoordinator(storage)
    let polls = 0
    const worker: CoordinatedWorker = {
      poll: async () => {
        polls += 1
        return 0
      },
    }

    coordinator.register('email', worker)
    await vi.advanceTimersByTimeAsync(0)
    expect(polls).toBe(1)

    coordinator.unregister(worker)
    await vi.advanceTimersByTimeAsync(5_000)
    expect(polls).toBe(1)
  })

  it('falls back to direct claim calls when the adapter has no claimQueues', async () => {
    const { storage, started, release } = gatedStorage()
    const coordinator = getCoordinator(storage)

    const first = coordinator.claim(claimInput)
    const second = coordinator.claim({ ...claimInput, queue: 'sms' })
    // Both requests reached storage synchronously, one call each.
    expect(started).toEqual(['claim', 'claim'])

    release()
    await Promise.all([first, second])
  })

  it('leaves non-claim operations on the direct storage path', async () => {
    const { storage, started, release } = gatedStorage()
    const coordinator = getCoordinator(storage)

    const pending = [
      coordinator.enqueue(enqueueInput),
      coordinator.complete({ id: 'job-1', leaseToken: 'lease-1', now }),
      coordinator.fail({ id: 'job-1', leaseToken: 'lease-1', now, error: '', retryAt: null }),
      coordinator.heartbeat({ id: 'job-1', leaseToken: 'lease-1', now, leaseDuration: 30_000 }),
      coordinator.cleanup({
        queue: 'email',
        retention: {
          completed: { count: 0, maxAge: null },
          failed: { count: 10, maxAge: null },
        },
        now,
        limit: 500,
      }),
    ]
    expect(started).toEqual(['enqueue', 'complete', 'fail', 'heartbeat', 'cleanup'])

    release()
    await Promise.all(pending)
  })

  it('coalesces same-sweep worker claims into one grouped call in poll order', async () => {
    vi.useFakeTimers()
    const { storage, calls } = groupedStorage()
    const coordinator = getCoordinator(storage)
    const polled: string[] = []
    function worker(queue: string): CoordinatedWorker {
      return {
        poll: async () => {
          polled.push(queue)
          await coordinator.claim({ queue, limit: 1, now, leaseDuration: 30_000 })
          return 0
        },
      }
    }

    const first = worker('a')
    const second = worker('b')
    coordinator.register('a', first)
    coordinator.register('b', second)
    await vi.advanceTimersByTimeAsync(0)

    calls.length = 0
    polled.length = 0
    coordinator.wakeQueue('a')
    coordinator.wakeQueue('b')
    await vi.advanceTimersByTimeAsync(0)

    expect(calls).toHaveLength(1)
    expect(calls[0]!.map((request) => request.queue)).toEqual(polled)
    expect(calls[0]!.map((request) => request.queue).sort()).toEqual(['a', 'b'])

    coordinator.unregister(first)
    coordinator.unregister(second)
  })

  it('maps grouped results back to request order', async () => {
    const { storage } = groupedStorage((requests) =>
      requests.map((request) => [claimedJob(request.queue)]),
    )
    const coordinator = getCoordinator(storage)

    const first = coordinator.claim({ ...claimInput, queue: 'a' })
    const second = coordinator.claim({ ...claimInput, queue: 'b' })
    const [jobsA, jobsB] = await Promise.all([first, second])

    expect(jobsA!.map((job) => job.id)).toEqual(['a'])
    expect(jobsB!.map((job) => job.id)).toEqual(['b'])
  })

  it('keeps repeated queue requests distinct and in order', async () => {
    let issued = 0
    const { storage } = groupedStorage((requests) =>
      requests.map(() => {
        if (issued >= 2) return []
        issued += 1
        return [claimedJob('email', `job-${issued}`)]
      }),
    )
    const coordinator = getCoordinator(storage)

    const first = coordinator.claim({ ...claimInput, queue: 'email' })
    const second = coordinator.claim({ ...claimInput, queue: 'email' })
    const [jobsFirst, jobsSecond] = await Promise.all([first, second])

    expect(jobsFirst!.map((job) => job.id)).toEqual(['job-1'])
    expect(jobsSecond!.map((job) => job.id)).toEqual(['job-2'])
  })

  it('rejects every claim in a batch when the grouped call fails', async () => {
    const { storage } = groupedStorage(() => {
      throw new Error('grouped failure')
    })
    const coordinator = getCoordinator(storage)

    const first = coordinator.claim({ ...claimInput, queue: 'a' })
    const second = coordinator.claim({ ...claimInput, queue: 'b' })
    const results = await Promise.allSettled([first, second])

    expect(results).toEqual([
      { status: 'rejected', reason: new Error('grouped failure') },
      { status: 'rejected', reason: new Error('grouped failure') },
    ])
  })

  it('rejects every claim when grouped result cardinality is invalid', async () => {
    const { storage } = groupedStorage(() => [[]])
    const coordinator = getCoordinator(storage)

    const first = coordinator.claim({ ...claimInput, queue: 'a' })
    const second = coordinator.claim({ ...claimInput, queue: 'b' })
    const results = await Promise.allSettled([first, second])

    expect(results).toEqual([
      {
        status: 'rejected',
        reason: new Error('Grouped claim returned 1 results for 2 requests'),
      },
      {
        status: 'rejected',
        reason: new Error('Grouped claim returned 1 results for 2 requests'),
      },
    ])
  })

  it('wakes only workers of the requested queue', async () => {
    vi.useFakeTimers()
    const { storage } = gatedStorage()
    const coordinator = getCoordinator(storage)
    const polls = { email: 0, sms: 0 }
    const email: CoordinatedWorker = {
      poll: async () => {
        polls.email += 1
        return 0
      },
    }
    const sms: CoordinatedWorker = {
      poll: async () => {
        polls.sms += 1
        return 0
      },
    }

    coordinator.register('email', email)
    coordinator.register('sms', sms)
    await vi.advanceTimersByTimeAsync(0)
    expect(polls).toEqual({ email: 1, sms: 1 })

    coordinator.wakeQueue('email')
    await vi.advanceTimersByTimeAsync(0)
    expect(polls).toEqual({ email: 2, sms: 1 })

    coordinator.wakeWorker(email)
    await vi.advanceTimersByTimeAsync(0)
    expect(polls).toEqual({ email: 3, sms: 1 })

    coordinator.unregister(email)
    coordinator.unregister(sms)
  })
})
