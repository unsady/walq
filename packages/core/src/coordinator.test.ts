import type { ClaimedJob, ClaimInput, EnqueueInput, Storage, StoredJob } from '@walq/core/storage'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { getCoordinator, type CoordinatedWorker } from './coordinator.js'
import { deferred } from './delay.js'
import { GroupedTestStorage, TestStorage } from './fixtures/storage.js'

const now = 1_000

const enqueueInput: EnqueueInput = {
  queue: 'email',
  name: 'email',
  data: '{}',
  now: 1_000,
  availableAt: 1_000,
  priority: 0,
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
  priority: 0,
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
  handler: (requests: ClaimInput[]) => ClaimedJob[][] = (requests) => requests.map(() => []),
): GroupedStorage {
  const calls: ClaimInput[][] = []
  const storage = new GroupedTestStorage()
  storage.claim = async () => {
    throw new Error('claim must not be called when claimQueues exists')
  }
  storage.claimQueues = async ({ requests }) => {
    calls.push(requests)
    return handler(requests)
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
  const storage = new TestStorage()
  storage.enqueue = async () => {
    started.push('enqueue')
    await gate.promise
    return storedJob
  }
  storage.claim = async () => {
    started.push('claim')
    await gate.promise
    return []
  }

  return { storage, started, release: () => gate.resolve() }
}

afterEach(() => {
  vi.useRealTimers()
})

describe('StorageCoordinator', () => {
  it('allows storage operations to overlap', async () => {
    const { storage, started, release } = gatedStorage()
    const coordinator = getCoordinator(storage)

    const first = coordinator.enqueue(enqueueInput)
    const second = coordinator.claim(claimInput)
    expect(started).toEqual(expect.arrayContaining(['enqueue', 'claim']))

    release()
    await Promise.all([first, second])
  })

  it('polls ready workers concurrently', async () => {
    vi.useFakeTimers()
    const storage = new TestStorage()
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

  it.each(['throw', 'reject'])('handles poll %s and logger failures', async (mode) => {
    vi.useFakeTimers()
    const log = vi.spyOn(console, 'error').mockImplementation(() => {})
    const storage = new TestStorage()
    const coordinator = getCoordinator(storage)
    const failure = new Error('unexpected poll failure')
    const failing: CoordinatedWorker = {
      poll: () => {
        if (mode === 'throw') throw failure
        return Promise.reject(failure)
      },
    }
    const poll = vi.fn<CoordinatedWorker['poll']>(async () => 0)
    const healthy: CoordinatedWorker = { poll }

    try {
      coordinator.register('failing', failing)
      coordinator.register('healthy', healthy)
      await vi.advanceTimersByTimeAsync(0)

      expect(log).toHaveBeenCalledWith(expect.stringContaining('failing'), failure)
      expect(poll).toHaveBeenCalledTimes(1)

      log.mockImplementation(() => {
        throw new Error('logger failed')
      })
      await vi.advanceTimersByTimeAsync(1_000)
      expect(poll.mock.calls.length).toBeGreaterThan(1)
    } finally {
      coordinator.unregister(failing)
      coordinator.unregister(healthy)
      log.mockRestore()
    }
  })

  it('polls only while workers are registered', async () => {
    vi.useFakeTimers()
    const storage = new TestStorage()
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

  it('claims the requested queues when the adapter has no grouped capability', async () => {
    const storage = new TestStorage()
    storage.jobs.push(claimedJob('email'), claimedJob('sms'))
    const coordinator = getCoordinator(storage)

    const [email, sms] = await Promise.all([
      coordinator.claim(claimInput),
      coordinator.claim({ ...claimInput, queue: 'sms' }),
    ])

    expect(email.map(({ id }) => id)).toEqual(['email'])
    expect(sms.map(({ id }) => id)).toEqual(['sms'])
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

  it('rejects the entire batch when a later grouped result is missing', async () => {
    const value: ClaimedJob[][] = [[], []]
    delete value[1]
    const { storage } = groupedStorage(() => value)
    const coordinator = getCoordinator(storage)
    const results = await Promise.allSettled([
      coordinator.claim({ ...claimInput, queue: 'a' }),
      coordinator.claim({ ...claimInput, queue: 'b' }),
    ])

    expect(results).toHaveLength(2)
    for (const result of results) {
      expect(result).toMatchObject({
        status: 'rejected',
        reason: expect.objectContaining({ message: expect.stringContaining('must be an array') }),
      })
    }
  })

  it('rejects a non-array grouped response', async () => {
    const { storage } = groupedStorage(() => ({ length: 1 }) as ClaimedJob[][])

    await expect(getCoordinator(storage).claim(claimInput)).rejects.toThrow(
      'Grouped claim must return an array',
    )
  })

  it('wakes only workers of the requested queue', async () => {
    vi.useFakeTimers()
    const storage = new TestStorage()
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

describe('grouped claim batching (performance contract)', () => {
  it('coalesces same-sweep worker claims into one grouped call', async () => {
    vi.useFakeTimers()
    const { storage, calls } = groupedStorage()
    const coordinator = getCoordinator(storage)
    function worker(queue: string): CoordinatedWorker {
      return {
        poll: async () => {
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
    coordinator.wakeQueue('a')
    coordinator.wakeQueue('b')
    await vi.advanceTimersByTimeAsync(0)

    expect(calls).toHaveLength(1)
    expect(calls[0]!.map((request) => request.queue).sort()).toEqual(['a', 'b'])

    coordinator.unregister(first)
    coordinator.unregister(second)
  })
})
