import type { ClaimInput, EnqueueInput, Storage, StoredJob } from '@walq/core/storage'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { getCoordinator, type CoordinatedWorker } from './coordinator.js'
import { deferred } from './delay.js'

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

type GatedStorage = {
  storage: Storage
  started: string[]
  release(): void
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
    async claim() {
      started.push('claim')
      await gate.promise
      return []
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

  it('runs one storage operation at a time', async () => {
    const { storage, started, release } = gatedStorage()
    const coordinator = getCoordinator(storage)

    const first = coordinator.enqueue(enqueueInput)
    const second = coordinator.claim(claimInput)
    await vi.waitFor(() => expect(started).toEqual(['enqueue']))

    release()
    await Promise.all([first, second])
    expect(started).toEqual(['enqueue', 'claim'])
  })

  it('rotates the poll order between workers', async () => {
    vi.useFakeTimers()
    const { storage } = gatedStorage()
    const coordinator = getCoordinator(storage)
    const order: string[] = []
    const worker = (name: string): CoordinatedWorker => ({
      poll: async () => {
        order.push(name)
        return 0
      },
    })

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
