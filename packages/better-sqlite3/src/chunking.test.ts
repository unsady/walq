import { describe, expect, it } from 'vitest'

import { claimBudget, claimChunkSize, chunkClaims } from './chunking.js'

interface Request {
  limit: number
}

function limits(chunks: Request[][]): number[][] {
  return chunks.map((chunk) => chunk.map(({ limit }) => limit))
}

describe('claim chunk size', () => {
  it('derives the queue count from the job budget', () => {
    expect(claimChunkSize(16)).toBe(32)
    expect(claimChunkSize(32)).toBe(16)
    expect(claimChunkSize(64)).toBe(8)
  })

  it('fills the budget for limits that divide it', () => {
    expect(claimChunkSize(1) * 1).toBe(claimBudget)
    expect(claimChunkSize(16) * 16).toBe(claimBudget)
    expect(claimChunkSize(512) * 512).toBe(claimBudget)
  })

  it('never plans more jobs than the budget for a uniform limit', () => {
    for (const limit of [1, 3, 15, 16, 17, 255, 256, 300, 511, 512]) {
      expect(claimChunkSize(limit) * limit).toBeLessThanOrEqual(claimBudget)
    }

    expect(claimChunkSize(300)).toBe(1)
    expect(claimChunkSize(511)).toBe(1)
  })

  it('never returns less than one queue for a limit above the budget', () => {
    expect(claimChunkSize(1000)).toBe(1)
    expect(claimChunkSize(Number.MAX_SAFE_INTEGER)).toBe(1)
  })
})

describe('chunked grouped claims', () => {
  it('returns no transactions for an empty batch', () => {
    expect(chunkClaims([], ({ limit }: Request) => limit)).toEqual([])
  })

  it('keeps a small batch in one transaction', () => {
    const requests = Array.from({ length: 5 }, () => ({ limit: 16 }))

    expect(limits(chunkClaims(requests, ({ limit }) => limit))).toEqual([[16, 16, 16, 16, 16]])
  })

  it('splits a batch into budget-sized transactions and preserves order', () => {
    const requests = Array.from({ length: 70 }, (_, index) => ({ limit: 16, index }))

    const chunks = chunkClaims(requests, ({ limit }) => limit)

    expect(chunks.map((chunk) => chunk.length)).toEqual([32, 32, 6])
    expect(chunks.flat()).toEqual(requests)
  })

  it('accumulates the limits of every request in a transaction', () => {
    const requests: Request[] = [
      { limit: 1 },
      ...Array.from({ length: 3 }, () => ({ limit: 512 })),
      ...Array.from({ length: 2 }, () => ({ limit: 16 })),
    ]

    expect(limits(chunkClaims(requests, ({ limit }) => limit))).toEqual([
      [1],
      [512],
      [512],
      [512],
      [16, 16],
    ])
  })

  it('fills a transaction with mixed limits instead of sizing it from the first request', () => {
    const requests: Request[] = [
      ...Array.from({ length: 10 }, () => ({ limit: 1 })),
      { limit: 502 },
      { limit: 2 },
      { limit: 1 },
      { limit: 1 },
    ]

    expect(limits(chunkClaims(requests, ({ limit }) => limit))).toEqual([
      [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 502],
      [2, 1, 1],
    ])
  })

  it('gives a request above the budget a transaction of its own', () => {
    const requests: Request[] = [{ limit: 1000 }, { limit: 1 }, { limit: 1 }]

    expect(chunkClaims(requests, ({ limit }) => limit).map((chunk) => chunk.length)).toEqual([1, 2])
  })

  it('keeps every shared transaction inside the budget when the limits vary', () => {
    const shape = [1, 16, 64, 512, 300, 511, 3, 128, 1000]
    const requests = Array.from({ length: 200 }, (_, index) => ({
      limit: shape[index % shape.length] ?? 1,
    }))

    const chunks = chunkClaims(requests, ({ limit }) => limit)

    expect(chunks.flat()).toEqual(requests)
    // A single request above the budget cannot be split any further.
    const oversized = chunks.filter(
      (chunk) =>
        chunk.length > 1 && chunk.reduce((sum, request) => sum + request.limit, 0) > claimBudget,
    )
    expect(oversized).toEqual([])
  })

  it('keeps a batch of single-job requests inside the budget', () => {
    const requests = Array.from({ length: 700 }, () => ({ limit: 1 }))

    expect(chunkClaims(requests, ({ limit }) => limit).map((chunk) => chunk.length)).toEqual([
      512, 188,
    ])
  })
})
