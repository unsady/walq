import { describe, expect, it } from 'vitest'

import { claimBudget, chunkClaims } from './chunking.js'

interface Request {
  limit: number
}

function limits(chunks: Request[][]): number[][] {
  return chunks.map((chunk) => chunk.map(({ limit }) => limit))
}

describe('grouped claim transaction budget (performance contract)', () => {
  it('returns no transactions for an empty batch', () => {
    expect(chunkClaims([], ({ limit }: Request) => limit)).toEqual([])
  })

  it('splits a batch into budget-sized transactions and preserves order', () => {
    const perChunk = Math.floor(claimBudget / 16)
    const requests = Array.from({ length: perChunk * 2 + 1 }, (_, index) => ({ limit: 16, index }))

    const chunks = chunkClaims(requests, ({ limit }) => limit)

    expect(chunks.map((chunk) => chunk.length)).toEqual([perChunk, perChunk, 1])
    expect(chunks.flat()).toEqual(requests)
  })

  it('fills a transaction with mixed limits instead of sizing it from the first request', () => {
    const requests: Request[] = [
      { limit: 1 },
      { limit: 1 },
      { limit: claimBudget - 2 },
      { limit: 1 },
    ]

    expect(limits(chunkClaims(requests, ({ limit }) => limit))).toEqual([
      [1, 1, claimBudget - 2],
      [1],
    ])
  })

  it('gives a request above the budget a transaction of its own', () => {
    const requests: Request[] = [{ limit: claimBudget + 1 }, { limit: 1 }, { limit: 1 }]

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
})
