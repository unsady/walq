/**
 * Grouped claims are split into several transactions so one poller sweep over
 * many queues does not hold the writer lock for the whole round. The budget is
 * internal: the adapter owns the trade-off between lock hold and commit count.
 */
export const claimBudget = 512

/** Queues one transaction may cover at a uniform limit: 16 -> 32, 32 -> 16, 64 -> 8. */
export function claimChunkSize(limit: number): number {
  return Math.max(1, Math.floor(claimBudget / limit))
}

/**
 * Splits an ordered batch into transaction-sized runs by accumulating the limit
 * of every request, so a run never asks for more than `claimBudget` jobs even
 * when the limits differ. A request above the budget cannot be split and takes a
 * transaction of its own. Order is preserved, so results still map to requests
 * one to one.
 */
export function chunkClaims<T>(items: readonly T[], limitOf: (item: T) => number): T[][] {
  const chunks: T[][] = []
  let current: T[] = []
  let claimed = 0

  for (const item of items) {
    const limit = limitOf(item)
    if (current.length > 0 && claimed + limit > claimBudget) {
      chunks.push(current)
      current = []
      claimed = 0
    }
    current.push(item)
    claimed += limit
  }

  if (current.length > 0) chunks.push(current)
  return chunks
}
