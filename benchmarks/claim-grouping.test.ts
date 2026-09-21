import { describe, expect, it } from 'vitest'

import {
  claimGroupingScenarioName,
  claimGroupingScenarios,
  claimLimitOverride,
  claimQueueOverride,
  fullClaimGroupingGrid,
  quickClaimGroupingGrid,
  withClaimGroupingTiers,
} from './claim-grouping.js'

describe('claim grouping scenarios', () => {
  it('builds the quick matrix', () => {
    const scenarios = claimGroupingScenarios(quickClaimGroupingGrid)

    expect(scenarios).toHaveLength(24)
    expect(new Set(scenarios.map(claimGroupingScenarioName)).size).toBe(24)
  })

  it('covers every requested queue count and limit in the full matrix', () => {
    const scenarios = claimGroupingScenarios(fullClaimGroupingGrid)

    expect(scenarios).toHaveLength(48)
    expect(new Set(scenarios.map((scenario) => scenario.queues))).toEqual(new Set([1, 4, 8, 32]))
    expect(new Set(scenarios.map((scenario) => scenario.limit))).toEqual(new Set([1, 4, 16]))
  })
})

describe('claim grouping tier overrides', () => {
  it('parses comma-separated queue and limit lists', () => {
    expect(claimQueueOverride('32, 64,128')).toEqual([32, 64, 128])
    expect(claimLimitOverride('16')).toEqual([16])
    expect(claimQueueOverride(undefined)).toBeUndefined()
    expect(claimLimitOverride('')).toBeUndefined()
  })

  it('rejects invalid tiers instead of defaulting silently', () => {
    expect(() => claimQueueOverride('32,x')).toThrow('BENCH_CLAIM_QUEUES')
    expect(() => claimQueueOverride('0')).toThrow('BENCH_CLAIM_QUEUES')
    expect(() => claimLimitOverride('1.5')).toThrow('BENCH_CLAIM_LIMITS')
  })

  it('replaces only the provided tiers and keeps the rest of the grid', () => {
    const grid = withClaimGroupingTiers(quickClaimGroupingGrid, {
      queues: [32, 64, 128],
      limits: [16],
    })

    expect(grid.queues).toEqual([32, 64, 128])
    expect(grid.limits).toEqual([16])
    expect(grid.modes).toEqual(quickClaimGroupingGrid.modes)
    expect(grid.placements).toEqual(quickClaimGroupingGrid.placements)
    expect(claimGroupingScenarios(grid)).toHaveLength(12)
  })
})
