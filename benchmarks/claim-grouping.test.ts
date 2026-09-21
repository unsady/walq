import { describe, expect, it } from 'vitest'

import {
  claimChunkOverride,
  claimGroupingScenarioName,
  claimGroupingScenarios,
  claimLimitOverride,
  claimModeOverride,
  claimQueueOverride,
  fullClaimGroupingGrid,
  quickClaimGroupingGrid,
  withClaimGroupingTiers,
} from './claim-grouping.js'

describe('claim grouping scenarios', () => {
  it('builds the quick matrix', () => {
    const scenarios = claimGroupingScenarios(quickClaimGroupingGrid)

    expect(scenarios).toHaveLength(36)
    expect(new Set(scenarios.map(claimGroupingScenarioName)).size).toBe(36)
  })

  it('covers every requested queue count and limit in the full matrix', () => {
    const scenarios = claimGroupingScenarios(fullClaimGroupingGrid)

    expect(scenarios).toHaveLength(72)
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

  it('parses chunk tiers with an explicit all marker', () => {
    expect(claimChunkOverride('all,16,32,64')).toEqual([undefined, 16, 32, 64])
    expect(claimChunkOverride('32')).toEqual([32])
    expect(claimChunkOverride(undefined)).toBeUndefined()
  })

  it('parses claim modes', () => {
    expect(claimModeOverride('grouped')).toEqual(['grouped'])
    expect(claimModeOverride('current,grouped,production')).toEqual([
      'current',
      'grouped',
      'production',
    ])
    expect(claimModeOverride('')).toBeUndefined()
  })

  it('rejects invalid tiers instead of defaulting silently', () => {
    expect(() => claimQueueOverride('32,x')).toThrow('BENCH_CLAIM_QUEUES')
    expect(() => claimQueueOverride('0')).toThrow('BENCH_CLAIM_QUEUES')
    expect(() => claimLimitOverride('1.5')).toThrow('BENCH_CLAIM_LIMITS')
    expect(() => claimChunkOverride('big')).toThrow('BENCH_CLAIM_CHUNKS')
    expect(() => claimModeOverride('batched')).toThrow('BENCH_CLAIM_MODES')
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
    expect(claimGroupingScenarios(grid)).toHaveLength(18)
  })

  it('keeps chunk tiers out of the current and production modes', () => {
    const grid = withClaimGroupingTiers(quickClaimGroupingGrid, {
      queues: [32],
      limits: [16],
      modes: ['production'],
      chunks: [undefined, 16],
    })

    expect(claimGroupingScenarios(grid).map(claimGroupingScenarioName)).toEqual([
      'production / solo / 32 queues / limit 16',
      'production / competing / 32 queues / limit 16',
    ])
  })

  it('keeps the base name until chunks are configured, then labels every chunk tier', () => {
    const base = withClaimGroupingTiers(quickClaimGroupingGrid, { queues: [128], limits: [16] })
    expect(claimGroupingScenarios(base).map(claimGroupingScenarioName)).toContain(
      'grouped / solo / 128 queues / limit 16',
    )

    const chunked = withClaimGroupingTiers(quickClaimGroupingGrid, {
      queues: [128],
      limits: [16],
      modes: ['grouped'],
      chunks: [undefined, 16, 32, 64],
    })
    const scenarios = claimGroupingScenarios(chunked)
    expect(scenarios).toHaveLength(8)
    expect(scenarios.map(claimGroupingScenarioName)).toEqual(
      expect.arrayContaining([
        'grouped / solo / 128 queues / limit 16 / chunk all',
        'grouped / solo / 128 queues / limit 16 / chunk 16',
        'grouped / competing / 128 queues / limit 16 / chunk 32',
        'grouped / competing / 128 queues / limit 16 / chunk 64',
      ]),
    )
  })
})
