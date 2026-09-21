import { describe, expect, it } from 'vitest'

import {
  claimChunkOverride,
  claimGroupingScenarioName,
  claimGroupingScenarios,
  claimLimitOverride,
  claimModeOverride,
  claimQueueOverride,
  fullClaimGroupingGrid,
  invalidReason,
  minimumCompetitorSamples,
  quickClaimGroupingGrid,
  summarizeRuns,
  withClaimGroupingTiers,
  type ClaimGroupingOutcome,
  type ClaimGroupingScenario,
} from './claim-grouping.js'
import type { ClaimCompetitorReport } from './fixtures/claim-competitor-worker.js'
import type { Collected } from './harness.js'

describe('claim grouping scenarios', () => {
  it('builds the quick matrix', () => {
    const scenarios = claimGroupingScenarios(quickClaimGroupingGrid)

    expect(scenarios).toHaveLength(12)
    expect(new Set(scenarios.map((scenario) => scenario.mode))).toEqual(new Set(['production']))
    expect(new Set(scenarios.map(claimGroupingScenarioName)).size).toBe(12)
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
    expect(claimGroupingScenarios(grid)).toHaveLength(6)
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
    const base = withClaimGroupingTiers(quickClaimGroupingGrid, {
      queues: [128],
      limits: [16],
      modes: ['grouped'],
    })
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

function competitor(overrides: Partial<ClaimCompetitorReport> = {}): ClaimCompetitorReport {
  return {
    enqueueSamples: Array.from({ length: minimumCompetitorSamples }, () => 0.01),
    completeSamples: Array.from({ length: minimumCompetitorSamples }, () => 0.01),
    operations: minimumCompetitorSamples,
    errors: 0,
    firstError: null,
    ...overrides,
  }
}

function outcome(overrides: Partial<ClaimGroupingOutcome> = {}): ClaimGroupingOutcome {
  return {
    elapsed: 100,
    claimed: 4,
    duplicates: 0,
    calls: [
      { duration: 10, jobs: 2 },
      { duration: 10, jobs: 2 },
    ],
    eventLoopSamples: [1],
    competitor: competitor(),
    ...overrides,
  }
}

function collected(
  outcomes: ClaimGroupingOutcome[],
  failures: string[] = [],
): Collected<ClaimGroupingOutcome> {
  return { outcomes, failures }
}

describe('invalidReason', () => {
  it('accepts a complete solo run without competitor samples', () => {
    const solo = outcome({
      competitor: competitor({ enqueueSamples: [], completeSamples: [], operations: 0 }),
    })

    expect(invalidReason(solo, 4, 'solo')).toBeUndefined()
  })

  it('rejects a competing run with too few competitor samples', () => {
    const report = competitor({
      enqueueSamples: [0.01],
      completeSamples: [0.01, 0.02],
      operations: 2,
    })

    expect(invalidReason(outcome({ competitor: report }), 4, 'competing')).toBe(
      `competitor produced 1 enqueue and 2 complete samples, need ${minimumCompetitorSamples} of each`,
    )
  })

  it('accepts a competing run with enough samples', () => {
    expect(invalidReason(outcome(), 4, 'competing')).toBeUndefined()
  })

  it('rejects short, duplicated and erroring runs', () => {
    expect(invalidReason(outcome({ claimed: 3 }), 4, 'solo')).toBe('claimed 3 of 4 jobs')
    expect(invalidReason(outcome({ duplicates: 1 }), 4, 'solo')).toBe('1 duplicate claims')
    expect(
      invalidReason(
        outcome({ competitor: competitor({ errors: 2, firstError: 'boom' }) }),
        4,
        'competing',
      ),
    ).toBe('2 competitor errors: boom')
  })
})

describe('summarizeRuns metric labels', () => {
  const production: ClaimGroupingScenario = {
    queues: 8,
    limit: 16,
    mode: 'production',
    placement: 'solo',
    chunkSize: undefined,
    chunksConfigured: false,
  }
  const grouped: ClaimGroupingScenario = { ...production, mode: 'grouped' }

  it('labels the production claimQueues call as an API call, not a transaction', () => {
    const result = summarizeRuns(production, 4, collected([outcome()]))

    expect(result.metrics['claim call p50 (µs)']).toBe(10000)
    expect(result.metrics['jobs/claim call']).toBe(2)
    expect(result.metrics['claim calls']).toBe(2)
    expect(result.metrics['transaction p50 (µs)']).toBeUndefined()
    expect(result.metrics['jobs/transaction']).toBeUndefined()
    expect(result.samples[0]?.calls).toBe(2)
  })

  it('keeps transaction labels for the prototype modes', () => {
    const result = summarizeRuns(grouped, 4, collected([outcome()]))

    expect(result.metrics['transaction p50 (µs)']).toBe(10000)
    expect(result.metrics['jobs/transaction']).toBe(2)
    expect(result.metrics.commits).toBe(2)
    expect(result.metrics['claim call p50 (µs)']).toBeUndefined()
  })

  it('excludes invalid runs and notes the reason', () => {
    const result = summarizeRuns(grouped, 4, collected([outcome(), outcome({ claimed: 1 })]))

    expect(result.samples).toHaveLength(1)
    expect(result.ok).toBe(false)
    expect(result.notes).toContain('claimed 1 of 4 jobs')
  })
})
