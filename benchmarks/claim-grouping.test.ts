import { describe, expect, it } from 'vitest'

import {
  claimGroupingScenarioName,
  claimGroupingScenarios,
  claimLimitOverride,
  claimQueueOverride,
  fullClaimGroupingGrid,
  invalidReason,
  minimumCompetitorSamples,
  measureClaimRound,
  quickClaimGroupingGrid,
  summarizeRuns,
  withClaimGroupingTiers,
  type ClaimGroupingOutcome,
  type ClaimGroupingScenario,
} from './claim-grouping.js'
import type { ClaimCompetitorReport } from './fixtures/claim-competitor-worker.js'
import type { Collected } from './harness.js'

describe('claim round event-loop probe (measurement contract)', () => {
  it('observes every turn throughout the round', async () => {
    const samples: number[] = []
    const result = await measureClaimRound(async () => {
      await new Promise<void>((resolve) => setImmediate(resolve))
      await new Promise<void>((resolve) => setImmediate(resolve))

      return 42
    }, samples)

    expect(result.value).toBe(42)
    expect(result.duration).toBeGreaterThanOrEqual(0)
    expect(samples).toHaveLength(3)
  })

  it('stops the probe when a claim rejects', async () => {
    const samples: number[] = []

    await expect(
      measureClaimRound(() => {
        throw new Error('claim rejected')
      }, samples),
    ).rejects.toThrow('claim rejected')
    expect(samples).toHaveLength(1)
    await new Promise<void>((resolve) => setImmediate(resolve))
    expect(samples).toHaveLength(1)
  })
})

describe('claim grouping scenarios', () => {
  it('keeps two production smoke cases in quick and eight in full', () => {
    expect(claimGroupingScenarios(quickClaimGroupingGrid).map(claimGroupingScenarioName)).toEqual([
      'production / solo / 32 queues / limit 16',
      'production / competing / 32 queues / limit 16',
    ])

    const scenarios = claimGroupingScenarios(fullClaimGroupingGrid)

    expect(scenarios).toHaveLength(8)
    expect(new Set(scenarios.map(claimGroupingScenarioName)).size).toBe(8)
    expect(new Set(scenarios.map((scenario) => scenario.queues))).toEqual(new Set([1, 32]))
    expect(new Set(scenarios.map((scenario) => scenario.limit))).toEqual(new Set([1, 16]))
    expect(new Set(scenarios.map((scenario) => scenario.placement))).toEqual(
      new Set(['solo', 'competing']),
    )
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

  it('replaces only the provided tiers and keeps the source grid unchanged', () => {
    const grid = withClaimGroupingTiers(quickClaimGroupingGrid, {
      queues: [32, 64, 128],
      limits: [16],
    })

    expect(grid.queues).toEqual([32, 64, 128])
    expect(grid.limits).toEqual([16])
    expect(grid.placements).toEqual(quickClaimGroupingGrid.placements)
    expect(quickClaimGroupingGrid.queues).toEqual([32])
    expect(claimGroupingScenarios(grid)).toHaveLength(6)
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
    placement: 'solo',
  }

  it('labels the production claimQueues call as an API call, not a transaction', () => {
    const result = summarizeRuns(production, 4, collected([outcome()]))

    expect(result.metrics['claim call p50 (µs)']).toBe(10000)
    expect(result.metrics['jobs/claim call']).toBe(2)
    expect(result.metrics['claim calls']).toBe(2)
    expect(result.metrics['transaction p50 (µs)']).toBeUndefined()
    expect(result.metrics['jobs/transaction']).toBeUndefined()
    expect(result.samples[0]?.calls).toBe(2)
  })

  it('excludes invalid runs and notes the reason', () => {
    const result = summarizeRuns(production, 4, collected([outcome(), outcome({ claimed: 1 })]))

    expect(result.samples).toHaveLength(1)
    expect(result.ok).toBe(false)
    expect(result.notes).toContain('claimed 1 of 4 jobs')
  })
})
