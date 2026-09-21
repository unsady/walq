import { describe, expect, it } from 'vitest'

import {
  completeBatchInvalidReason,
  completeBatchScenarioName,
  completeBatchScenarios,
  fullCompleteBatchGrid,
  quickCompleteBatchGrid,
  summarizeCompleteBatchRuns,
  type CompleteBatchOutcome,
} from './complete-batch.js'
import type { Collected } from './harness.js'

describe('complete batch scenarios', () => {
  it('expands the batch tiers of both grids', () => {
    expect(completeBatchScenarios(quickCompleteBatchGrid).map(completeBatchScenarioName)).toEqual([
      'complete batch 1',
      'complete batch 4',
      'complete batch 16',
    ])
    expect(completeBatchScenarios(fullCompleteBatchGrid)).toHaveLength(3)
  })
})

function outcome(overrides: Partial<CompleteBatchOutcome> = {}): CompleteBatchOutcome {
  return {
    applied: 6,
    commitSamples: [10, 10],
    workloadDuration: 20,
    error: undefined,
    ...overrides,
  }
}

function collected(
  outcomes: CompleteBatchOutcome[],
  failures: string[] = [],
): Collected<CompleteBatchOutcome> {
  return { outcomes, failures }
}

describe('completeBatchInvalidReason', () => {
  it('rejects incomplete and erroring runs', () => {
    expect(completeBatchInvalidReason(outcome(), 6)).toBeUndefined()
    expect(completeBatchInvalidReason(outcome({ applied: 5 }), 6)).toBe('completed 5 of 6 jobs')
    expect(completeBatchInvalidReason(outcome({ error: 'boom' }), 6)).toBe('boom')
  })
})

describe('summarizeCompleteBatchRuns', () => {
  it('divides summed commit time by the jobs actually applied, not by the batch size', () => {
    // jobs 6 with batch 4: the second commit applies only 2 jobs, so the per-job mean is
    // (10 + 10) ms / 6 = 3333.33 µs, not 10 ms / 4 = 2500 µs.
    const result = summarizeCompleteBatchRuns({ batch: 4 }, 6, collected([outcome()]))

    expect(result.metrics['per-job mean (µs)']).toBeCloseTo(3333.33, 1)
    expect(result.metrics.commits).toBe(2)
  })

  it('matches the batch-size division when every batch is full', () => {
    const result = summarizeCompleteBatchRuns({ batch: 4 }, 8, collected([outcome({ applied: 8 })]))

    expect(result.metrics['per-job mean (µs)']).toBeCloseTo(2500, 6)
  })

  it('aggregates the per-job mean by median across runs instead of pooling', () => {
    const result = summarizeCompleteBatchRuns(
      { batch: 4 },
      8,
      collected([
        outcome({ applied: 8, commitSamples: [4, 4] }),
        outcome({ applied: 8, commitSamples: [40, 40] }),
        outcome({ applied: 8, commitSamples: [400, 400] }),
      ]),
    )

    // Per-run per-job costs are 1000, 10000 and 100000 µs; the median is 10000 µs.
    expect(result.metrics['per-job mean (µs)']).toBeCloseTo(10_000, 6)
  })

  it('excludes invalid runs from the metrics', () => {
    const result = summarizeCompleteBatchRuns({ batch: 4 }, 6, collected([outcome({ applied: 5 })]))

    expect(result.samples).toHaveLength(0)
    expect(result.ok).toBe(false)
    expect(result.notes).toContain('completed 5 of 6 jobs')
  })
})
