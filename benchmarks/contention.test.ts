import { describe, expect, it } from 'vitest'

import {
  aggregateReports,
  contentionScenarios,
  fullContentionGrid,
  invalidReason,
  quickContentionGrid,
  summarizeRuns,
  type ContentionRunOutcome,
  type ContentionScenario,
} from './contention.js'
import type { DrainReport, EnqueueReport } from './fixtures/contention-worker.js'
import type { Collected } from './harness.js'

function enqueueReport(overrides: Partial<EnqueueReport> = {}): EnqueueReport {
  return {
    phase: 'enqueue',
    startedAt: 100,
    finishedAt: 200,
    samples: [0.01, 0.02],
    errors: 0,
    aborted: false,
    firstError: null,
    ...overrides,
  }
}

function drainReport(overrides: Partial<DrainReport> = {}): DrainReport {
  return {
    phase: 'drain',
    startedAt: 200,
    finishedAt: 400,
    completed: 2,
    lostLeases: 0,
    completedIds: ['a', 'b'],
    claims: 2,
    emptyClaims: 1,
    claimSamples: [0.02, 0.03],
    completeSamples: [0.01, 0.01],
    errors: 0,
    aborted: false,
    firstError: null,
    ...overrides,
  }
}

function outcome(overrides: Partial<ContentionRunOutcome> = {}): ContentionRunOutcome {
  return {
    enqueueElapsed: 100,
    drainElapsed: 200,
    enqueued: 2,
    completed: 2,
    lostLeases: 0,
    duplicates: 0,
    claims: 2,
    emptyClaims: 0,
    errors: 0,
    aborted: false,
    firstError: null,
    enqueueSamples: [0.01],
    claimSamples: [0.02],
    completeSamples: [0.01],
    ...overrides,
  }
}

function collected(
  outcomes: ContentionRunOutcome[],
  failures: string[] = [],
): Collected<ContentionRunOutcome> {
  return { outcomes, failures }
}

const scenario: ContentionScenario = { threads: 4, batch: 1, placement: 'shared' }

describe('contentionScenarios', () => {
  it('skips the per-thread placement for a single thread', () => {
    const scenarios = contentionScenarios(quickContentionGrid)

    expect(scenarios.some((entry) => entry.threads === 1 && entry.placement === 'per-thread')).toBe(
      false,
    )
    expect(scenarios).toHaveLength(6)
    expect(contentionScenarios(fullContentionGrid)).toHaveLength(28)
  })

  it('keeps both placements of one configuration next to each other', () => {
    const scenarios = contentionScenarios(quickContentionGrid).filter(
      (entry) => entry.threads === 4,
    )

    expect(scenarios.map((entry) => entry.placement)).toEqual([
      'shared',
      'per-thread',
      'shared',
      'per-thread',
    ])
  })
})

describe('aggregateReports', () => {
  it('spans both phases and counts successful enqueues', () => {
    const outcome = aggregateReports(
      [enqueueReport({ startedAt: 50 }), enqueueReport({ startedAt: 100, finishedAt: 250 })],
      [drainReport()],
    )

    expect(outcome.enqueueElapsed).toBe(200)
    expect(outcome.drainElapsed).toBe(200)
    expect(outcome.enqueued).toBe(4)
    expect(outcome.completed).toBe(2)
    expect(outcome.duplicates).toBe(0)
  })

  it('detects the same job completed by two threads', () => {
    const outcome = aggregateReports(
      [enqueueReport()],
      [
        drainReport({ completed: 2, completedIds: ['a', 'b'] }),
        drainReport({ completed: 1, completedIds: ['a'] }),
      ],
    )

    expect(outcome.completed).toBe(3)
    expect(outcome.duplicates).toBe(1)
  })

  it('collects errors from both phases', () => {
    const outcome = aggregateReports(
      [enqueueReport({ errors: 1, firstError: 'SQLITE_BUSY' })],
      [drainReport({ errors: 2 })],
    )

    expect(outcome.errors).toBe(3)
    expect(outcome.firstError).toBe('SQLITE_BUSY')
  })
})

describe('invalidReason', () => {
  it('accepts a complete run', () => {
    expect(invalidReason(outcome(), 2)).toBeUndefined()
  })

  it('rejects aborted, short, lost and erroring runs', () => {
    expect(invalidReason(outcome({ aborted: true }), 2)).toBe('run aborted')
    expect(invalidReason(outcome({ enqueued: 1 }), 2)).toBe('enqueued 1 of 2 jobs')
    expect(invalidReason(outcome({ completed: 1 }), 2)).toBe('confirmed 1 of 2 jobs')
    expect(invalidReason(outcome({ lostLeases: 3 }), 2)).toBe('3 leases lost')
    expect(invalidReason(outcome({ duplicates: 1 }), 2)).toBe('1 duplicate completions')
    expect(invalidReason(outcome({ errors: 4 }), 2)).toBe('4 storage errors')
  })
})

describe('summarizeRuns', () => {
  it('excludes invalid runs from the metrics', () => {
    const result = summarizeRuns(
      scenario,
      2,
      collected([
        outcome({ drainElapsed: 100 }),
        outcome({ drainElapsed: 200 }),
        outcome({ drainElapsed: 300 }),
        outcome({ drainElapsed: 10, errors: 5 }),
      ]),
    )

    expect(result.metrics['drain jobs/sec']).toBe(10)
    expect(result.metrics['drain (ms)']).toBe(200)
    expect(result.metrics.errors).toBe(5)
    expect(result.samples).toHaveLength(3)
    expect(result.notes).toEqual(['1 of 4 runs are invalid: 5 storage errors'])
    expect(result.ok).toBe(false)
  })

  it('reports no usable run without dividing by zero', () => {
    const result = summarizeRuns(scenario, 2, collected([], ['run timed out']))

    expect(result.metrics['drain jobs/sec']).toBe(0)
    expect(result.metrics['empty claims']).toBe(0)
    expect(result.notes).toEqual(['1 of 1 runs failed: run timed out'])
    expect(result.ok).toBe(false)
  })
})
