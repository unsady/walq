import { describe, expect, it } from 'vitest'

import { readBenchEnvironment } from './bench-options.js'
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
import { definitions as journalDefinitions, summarizeJournal } from './journal.suite.js'

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
    emptyClaimSamples: [0.03],
    loopSamples: [],
    busy: 0,
    timeouts: 0,
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
    emptyClaimSamples: [],
    loopSamples: [],
    busy: 0,
    timeouts: 0,
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

describe('journal comparison', () => {
  it('pairs exactly the requested worker and batch configurations in both grids', () => {
    const expected = [1, 4].flatMap((threads) =>
      [1, 16].flatMap((batch) =>
        ['WAL', 'DELETE'].map((journal) => `${journal} / ${threads} workers / batch ${batch}`),
      ),
    )

    for (const grid of ['quick', 'full']) {
      const definitions = journalDefinitions(readBenchEnvironment({ BENCH_GRID: grid }))

      expect(definitions.map((definition) => definition.scenario)).toEqual(expected)
      expect(
        definitions.every((definition) => definition.jobs === (grid === 'full' ? 10000 : 2000)),
      ).toBe(true)
    }

    const filtered = journalDefinitions(
      readBenchEnvironment({ BENCH_ONLY: 'DELETE', BENCH_JOBS: '17' }),
    )
    expect(filtered).toHaveLength(4)
    expect(filtered.every((definition) => definition.jobs === 17)).toBe(true)
  })

  it('uses elapsed totals, separates latencies and retains invalid diagnostics', () => {
    const runs = [
      outcome({ drainElapsed: 100, emptyClaimSamples: [0.3], loopSamples: [1] }),
      outcome({ drainElapsed: 300, emptyClaimSamples: [0.5], loopSamples: [2] }),
      outcome({ drainElapsed: 1, busy: 2, timeouts: 1, errors: 2 }),
    ]
    const result = summarizeJournal(
      { threads: 4, batch: 16, journal: 'DELETE' },
      2,
      collected(runs, ['run timed out']),
    )

    expect(result.params.synchronous).toBe('FULL')
    expect(result.metrics['jobs/sec']).toBe(10)
    expect(result.metrics['claim p95 (µs)']).toBe(20)
    expect(result.metrics['complete p95 (µs)']).toBe(10)
    expect(result.metrics['empty claim p95 (µs)']).toBe(300)
    expect(result.metrics['event-loop p95 (µs)']).toBe(1000)
    expect(result.metrics['empty samples']).toBe(2)
    expect(result.metrics.SQLITE_BUSY).toBe(2)
    expect(result.metrics.timeouts).toBe(2)
    expect(result.samples.map((sample) => sample.valid)).toEqual([1, 1, 0])
    expect(result.runs).toEqual(runs)
    expect(result.ok).toBe(false)
  })

  it('reports a failed run without fabricated latency samples', () => {
    const result = summarizeJournal(
      { threads: 1, batch: 1, journal: 'WAL' },
      2,
      collected([], ['run timed out']),
    )

    expect(result.metrics['jobs/sec']).toBe(0)
    expect(result.metrics['claim samples']).toBe(0)
    expect(result.metrics.timeouts).toBe(1)
    expect(result.ok).toBe(false)
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
