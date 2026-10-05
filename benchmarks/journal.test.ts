import { describe, expect, it } from 'vitest'

import { readBenchEnvironment } from './bench-options.js'
import type { DrainReport } from './fixtures/journal-worker.js'
import {
  invalidReason,
  outcomeFromReport,
  type JournalRunOutcome,
  type JournalScenario,
} from './journal.js'
import { applyDurabilityComparison, definitions, summarizeJournal } from './journal.suite.js'

function outcome(overrides: Partial<JournalRunOutcome> = {}): JournalRunOutcome {
  return {
    drainElapsed: 200,
    completed: 2,
    lostLeases: 0,
    duplicates: 0,
    claims: 2,
    emptyClaims: 1,
    errors: 0,
    aborted: false,
    firstError: null,
    claimSamples: [0.02],
    completeSamples: [0.01],
    emptyClaimSamples: [],
    loopSamples: [],
    busy: 0,
    timeouts: 0,
    ...overrides,
  }
}

const scenario: JournalScenario = { batch: 16, journal: 'WAL', synchronous: 'full' }

function result(synchronous: 'normal' | 'full', duration: number, batch = 16) {
  return summarizeJournal({ ...scenario, synchronous, batch }, 2, {
    outcomes: [outcome({ drainElapsed: duration })],
    failures: [],
  })
}

describe('journal comparison', () => {
  it('keeps six single-connection cases, reusing WAL/FULL for both comparisons', () => {
    const expected = [1, 16].flatMap((batch) => [
      `WAL / NORMAL / batch ${batch}`,
      `WAL / FULL / batch ${batch}`,
      `DELETE / FULL / batch ${batch}`,
    ])

    for (const grid of ['quick', 'full']) {
      const scenarios = definitions(
        readBenchEnvironment({ BENCH_GRID: grid, BENCH_SYNCHRONOUS: 'normal' }),
      )

      expect(scenarios.map((definition) => definition.scenario)).toEqual(expected)
      expect(
        scenarios.every((definition) => definition.jobs === (grid === 'full' ? 10000 : 2000)),
      ).toBe(true)
    }

    const filtered = definitions(readBenchEnvironment({ BENCH_ONLY: 'DELETE', BENCH_JOBS: '17' }))
    expect(filtered).toHaveLength(2)
    expect(filtered.every((definition) => definition.jobs === 17)).toBe(true)
  })

  it('uses elapsed totals, separates latencies and retains invalid diagnostics', () => {
    const runs = [
      outcome({ drainElapsed: 100, emptyClaimSamples: [0.3], loopSamples: [1] }),
      outcome({ drainElapsed: 300, emptyClaimSamples: [0.5], loopSamples: [2] }),
      outcome({ drainElapsed: 1, busy: 2, timeouts: 1, errors: 2 }),
    ]
    const summary = summarizeJournal(scenario, 2, { outcomes: runs, failures: ['run timed out'] })

    expect(summary.params.synchronous).toBe('FULL')
    expect(summary.params.threads).toBe(1)
    expect(summary.metrics['jobs/sec']).toBe(10)
    expect(summary.metrics['claim p95 (µs)']).toBe(20)
    expect(summary.metrics['complete p95 (µs)']).toBe(10)
    expect(summary.metrics['empty claim p95 (µs)']).toBe(300)
    expect(summary.metrics['event-loop p95 (µs)']).toBe(1000)
    expect(summary.metrics['empty samples']).toBe(2)
    expect(summary.metrics.SQLITE_BUSY).toBe(2)
    expect(summary.metrics.timeouts).toBe(2)
    expect(summary.samples.map((sample) => sample.valid)).toEqual([1, 1, 0])
    expect(summary.runs).toEqual(runs)
    expect(summary.ok).toBe(false)
    expect(result('normal', 200).params.synchronous).toBe('NORMAL')
  })

  it('reports failed or empty runs without fabricated latency samples', () => {
    const summary = summarizeJournal(scenario, 2, { outcomes: [], failures: ['run timed out'] })

    expect(summary.metrics['jobs/sec']).toBe(0)
    expect(summary.metrics['claim samples']).toBe(0)
    expect(summary.metrics.timeouts).toBe(1)
    expect(summary.ok).toBe(false)
    expect(summarizeJournal(scenario, 2, { outcomes: [], failures: [] }).ok).toBe(false)
  })
})

describe('durability comparison', () => {
  it('computes FULL throughput drop against the matching NORMAL baseline', () => {
    const results = [
      result('normal', 100),
      result('full', 400),
      result('normal', 200, 1),
      result('full', 100, 1),
    ]
    applyDurabilityComparison(results)

    expect(results[1]!.metrics['FULL drop (%)']).toBe(75)
    expect(results[3]!.metrics['FULL drop (%)']).toBe(-100)
    expect(results[0]!.metrics['FULL drop (%)']).toBeUndefined()
  })

  it('does not fabricate a comparison for filtered, invalid or mismatched pairs', () => {
    const invalid = result('normal', 100)
    invalid.ok = false
    const otherJobs = result('normal', 100)
    otherJobs.params.jobs = 3
    const zero = result('normal', 100)
    zero.metrics['jobs/sec'] = 0
    const full = result('full', 200)

    for (const results of [
      [full],
      [invalid, full],
      [otherJobs, full],
      [zero, full],
      [result('normal', 100, 1), full],
    ]) {
      applyDurabilityComparison(results)
      expect(full.metrics['FULL drop (%)']).toBeUndefined()
    }

    const invalidFull = result('full', 200)
    invalidFull.ok = false
    const deleteFull = summarizeJournal({ ...scenario, journal: 'DELETE' }, 2, {
      outcomes: [outcome()],
      failures: [],
    })
    applyDurabilityComparison([result('normal', 100), invalidFull, deleteFull])
    expect(invalidFull.metrics['FULL drop (%)']).toBeUndefined()
    expect(deleteFull.metrics['FULL drop (%)']).toBeUndefined()
  })
})

describe('outcome validation', () => {
  it('preserves drain timings and detects duplicate completions', () => {
    const report: DrainReport = {
      ...outcome(),
      phase: 'drain',
      startedAt: 100,
      finishedAt: 300,
      completedIds: ['a', 'a'],
    }
    const run = outcomeFromReport(report)

    expect(run.drainElapsed).toBe(200)
    expect(run.duplicates).toBe(1)
    expect(run.completed).toBe(2)
    expect(invalidReason(run, 2)).toBe('1 duplicate completions')
  })

  it('accepts complete runs and rejects aborted, short, lost and erroring runs', () => {
    expect(invalidReason(outcome(), 2)).toBeUndefined()
    expect(invalidReason(outcome({ aborted: true }), 2)).toBe('run aborted')
    expect(invalidReason(outcome({ completed: 1 }), 2)).toBe('confirmed 1 of 2 jobs')
    expect(invalidReason(outcome({ lostLeases: 3 }), 2)).toBe('3 leases lost')
    expect(invalidReason(outcome({ errors: 4 }), 2)).toBe('4 storage errors')
    expect(invalidReason(outcome({ drainElapsed: 0 }), 2)).toBe('invalid drain duration')
  })
})
