import { describe, expect, it } from 'vitest'

import {
  formatCount,
  fullRetentionGrid,
  quickRetentionGrid,
  retentionBatchOverride,
  retentionInvalidReason,
  retentionScenarioName,
  retentionScenarios,
  summarizeRetentionRuns,
  withRetentionBatch,
  type RetentionOutcome,
  type RetentionScenario,
} from './retention.js'

function outcome(overrides: Partial<RetentionOutcome> = {}): RetentionOutcome {
  return {
    claimed: 100,
    completed: 100,
    duplicates: 0,
    lostLeases: 0,
    enqueueSamples: [1, 2, 3],
    claimSamples: [1, 2, 3],
    completeSamples: [1, 2, 3],
    eventLoopSamples: [1, 2, 3],
    workloadDuration: 100,
    errors: [],
    cleanup: 10,
    cleanupBatches: 2,
    cleanupBatchSamples: [1, 2],
    cleanupStallSamples: [1, 2],
    vacuum: 5,
    vacuumStall: 1,
    checkpoint: 2,
    before: { db: 1024 * 1024, wal: 0, pages: 100 },
    after: { db: 512 * 1024, wal: 0, pages: 50 },
    activeJobs: 100,
    ...overrides,
  }
}

describe('retention grid', () => {
  it('keeps only the zero and 25k baseline plus 25k cleanup in quick', () => {
    const scenarios = retentionScenarios(quickRetentionGrid)
    const described = scenarios.map((scenario) => `${scenario.history}/${scenario.cleanup}`).sort()

    expect(described).toEqual([
      '0/retained',
      '25000/delete',
      '25000/delete-vacuum',
      '25000/retained',
    ])
    expect(new Set(scenarios.map(retentionScenarioName)).size).toBe(4)
  })

  it('includes a million-row history in the full matrix', () => {
    const scenarios = retentionScenarios(fullRetentionGrid)

    expect(scenarios).toHaveLength(19)
    expect(new Set(scenarios.map(retentionScenarioName)).size).toBe(19)
    expect(scenarios.some((scenario) => scenario.history === 1_000_000)).toBe(true)
    expect(scenarios.some((scenario) => scenario.connection === 'reopened')).toBe(true)
  })

  it('keeps zero history as the retained baseline only', () => {
    for (const grid of [quickRetentionGrid, fullRetentionGrid]) {
      const zero = retentionScenarios(grid).filter((scenario) => scenario.history === 0)
      expect(zero.length).toBeGreaterThan(0)
      expect(zero.every((scenario) => scenario.cleanup === 'retained')).toBe(true)
    }
  })

  it('only applies batch sizes to cleanup scenarios', () => {
    const scenarios = retentionScenarios(quickRetentionGrid)
    const retained = scenarios.filter((scenario) => scenario.cleanup === 'retained')
    const cleaned = scenarios.filter((scenario) => scenario.cleanup !== 'retained')

    expect(retained.every((scenario) => scenario.batch === 0)).toBe(true)
    expect(cleaned.every((scenario) => scenario.batch > 0)).toBe(true)
  })

  it('collapses every tier to one batch size when overridden', () => {
    const grid = withRetentionBatch(fullRetentionGrid, 7_000)
    const scenarios = retentionScenarios(grid).filter((scenario) => scenario.cleanup !== 'retained')

    expect(scenarios.length).toBeGreaterThan(0)
    expect(scenarios.every((scenario) => scenario.batch === 7_000)).toBe(true)
  })

  it('parses a positive batch override and rejects invalid values', () => {
    expect(retentionBatchOverride(undefined)).toBeUndefined()
    expect(retentionBatchOverride('')).toBeUndefined()
    expect(retentionBatchOverride('2500')).toBe(2_500)
    expect(() => retentionBatchOverride('0')).toThrow('BENCH_RETENTION_BATCH')
    expect(() => retentionBatchOverride('-1')).toThrow('BENCH_RETENTION_BATCH')
    expect(() => retentionBatchOverride('1.5')).toThrow('BENCH_RETENTION_BATCH')
  })

  it('formats large counts compactly', () => {
    expect(formatCount(0)).toBe('0')
    expect(formatCount(1_000)).toBe('1k')
    expect(formatCount(25_000)).toBe('25k')
    expect(formatCount(250_000)).toBe('250k')
    expect(formatCount(1_000_000)).toBe('1M')
  })
})

describe('retentionInvalidReason', () => {
  it('accepts a complete run', () => {
    expect(retentionInvalidReason(outcome(), 100)).toBeUndefined()
  })

  it('rejects incomplete, duplicated, and lost work', () => {
    expect(retentionInvalidReason(outcome({ claimed: 99 }), 100)).toContain('claimed 99')
    expect(retentionInvalidReason(outcome({ completed: 98 }), 100)).toContain('completed 98')
    expect(retentionInvalidReason(outcome({ duplicates: 2 }), 100)).toContain('duplicate')
    expect(retentionInvalidReason(outcome({ lostLeases: 1 }), 100)).toContain('lost lease')
  })

  it('rejects storage errors', () => {
    expect(retentionInvalidReason(outcome({ errors: ['boom'] }), 100)).toBe('boom')
  })
})

describe('summarizeRetentionRuns', () => {
  const scenario: RetentionScenario = {
    history: 25_000,
    cleanup: 'delete-vacuum',
    batch: 10_000,
    connection: 'warm',
  }

  it('reports throughput, percentiles, cleanup, and sizes', () => {
    const result = summarizeRetentionRuns(scenario, 100, {
      outcomes: [outcome(), outcome({ workloadDuration: 300 })],
      failures: [],
    })

    expect(result.suite).toBe('retention')
    expect(result.scenario).toContain('delete-vacuum')
    expect(result.metrics['active jobs/sec']).toBeCloseTo(333.33, 1)
    expect(result.metrics['enqueue p50 (µs)']).toBeCloseTo(2_000, 6)
    expect(result.metrics['claim p99 (µs)']).toBeCloseTo(3_000, 6)
    expect(result.metrics['cleanup (ms)']).toBe(10)
    expect(result.metrics['vacuum (ms)']).toBe(5)
    expect(result.metrics['db before (MiB)']).toBe(1)
    expect(result.metrics['db after (MiB)']).toBe(0.5)
    expect(result.ok).toBe(true)
    expect(result.notes).toEqual([])
  })

  it('marks invalid runs and keeps the reason in the notes', () => {
    const result = summarizeRetentionRuns(scenario, 100, {
      outcomes: [outcome(), outcome({ completed: 50 })],
      failures: ['setup exploded'],
    })

    expect(result.ok).toBe(false)
    expect(result.notes).toEqual(['setup exploded', 'completed 50 of 100 jobs'])
    expect(result.metrics['active jobs/sec']).toBeCloseTo(1_000, 6)
  })

  it('reports zero rates without valid outcomes instead of dividing by zero', () => {
    const result = summarizeRetentionRuns(scenario, 100, {
      outcomes: [],
      failures: [],
    })

    expect(result.metrics['active jobs/sec']).toBe(0)
    expect(result.ok).toBe(true)
  })
})
