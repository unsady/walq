import { Queue } from '@walq/core'
import { describe, expect, it, vi } from 'vitest'

import {
  coordinatorScenarios,
  defineCoordinatorScenario,
  fullCoordinatorGrid,
  invalidReason,
  quickCoordinatorGrid,
  scenarioName,
  summarizeRuns,
  type CoordinatorRunOutcome,
  type CoordinatorScenario,
} from './coordinator.js'
import { collectRuns, type Collected } from './harness.js'

function outcome(overrides: Partial<CoordinatorRunOutcome> = {}): CoordinatorRunOutcome {
  return {
    elapsed: 10,
    firstHandler: undefined,
    claims: 10,
    emptyClaims: 0,
    confirmed: 10,
    lostLeases: 0,
    duplicates: 0,
    groupedCalls: 0,
    groupedRequests: 0,
    claimSamples: [0.01],
    completeSamples: [0.005],
    ...overrides,
  }
}

function collected(
  outcomes: CoordinatorRunOutcome[],
  failures: string[] = [],
): Collected<CoordinatorRunOutcome> {
  return { outcomes, failures }
}

const scenario: CoordinatorScenario = { mode: 'shared', queues: 8, profile: 'saturated' }

describe('cleanup', () => {
  it.each([false, true])('aborts on stuck close even when the handler failed: %s', async (fail) => {
    vi.useFakeTimers()
    const original = Queue.prototype.process
    const close = vi.fn<() => Promise<void>>(async () => new Promise<void>(() => {}))
    const process = vi.spyOn(Queue.prototype, 'process').mockImplementation(function (
      this: Queue<unknown>,
      processor,
      options,
    ) {
      const worker = original.call(
        this,
        fail
          ? async () => {
              throw new Error('injected handler failure')
            }
          : processor,
        options,
      )
      return {
        close: async () => {
          await worker.close()
          await close()
        },
      }
    })
    try {
      const definitions = (['shared', 'isolated'] as const).map((mode) =>
        defineCoordinatorScenario({ mode, queues: 1, profile: 'saturated' }, 1),
      )
      const pending = collectRuns(
        definitions.map((definition) => definition.descriptor),
        { jobs: 1, repeats: 1, warmup: 0, only: undefined, report: () => {} },
        (descriptor) => descriptor.run(),
      )
      await vi.advanceTimersByTimeAsync(1_000)
      const collected = await pending
      const results = definitions.map((definition) =>
        definition.descriptor.summarize(
          collected.scenarios.get(definition.descriptor) ?? { outcomes: [], failures: [] },
        ),
      )

      expect(collected.abortReason).toBe('workers did not stop within 1000ms')
      expect(process).toHaveBeenCalledTimes(1)
      expect(close).toHaveBeenCalledTimes(1)
      expect(results).toHaveLength(2)
      expect(results.every((result) => !result.ok && result.samples.length === 0)).toBe(true)
      expect(vi.getTimerCount()).toBe(0)
    } finally {
      process.mockRestore()
      vi.useRealTimers()
    }
  })
})

describe('coordinatorScenarios', () => {
  it('collapses the single-queue variants that execute identically', () => {
    const single = coordinatorScenarios(quickCoordinatorGrid).filter(
      (scenario) => scenario.queues === 1,
    )

    // One queue shares a single coordinator either way, and `sparse` cannot thin it,
    // so only the preloaded and bursty baselines remain.
    expect(single.map((scenario) => `${scenario.profile}/${scenario.mode}`)).toEqual([
      'saturated/shared',
      'bursty/shared',
    ])
  })

  it('keeps both modes of one multi-queue configuration next to each other', () => {
    const modes = coordinatorScenarios(quickCoordinatorGrid)
      .filter((scenario) => scenario.queues > 1)
      .map((scenario) => scenario.mode)

    expect(modes).toEqual(['shared', 'isolated', 'shared', 'isolated', 'shared', 'isolated'])
  })

  it('keeps one scenario per distinct workload', () => {
    for (const grid of [quickCoordinatorGrid, fullCoordinatorGrid]) {
      const scenarios = coordinatorScenarios(grid)

      expect(new Set(scenarios.map(scenarioName)).size).toBe(scenarios.length)
    }
    expect(coordinatorScenarios(quickCoordinatorGrid)).toHaveLength(8)
    expect(coordinatorScenarios(fullCoordinatorGrid)).toHaveLength(20)
  })
})

describe('invalidReason', () => {
  it('accepts a complete run', () => {
    expect(invalidReason(outcome(), 10)).toBeUndefined()
  })

  it('rejects incomplete, lost and duplicated work', () => {
    expect(invalidReason(outcome({ confirmed: 9 }), 10)).toBe('confirmed 9 of 10 jobs')
    expect(invalidReason(outcome({ lostLeases: 1 }), 10)).toBe('1 leases lost')
    expect(invalidReason(outcome({ duplicates: 2 }), 10)).toBe('2 duplicate completions')
  })
})

describe('summarizeRuns', () => {
  it('computes metrics from valid runs only', () => {
    const result = summarizeRuns(
      scenario,
      10,
      collected([
        outcome({ elapsed: 10 }),
        outcome({ elapsed: 20 }),
        outcome({ elapsed: 30 }),
        outcome({ elapsed: 1, confirmed: 1 }),
      ]),
    )

    // The failed run is the fastest one, so it must not move the median.
    expect(result.metrics['jobs/sec']).toBe(500)
    expect(result.metrics['elapsed (ms)']).toBe(20)
    expect(result.samples).toHaveLength(3)
    expect(result.notes).toEqual(['1 of 4 runs are invalid: confirmed 1 of 10 jobs'])
    expect(result.ok).toBe(false)
  })

  it('reports failed runs and stays valid when every measured run succeeded', () => {
    const clean = summarizeRuns(scenario, 10, collected([outcome(), outcome()]))

    expect(clean.ok).toBe(true)
    expect(clean.notes).toEqual([])

    const broken = summarizeRuns(scenario, 10, collected([outcome()], ['run timed out']))
    expect(broken.notes).toEqual(['1 of 2 runs failed: run timed out'])
    expect(broken.ok).toBe(false)
  })

  it('aggregates latency by the median across runs instead of pooling samples', () => {
    const result = summarizeRuns(
      scenario,
      10,
      collected([
        outcome({ claimSamples: [1, 1, 1, 1] }),
        outcome({ claimSamples: [10] }),
        outcome({ claimSamples: [20] }),
      ]),
    )

    // Per-run p95 is 1, 10 and 20 ms; their median is 10 ms, not the pooled percentile.
    expect(result.metrics['claim p95 (µs)']).toBe(10_000)
  })

  it('reports grouped-call metrics when the adapter batches requests', () => {
    const result = summarizeRuns(
      scenario,
      10,
      collected([
        outcome({ groupedCalls: 2, groupedRequests: 8, claims: 8 }),
        outcome({ groupedCalls: 2, groupedRequests: 4, claims: 4 }),
      ]),
    )

    expect(result.metrics['grouped calls']).toBe(2)
    expect(result.metrics['requests/grouped call']).toBe(3)
  })

  it('survives a scenario without a single usable run', () => {
    const result = summarizeRuns(scenario, 10, collected([], ['run timed out']))

    expect(result.metrics['jobs/sec']).toBe(0)
    expect(result.metrics['claims/job']).toBe(0)
    expect(result.samples).toEqual([])
    expect(result.ok).toBe(false)
  })
})
