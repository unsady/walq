import type { BenchEnvironment } from './bench-options.js'
import {
  executeRun,
  invalidReason,
  summarizeRuns,
  type ContentionRunOutcome,
} from './contention.js'
import { matches, summarizePerRunMicros, type BenchmarkResult, type Collected } from './harness.js'
import { defineScenario, type ScenarioDefinition } from './scenario.js'

export interface JournalScenario {
  threads: number
  batch: number
  journal: 'WAL' | 'DELETE'
}

export interface JournalResult extends BenchmarkResult {
  /** Raw successful-call timings and diagnostics, including invalid runs. Milliseconds. */
  runs: ContentionRunOutcome[]
}

export function scenarioName(scenario: JournalScenario): string {
  return `${scenario.journal} / ${scenario.threads} workers / batch ${scenario.batch}`
}

export function summarizeJournal(
  scenario: JournalScenario,
  jobs: number,
  collected: Collected<ContentionRunOutcome>,
): JournalResult {
  const base = summarizeRuns({ ...scenario, placement: 'shared' }, jobs, collected)
  const valid = collected.outcomes.filter((outcome) => invalidReason(outcome, jobs) === undefined)
  const claim = summarizePerRunMicros(valid.map((outcome) => outcome.claimSamples))
  const complete = summarizePerRunMicros(valid.map((outcome) => outcome.completeSamples))
  const empty = summarizePerRunMicros(valid.map((outcome) => outcome.emptyClaimSamples))
  const loop = summarizePerRunMicros(valid.map((outcome) => outcome.loopSamples))
  const completed = valid.reduce((sum, outcome) => sum + outcome.completed, 0)
  const elapsed = valid.reduce((sum, outcome) => sum + outcome.drainElapsed, 0)

  return {
    ...base,
    suite: 'journal',
    scenario: scenarioName(scenario),
    params: { ...base.params, journal: scenario.journal, synchronous: 'FULL', busyTimeout: 2000 },
    metrics: {
      'jobs/sec': elapsed === 0 ? 0 : (completed / elapsed) * 1000,
      'claim p95 (µs)': claim.p95,
      'complete p95 (µs)': complete.p95,
      'empty claim p95 (µs)': empty.p95,
      'event-loop p95 (µs)': loop.p95,
      'event-loop max (µs)': loop.max,
      'claim samples': claim.count,
      'complete samples': complete.count,
      'empty samples': empty.count,
      'loop samples': loop.count,
      SQLITE_BUSY: collected.outcomes.reduce((sum, outcome) => sum + outcome.busy, 0),
      timeouts:
        collected.outcomes.reduce((sum, outcome) => sum + outcome.timeouts, 0) +
        collected.failures.filter((message) => message.includes('timed out')).length,
      errors: base.metrics.errors ?? 0,
    },
    samples: collected.outcomes.map((outcome) => ({
      valid: invalidReason(outcome, jobs) === undefined ? 1 : 0,
      completed: outcome.completed,
      'drain (ms)': outcome.drainElapsed,
      SQLITE_BUSY: outcome.busy,
      timeouts: outcome.timeouts,
      errors: outcome.errors,
    })),
    runs: collected.outcomes,
  }
}

export function definitions(environment: BenchEnvironment): ScenarioDefinition[] {
  const jobs = environment.jobs ?? (environment.grid === 'full' ? 10_000 : 2000)
  const scenarios: JournalScenario[] = [1, 4].flatMap((threads) =>
    [1, 16].flatMap((batch) =>
      (['WAL', 'DELETE'] as const).map((journal) => ({ threads, batch, journal })),
    ),
  )

  return scenarios
    .filter((scenario) => matches(scenarioName(scenario), environment.only))
    .map((scenario) =>
      defineScenario({
        suite: 'journal',
        scenario: scenarioName(scenario),
        jobs,
        run: () =>
          executeRun({ ...scenario, placement: 'shared' }, jobs, 'full', scenario.journal, true),
        summarize: (collected) => summarizeJournal(scenario, jobs, collected),
      }),
    )
}
