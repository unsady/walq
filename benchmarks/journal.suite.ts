import type { BenchEnvironment } from './bench-options.js'
import { matches, summarizePerRunMicros, type BenchmarkResult, type Collected } from './harness.js'
import {
  executeRun,
  invalidReason,
  type JournalRunOutcome,
  type JournalScenario,
} from './journal.js'
import { defineScenario, type ScenarioDefinition } from './scenario.js'

export interface JournalResult extends BenchmarkResult {
  /** Raw successful-call timings and diagnostics, including invalid runs. Milliseconds. */
  runs: JournalRunOutcome[]
}

export function scenarioName(scenario: JournalScenario): string {
  return `${scenario.journal} / ${scenario.synchronous.toUpperCase()} / batch ${scenario.batch}`
}

export function summarizeJournal(
  scenario: JournalScenario,
  jobs: number,
  collected: Collected<JournalRunOutcome>,
): JournalResult {
  const valid = collected.outcomes.filter((outcome) => invalidReason(outcome, jobs) === undefined)
  const invalid = collected.outcomes
    .map((outcome) => invalidReason(outcome, jobs))
    .filter((reason): reason is string => reason !== undefined)
  const notes = [...collected.failures]

  if (invalid.length > 0) notes.push(...invalid)
  if (valid.length === 0) notes.push('No valid measured runs')

  const claim = summarizePerRunMicros(valid.map((outcome) => outcome.claimSamples))
  const complete = summarizePerRunMicros(valid.map((outcome) => outcome.completeSamples))
  const empty = summarizePerRunMicros(valid.map((outcome) => outcome.emptyClaimSamples))
  const loop = summarizePerRunMicros(valid.map((outcome) => outcome.loopSamples))
  const completed = valid.reduce((sum, outcome) => sum + outcome.completed, 0)
  const elapsed = valid.reduce((sum, outcome) => sum + outcome.drainElapsed, 0)

  return {
    suite: 'journal',
    scenario: scenarioName(scenario),
    params: {
      files: 1,
      threads: 1,
      batch: scenario.batch,
      jobs,
      journal: scenario.journal,
      synchronous: scenario.synchronous.toUpperCase(),
      busyTimeout: 2000,
    },
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
      errors: collected.outcomes.reduce((sum, outcome) => sum + outcome.errors, 0),
    },
    samples: collected.outcomes.map((outcome) => ({
      valid: invalidReason(outcome, jobs) === undefined ? 1 : 0,
      completed: outcome.completed,
      'drain (ms)': outcome.drainElapsed,
      SQLITE_BUSY: outcome.busy,
      timeouts: outcome.timeouts,
      errors: outcome.errors,
    })),
    notes,
    ok: notes.length === 0,
    runs: collected.outcomes,
  }
}

/** Only compare valid WAL pairs with identical workload and batch size. */
export function applyDurabilityComparison(results: BenchmarkResult[]): void {
  for (const result of results) {
    if (!result.ok || result.params.journal !== 'WAL' || result.params.synchronous !== 'FULL')
      continue

    const baseline = results.find(
      (candidate) =>
        candidate.ok &&
        candidate.params.journal === 'WAL' &&
        candidate.params.synchronous === 'NORMAL' &&
        candidate.params.batch === result.params.batch &&
        candidate.params.jobs === result.params.jobs,
    )
    const normal = baseline?.metrics['jobs/sec']
    const full = result.metrics['jobs/sec']

    if (typeof normal !== 'number' || normal <= 0 || typeof full !== 'number') continue

    result.metrics['FULL drop (%)'] = (1 - full / normal) * 100
  }
}

export function definitions(environment: BenchEnvironment): ScenarioDefinition[] {
  const jobs = environment.jobs ?? (environment.grid === 'full' ? 10_000 : 2000)
  const scenarios: JournalScenario[] = [1, 16].flatMap((batch): JournalScenario[] => [
    { batch, journal: 'WAL', synchronous: 'normal' },
    { batch, journal: 'WAL', synchronous: 'full' },
    { batch, journal: 'DELETE', synchronous: 'full' },
  ])

  return scenarios
    .filter((scenario) => matches(scenarioName(scenario), environment.only))
    .map((scenario) =>
      defineScenario({
        suite: 'journal',
        scenario: scenarioName(scenario),
        jobs,
        run: () => executeRun(scenario, jobs),
        summarize: (collected) => summarizeJournal(scenario, jobs, collected),
      }),
    )
}
