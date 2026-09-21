import { randomUUID } from 'node:crypto'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { performance } from 'node:perf_hooks'

import { betterSqlite3 } from '@walq/better-sqlite3'
import Database from 'better-sqlite3'

import { synchronousPragma, type SynchronousMode } from './bench-options.js'
import {
  errorMessage,
  median,
  numeric,
  spread,
  summarizePerRunMicros,
  type BenchmarkResult,
  type Collected,
} from './harness.js'
import { defineScenario, type ScenarioDefinition } from './scenario.js'

export interface CompleteBatchGrid {
  batches: number[]
}

export interface CompleteBatchScenario {
  batch: number
}

export const quickCompleteBatchGrid: CompleteBatchGrid = { batches: [1, 4, 16] }
export const fullCompleteBatchGrid: CompleteBatchGrid = { batches: [1, 4, 16] }

const queue = 'complete-batch'
const leaseDuration = 30_000

export interface CompleteBatchOutcome {
  applied: number
  commitSamples: number[]
  workloadDuration: number
  error: string | undefined
}

export function completeBatchScenarioName(scenario: CompleteBatchScenario): string {
  return `complete batch ${scenario.batch}`
}

export function completeBatchScenarios(grid: CompleteBatchGrid): CompleteBatchScenario[] {
  return grid.batches.map((batch) => ({ batch }))
}

interface Lease {
  id: string
  leaseToken: string
}

/**
 * Benchmark-only prototype: apply several completions inside one immediate transaction.
 *
 * `complete batch 1` uses the production asynchronous `Storage.complete` path, which issues one
 * autocommit `UPDATE` through the adapter. The larger tiers use this prototype's synchronous SQL
 * inside one immediate transaction, so the measured gap mixes two changes: batching many
 * completions into one transaction, and comparing the adapter/async path with direct benchmark
 * SQL. It is not an isolation of batching alone.
 */
class BatchedCompleter {
  readonly #statement: Database.Statement
  readonly #transaction: Database.Transaction<(leases: Lease[], now: number) => number>

  constructor(db: Database.Database) {
    this.#statement = db
      .prepare(
        `
          UPDATE walq_jobs SET status = 'completed', finishedAt = @now, leaseToken = NULL, expiresAt = NULL
          WHERE id = @id AND status = 'active' AND leaseToken = @leaseToken AND expiresAt > @now
        `,
      )
      .safeIntegers(false)
    this.#transaction = db.transaction((leases: Lease[], now: number): number => {
      let applied = 0
      for (const lease of leases) {
        if (this.#statement.run({ ...lease, now }).changes === 1) applied += 1
      }
      return applied
    })
  }

  complete(leases: Lease[], now: number): number {
    return this.#transaction.immediate(leases, now)
  }
}

/** Seed live leases in one transaction so seeding stays outside every measured phase. */
function seedLeases(db: Database.Database, jobs: number): Lease[] {
  const insert = db.prepare(`
    INSERT INTO walq_jobs (
      id, queue, name, data, status, createdAt, availableAt, attemptsMade, attempts, leaseToken, expiresAt
    ) VALUES (@id, @queue, 'job', '{}', 'active', @now, @now, 1, 1, @leaseToken, @expiresAt)
  `)
  const leases: Lease[] = []
  const now = Date.now()
  const populate = db.transaction(() => {
    for (let index = 0; index < jobs; index += 1) {
      const lease = { id: randomUUID(), leaseToken: randomUUID() }
      insert.run({ ...lease, queue, now, expiresAt: now + leaseDuration })
      leases.push(lease)
    }
  })
  populate.immediate()

  return leases
}

async function executeRun(
  scenario: CompleteBatchScenario,
  jobs: number,
  synchronous: SynchronousMode,
): Promise<CompleteBatchOutcome> {
  const directory = mkdtempSync(join(tmpdir(), 'walq-complete-batch-'))
  const path = join(directory, 'walq.sqlite')
  const db = new Database(path)
  db.pragma('journal_mode = WAL')
  db.pragma(synchronousPragma(synchronous))
  db.pragma('busy_timeout = 2000')
  const storage = betterSqlite3(db)

  try {
    const leases = seedLeases(db, jobs)
    const completer = new BatchedCompleter(db)
    const commitSamples: number[] = []
    let applied = 0
    const started = performance.now()
    for (let index = 0; index < leases.length; index += scenario.batch) {
      const group = leases.slice(index, index + scenario.batch)
      const now = Date.now()
      const commitStarted = performance.now()
      if (scenario.batch === 1) {
        const lease = group[0]
        if (lease !== undefined) {
          const result = await storage.complete({ id: lease.id, leaseToken: lease.leaseToken, now })
          if (result === 'applied') applied += 1
        }
      } else {
        applied += completer.complete(group, now)
      }
      commitSamples.push(performance.now() - commitStarted)
    }

    return {
      applied,
      commitSamples,
      workloadDuration: performance.now() - started,
      error: undefined,
    }
  } catch (error) {
    return { applied: 0, commitSamples: [], workloadDuration: 0, error: errorMessage(error) }
  } finally {
    db.close()
    rmSync(directory, { recursive: true, force: true })
  }
}

export function completeBatchInvalidReason(
  outcome: CompleteBatchOutcome,
  jobs: number,
): string | undefined {
  if (outcome.error !== undefined) return outcome.error
  if (outcome.applied !== jobs) return `completed ${outcome.applied} of ${jobs} jobs`

  return undefined
}

export function summarizeCompleteBatchRuns(
  scenario: CompleteBatchScenario,
  jobs: number,
  collected: Collected<CompleteBatchOutcome>,
): BenchmarkResult {
  const valid = collected.outcomes.filter(
    (outcome) => completeBatchInvalidReason(outcome, jobs) === undefined,
  )
  const invalid = collected.outcomes
    .map((outcome) => completeBatchInvalidReason(outcome, jobs))
    .filter((reason): reason is string => reason !== undefined)
  const notes = [...collected.failures, ...invalid]
  const rates = valid.map((outcome) => (outcome.applied / outcome.workloadDuration) * 1000)
  const commit = summarizePerRunMicros(valid.map((outcome) => outcome.commitSamples))
  // Per-run commit time divided by the jobs actually applied, then aggregated by median so a
  // run with a short last batch cannot inflate the per-job cost by assuming every batch was full.
  const perJobMean = median(
    valid.map((outcome) =>
      outcome.applied === 0
        ? 0
        : (outcome.commitSamples.reduce((total, sample) => total + sample, 0) * 1000) /
          outcome.applied,
    ),
  )

  return {
    suite: 'complete-batch',
    scenario: completeBatchScenarioName(scenario),
    params: { batch: scenario.batch, jobs },
    metrics: {
      'complete jobs/sec': median(rates),
      'spread (%)': spread(rates),
      commits: Math.ceil(jobs / scenario.batch),
      'commit p50 (µs)': commit.p50,
      'commit p95 (µs)': commit.p95,
      'commit p99 (µs)': commit.p99,
      'per-job mean (µs)': perJobMean,
    },
    samples: valid.map((outcome) => ({
      'complete jobs/sec': (outcome.applied / outcome.workloadDuration) * 1000,
      'workload (ms)': outcome.workloadDuration,
      applied: outcome.applied,
    })),
    notes,
    ok: notes.length === 0,
  }
}

/** Adapter used by the Vitest benchmark file. */
export function defineCompleteBatchScenario(
  scenario: CompleteBatchScenario,
  jobs: number,
  synchronous: SynchronousMode,
): ScenarioDefinition {
  return defineScenario({
    suite: 'complete-batch',
    scenario: completeBatchScenarioName(scenario),
    jobs,
    run: () => executeRun(scenario, jobs, synchronous),
    summarize: (collected) => summarizeCompleteBatchRuns(scenario, jobs, collected),
    throughput: (result) => numeric(result.metrics['complete jobs/sec']),
    latency: (result) => result.samples.map((sample) => numeric(sample['workload (ms)'])),
  })
}
