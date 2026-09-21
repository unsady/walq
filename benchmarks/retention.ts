import { randomUUID } from 'node:crypto'
import { mkdtempSync, rmSync, statSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { performance } from 'node:perf_hooks'

import { betterSqlite3 } from '@walq/better-sqlite3'
import type { Storage } from '@walq/core/storage'
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

export type RetentionCleanup = 'retained' | 'delete' | 'delete-vacuum'
export type RetentionConnection = 'warm' | 'reopened'

/** One explicit corner of the matrix, expanded independently to avoid a full Cartesian product. */
export interface RetentionTier {
  history: number[]
  cleanup: RetentionCleanup[]
  batches: number[]
  connections: RetentionConnection[]
}

export interface RetentionGrid {
  tiers: RetentionTier[]
}

export interface RetentionScenario {
  history: number
  cleanup: RetentionCleanup
  batch: number
  connection: RetentionConnection
}

export const quickRetentionGrid: RetentionGrid = {
  tiers: [
    { history: [0], cleanup: ['retained'], batches: [10_000], connections: ['warm'] },
    { history: [25_000], cleanup: ['retained'], batches: [10_000], connections: ['warm'] },
    {
      history: [25_000],
      cleanup: ['delete', 'delete-vacuum'],
      batches: [10_000],
      connections: ['warm'],
    },
  ],
}

export const fullRetentionGrid: RetentionGrid = {
  tiers: [
    { history: [0], cleanup: ['retained'], batches: [10_000], connections: ['warm', 'reopened'] },
    {
      history: [1_000, 25_000, 250_000, 1_000_000],
      cleanup: ['retained'],
      batches: [10_000],
      connections: ['warm'],
    },
    {
      history: [25_000, 250_000],
      cleanup: ['retained'],
      batches: [10_000],
      connections: ['reopened'],
    },
    {
      history: [1_000, 25_000, 250_000, 1_000_000],
      cleanup: ['delete'],
      batches: [10_000],
      connections: ['warm'],
    },
    {
      history: [1_000, 25_000, 250_000, 1_000_000],
      cleanup: ['delete-vacuum'],
      batches: [10_000],
      connections: ['warm'],
    },
    {
      history: [250_000],
      cleanup: ['delete', 'delete-vacuum'],
      batches: [50_000],
      connections: ['warm'],
    },
    {
      history: [250_000],
      cleanup: ['delete-vacuum'],
      batches: [10_000],
      connections: ['reopened'],
    },
  ],
}

const queue = 'retention'
const warmupQueue = 'retention-warmup'
const activeClaimLimit = 20
const activeLeaseDuration = 60_000
const warmupJobs = 25

interface SizeSnapshot {
  db: number
  wal: number
  pages: number
}

export interface RetentionCleanupOutcome {
  cleanup: number
  cleanupBatches: number
  cleanupBatchSamples: number[]
  cleanupStallSamples: number[]
  vacuum: number
  vacuumStall: number
  checkpoint: number
}

export interface RetentionActiveOutcome {
  claimed: number
  completed: number
  duplicates: number
  lostLeases: number
  enqueueSamples: number[]
  claimSamples: number[]
  completeSamples: number[]
  eventLoopSamples: number[]
  workloadDuration: number
  errors: string[]
}

export type RetentionOutcome = RetentionActiveOutcome &
  RetentionCleanupOutcome & {
    activeJobs: number
    before: SizeSnapshot
    after: SizeSnapshot
  }

/** Expand the tiers into scenarios, deduplicating overlaps between tiers. */
export function retentionScenarios(grid: RetentionGrid): RetentionScenario[] {
  const scenarios: RetentionScenario[] = []
  const seen = new Set<string>()
  function add(scenario: RetentionScenario): void {
    const key = `${scenario.history}|${scenario.cleanup}|${scenario.batch}|${scenario.connection}`
    if (seen.has(key)) return
    seen.add(key)
    scenarios.push(scenario)
  }

  for (const tier of grid.tiers) {
    const batches = tier.batches.length > 0 ? tier.batches : [0]
    for (const history of tier.history) {
      for (const cleanup of tier.cleanup) {
        // There is nothing to retain or clean at zero history beyond the baseline.
        if (history === 0 && cleanup !== 'retained') continue
        if (cleanup === 'retained') {
          for (const connection of tier.connections) {
            add({ history, cleanup, batch: 0, connection })
          }
        } else {
          for (const batch of batches) {
            for (const connection of tier.connections) {
              add({ history, cleanup, batch, connection })
            }
          }
        }
      }
    }
  }

  return scenarios
}

export function formatCount(value: number): string {
  if (value >= 1_000_000) {
    return `${Number((value / 1_000_000).toFixed(value % 1_000_000 === 0 ? 0 : 1))}M`
  }
  if (value >= 1_000) {
    return `${Number((value / 1_000).toFixed(value % 1_000 === 0 ? 0 : 1))}k`
  }

  return String(value)
}

export function retentionScenarioName(scenario: RetentionScenario): string {
  const history = `${formatCount(scenario.history)} history`
  if (scenario.cleanup === 'retained') {
    return `retained / ${history} / ${scenario.connection}`
  }

  return `${scenario.cleanup} / ${history} / ${formatCount(scenario.batch)} batch / ${scenario.connection}`
}

/** Optional override that collapses every tier to a single cleanup batch size. */
export function retentionBatchOverride(value: string | undefined): number | undefined {
  if (value === undefined || value === '') return undefined
  const parsed = Number(value)
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`BENCH_RETENTION_BATCH must be a positive integer, received "${value}"`)
  }

  return parsed
}

export function withRetentionBatch(grid: RetentionGrid, batch: number | undefined): RetentionGrid {
  if (batch === undefined) return grid

  return { tiers: grid.tiers.map((tier) => ({ ...tier, batches: [batch] })) }
}

function nextTurn(): Promise<number> {
  const started = performance.now()

  return new Promise((resolve) => setImmediate(() => resolve(performance.now() - started)))
}

function openDatabase(path: string, synchronous: SynchronousMode): Database.Database {
  const db = new Database(path)
  db.pragma('journal_mode = WAL')
  db.pragma(synchronousPragma(synchronous))
  db.pragma('busy_timeout = 2000')

  return db
}

function fileSize(path: string): number {
  try {
    return statSync(path).size
  } catch {
    return 0
  }
}

function snapshot(path: string, db: Database.Database): SizeSnapshot {
  return {
    db: fileSize(path),
    wal: fileSize(`${path}-wal`),
    pages: Number(db.pragma('page_count', { simple: true })),
  }
}

/**
 * Insert terminal history through benchmark-only SQL inside one transaction. This never goes
 * through the public `enqueue`/`complete` calls, so seeding stays outside every measured phase.
 */
function seedHistory(db: Database.Database, rows: number): void {
  if (rows === 0) return

  const insert = db
    .prepare(
      `
        INSERT INTO walq_jobs (
          id, queue, name, data, status, createdAt, availableAt, finishedAt, attemptsMade, attempts, error
        ) VALUES (@id, @queue, 'history', '{}', @status, @now, @now, @now, 1, 1, @error)
      `,
    )
    .safeIntegers(false)
  const now = Date.now()
  const populate = db.transaction(() => {
    for (let index = 0; index < rows; index += 1) {
      const failed = index % 2 === 1
      insert.run({
        id: randomUUID(),
        queue,
        status: failed ? 'failed' : 'completed',
        now,
        error: failed ? 'history failure' : null,
      })
    }
  })
  populate.immediate()
}

async function runCleanup(
  db: Database.Database,
  scenario: RetentionScenario,
): Promise<RetentionCleanupOutcome> {
  const empty: RetentionCleanupOutcome = {
    cleanup: 0,
    cleanupBatches: 0,
    cleanupBatchSamples: [],
    cleanupStallSamples: [],
    vacuum: 0,
    vacuumStall: 0,
    checkpoint: 0,
  }
  if (scenario.cleanup === 'retained') return empty

  const removeBatch = db
    .prepare(
      `
        DELETE FROM walq_jobs
        WHERE rowid IN (
          SELECT rowid FROM walq_jobs
          WHERE rowid > @after AND queue = @queue AND status IN ('completed', 'failed')
          ORDER BY rowid LIMIT @batch
        )
        RETURNING rowid
      `,
    )
    .safeIntegers(false)

  const cleanupBatchSamples: number[] = []
  const cleanupStallSamples: number[] = []
  const started = performance.now()
  let after = 0
  let batches = 0
  for (;;) {
    const turn = nextTurn()
    const batchStarted = performance.now()
    const rows = removeBatch.all({ after, queue, batch: scenario.batch }) as { rowid: number }[]
    cleanupBatchSamples.push(performance.now() - batchStarted)
    cleanupStallSamples.push(await turn)
    batches += 1
    if (rows.length === 0) break
    after = rows[rows.length - 1]?.rowid ?? after
    if (rows.length < scenario.batch) break
  }
  const cleanup = performance.now() - started

  let vacuum = 0
  let vacuumStall = 0
  let checkpoint = 0
  if (scenario.cleanup === 'delete-vacuum') {
    const turn = nextTurn()
    const vacuumStarted = performance.now()
    db.exec('VACUUM')
    vacuum = performance.now() - vacuumStarted
    vacuumStall = await turn

    const checkpointStarted = performance.now()
    db.pragma('wal_checkpoint(TRUNCATE)')
    checkpoint = performance.now() - checkpointStarted
  }

  return {
    cleanup,
    cleanupBatches: batches,
    cleanupBatchSamples,
    cleanupStallSamples,
    vacuum,
    vacuumStall,
    checkpoint,
  }
}

/** Warm prepared statements and the page cache without touching the measured queue. */
async function warmConnection(storage: Storage): Promise<void> {
  for (let index = 0; index < warmupJobs; index += 1) {
    const now = Date.now()
    await storage.enqueue({
      queue: warmupQueue,
      name: 'warmup',
      data: '{}',
      now,
      availableAt: now,
      attempts: 1,
    })
  }
  let completed = 0
  while (completed < warmupJobs) {
    const batch = await storage.claim({
      queue: warmupQueue,
      limit: activeClaimLimit,
      now: Date.now(),
      leaseDuration: activeLeaseDuration,
    })
    if (batch.length === 0) break
    for (const job of batch) {
      await storage.complete({ id: job.id, leaseToken: job.leaseToken, now: Date.now() })
      completed += 1
    }
  }
}

async function runActive(storage: Storage, jobs: number): Promise<RetentionActiveOutcome> {
  const enqueueSamples: number[] = []
  const claimSamples: number[] = []
  const completeSamples: number[] = []
  const eventLoopSamples: number[] = []
  const seen = new Set<string>()
  let claimed = 0
  let completed = 0
  let duplicates = 0
  let lostLeases = 0

  try {
    const started = performance.now()
    for (let index = 0; index < jobs; index += 1) {
      const now = Date.now()
      const enqueueStarted = performance.now()
      await storage.enqueue({
        queue,
        name: 'active',
        data: '{}',
        now,
        availableAt: now,
        attempts: 1,
      })
      enqueueSamples.push(performance.now() - enqueueStarted)
    }

    while (claimed < jobs) {
      const turn = nextTurn()
      const claimStarted = performance.now()
      const batch = await storage.claim({
        queue,
        limit: activeClaimLimit,
        now: Date.now(),
        leaseDuration: activeLeaseDuration,
      })
      claimSamples.push(performance.now() - claimStarted)
      eventLoopSamples.push(await turn)
      if (batch.length === 0) throw new Error(`claiming stopped after ${claimed} of ${jobs} jobs`)

      for (const job of batch) {
        if (seen.has(job.id)) duplicates += 1
        seen.add(job.id)
        claimed += 1
        const completeStarted = performance.now()
        const result = await storage.complete({
          id: job.id,
          leaseToken: job.leaseToken,
          now: Date.now(),
        })
        completeSamples.push(performance.now() - completeStarted)
        if (result === 'applied') completed += 1
        else lostLeases += 1
      }
    }

    return {
      claimed,
      completed,
      duplicates,
      lostLeases,
      enqueueSamples,
      claimSamples,
      completeSamples,
      eventLoopSamples,
      workloadDuration: performance.now() - started,
      errors: [],
    }
  } catch (error) {
    return {
      claimed,
      completed,
      duplicates,
      lostLeases,
      enqueueSamples,
      claimSamples,
      completeSamples,
      eventLoopSamples,
      workloadDuration: 0,
      errors: [errorMessage(error)],
    }
  }
}

async function executeRun(
  scenario: RetentionScenario,
  jobs: number,
  synchronous: SynchronousMode,
): Promise<RetentionOutcome> {
  const directory = mkdtempSync(join(tmpdir(), 'walq-retention-'))
  const path = join(directory, 'walq.sqlite')
  let db: Database.Database | undefined

  try {
    db = openDatabase(path, synchronous)
    let storage = betterSqlite3(db)
    seedHistory(db, scenario.history)
    const before = snapshot(path, db)
    const cleanup = await runCleanup(db, scenario)
    const after = snapshot(path, db)

    if (scenario.connection === 'reopened') {
      db.close()
      db = openDatabase(path, synchronous)
      storage = betterSqlite3(db)
    }

    await warmConnection(storage)
    const active = await runActive(storage, jobs)

    return {
      ...active,
      ...cleanup,
      activeJobs: jobs,
      before,
      after,
    }
  } finally {
    try {
      db?.close()
    } catch {
      // The connection may already be closed by the reopened branch.
    }
    rmSync(directory, { recursive: true, force: true })
  }
}

export function retentionInvalidReason(
  outcome: RetentionOutcome,
  jobs: number,
): string | undefined {
  if (outcome.claimed !== jobs) return `claimed ${outcome.claimed} of ${jobs} jobs`
  if (outcome.completed !== jobs) return `completed ${outcome.completed} of ${jobs} jobs`
  if (outcome.duplicates !== 0) return `${outcome.duplicates} duplicate claims`
  if (outcome.lostLeases !== 0) return `${outcome.lostLeases} lost leases`
  if (outcome.errors.length > 0) return outcome.errors[0]
  return undefined
}

function toMib(bytes: number): number {
  return bytes / (1024 * 1024)
}

export function summarizeRetentionRuns(
  scenario: RetentionScenario,
  jobs: number,
  collected: Collected<RetentionOutcome>,
): BenchmarkResult {
  const valid = collected.outcomes.filter(
    (outcome) => retentionInvalidReason(outcome, jobs) === undefined,
  )
  const invalid = collected.outcomes
    .map((outcome) => retentionInvalidReason(outcome, jobs))
    .filter((reason): reason is string => reason !== undefined)
  const notes = [...collected.failures]
  if (invalid.length > 0) notes.push(...invalid)

  const rates = valid.map((outcome) => (jobs / outcome.workloadDuration) * 1000)
  const enqueue = summarizePerRunMicros(valid.map((outcome) => outcome.enqueueSamples))
  const claim = summarizePerRunMicros(valid.map((outcome) => outcome.claimSamples))
  const complete = summarizePerRunMicros(valid.map((outcome) => outcome.completeSamples))
  const eventLoop = summarizePerRunMicros(valid.map((outcome) => outcome.eventLoopSamples))
  const batch = summarizePerRunMicros(valid.map((outcome) => outcome.cleanupBatchSamples))
  const stall = summarizePerRunMicros(valid.map((outcome) => outcome.cleanupStallSamples))
  const vacuumStalls = valid.map((outcome) => outcome.vacuumStall * 1000)

  return {
    suite: 'retention',
    scenario: retentionScenarioName(scenario),
    params: {
      history: scenario.history,
      cleanup: scenario.cleanup,
      batch: scenario.batch,
      connection: scenario.connection,
      jobs,
    },
    metrics: {
      'active jobs/sec': median(rates),
      'spread (%)': spread(rates),
      'enqueue p50 (µs)': enqueue.p50,
      'enqueue p95 (µs)': enqueue.p95,
      'enqueue p99 (µs)': enqueue.p99,
      'claim p50 (µs)': claim.p50,
      'claim p95 (µs)': claim.p95,
      'claim p99 (µs)': claim.p99,
      'complete p50 (µs)': complete.p50,
      'complete p95 (µs)': complete.p95,
      'complete p99 (µs)': complete.p99,
      'event loop p95 (µs)': eventLoop.p95,
      'event loop p99 (µs)': eventLoop.p99,
      'cleanup (ms)': median(valid.map((outcome) => outcome.cleanup)),
      'delete batches': median(valid.map((outcome) => outcome.cleanupBatches)),
      'delete batch p50 (µs)': batch.p50,
      'delete batch p95 (µs)': batch.p95,
      'delete batch p99 (µs)': batch.p99,
      'delete stall p95 (µs)': stall.p95,
      'delete stall p99 (µs)': stall.p99,
      'vacuum (ms)': median(valid.map((outcome) => outcome.vacuum)),
      'vacuum stall (µs)': median(vacuumStalls),
      'checkpoint (ms)': median(valid.map((outcome) => outcome.checkpoint)),
      'db before (MiB)': median(valid.map((outcome) => toMib(outcome.before.db))),
      'wal before (MiB)': median(valid.map((outcome) => toMib(outcome.before.wal))),
      'db after (MiB)': median(valid.map((outcome) => toMib(outcome.after.db))),
      'wal after (MiB)': median(valid.map((outcome) => toMib(outcome.after.wal))),
      'pages before': median(valid.map((outcome) => outcome.before.pages)),
      'pages after': median(valid.map((outcome) => outcome.after.pages)),
    },
    samples: valid.map((outcome) => ({
      'active jobs/sec': (jobs / outcome.workloadDuration) * 1000,
      'workload (ms)': outcome.workloadDuration,
      claimed: outcome.claimed,
      completed: outcome.completed,
      'cleanup (ms)': outcome.cleanup,
      'vacuum (ms)': outcome.vacuum,
      'db after (MiB)': toMib(outcome.after.db),
      'wal after (MiB)': toMib(outcome.after.wal),
    })),
    notes,
    ok: notes.length === 0,
  }
}

export function defineRetentionScenario(
  scenario: RetentionScenario,
  jobs: number,
  synchronous: SynchronousMode,
): ScenarioDefinition {
  return defineScenario({
    suite: 'retention',
    scenario: retentionScenarioName(scenario),
    jobs,
    run: () => executeRun(scenario, jobs, synchronous),
    summarize: (collected) => summarizeRetentionRuns(scenario, jobs, collected),
    throughput: (result) => numeric(result.metrics['active jobs/sec']),
    latency: (result) => result.samples.map((sample) => numeric(sample['workload (ms)'])),
  })
}
