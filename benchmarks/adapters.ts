import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { performance } from 'node:perf_hooks'
import { DatabaseSync } from 'node:sqlite'

import type { ClaimedJob, EnqueueInput, Storage } from '@walq/core/storage'

import { synchronousPragma, type BenchEnvironment, type SynchronousMode } from './bench-options.js'
import {
  guard,
  matches,
  spread,
  summarizePerRunMicros,
  type BenchmarkResult,
  type Collected,
} from './harness.js'
import { defineScenario, type ScenarioDefinition } from './scenario.js'

export type AdapterName = 'better-sqlite3' | 'node:sqlite'
export type DatabaseMode = 'memory' | 'wal'
export type AdapterOperation =
  | 'enqueueMany'
  | 'claim'
  | 'claimQueues-grouped'
  | 'lifecycle'
  | 'cleanup'

export interface AdapterScenario {
  adapter: AdapterName
  database: DatabaseMode
  operation: AdapterOperation
  jobs: number
  batch: number
}

/** Optional built-package entry points for the cross-runtime CLI. */
export interface AdapterEntries {
  sqlite: string
  betterSqlite3: string
}

interface Connection {
  exec(sql: string): unknown
  prepare(sql: string): { get(): unknown }
  close(): void
}

export interface AdapterOutcome {
  jobs: number
  duration: number
  latency: number[]
  sqlite: string
}

export interface AdapterResult extends BenchmarkResult {
  /** Raw operation timings, in milliseconds, retained for each measured run. */
  runs: AdapterOutcome[]
}

const operations: AdapterOperation[] = [
  'enqueueMany',
  'claim',
  'claimQueues-grouped',
  'lifecycle',
  'cleanup',
]
const queues = 16
const groups = 16

export function adapterNames(value: string | undefined, runtime = 'node'): AdapterName[] {
  const selected =
    value === undefined || value === ''
      ? runtime === 'node'
        ? ['better-sqlite3', 'node:sqlite']
        : ['node:sqlite']
      : value.split(',').map((name) => name.trim())

  if (selected.some((name) => name !== 'better-sqlite3' && name !== 'node:sqlite')) {
    throw new Error('BENCH_ADAPTERS must contain better-sqlite3 or node:sqlite')
  }
  if (runtime !== 'node' && selected.includes('better-sqlite3')) {
    throw new Error('The better-sqlite3 comparison is supported only on Node.js')
  }

  return [...new Set(selected)] as AdapterName[]
}

export function adapterScenarios(
  environment: BenchEnvironment,
  adapters: AdapterName[],
): AdapterScenario[] {
  const jobs = environment.jobs ?? (environment.grid === 'full' ? 10_000 : 1024)

  return (['memory', 'wal'] as const)
    .flatMap((database) =>
      operations.flatMap((operation) =>
        adapters.map((adapter) => ({ adapter, database, operation, jobs, batch: 64 })),
      ),
    )
    .filter((scenario) => matches(scenarioName(scenario), environment.only))
}

function scenarioName(scenario: AdapterScenario): string {
  return `${scenario.adapter}/${scenario.database}/${scenario.operation}`
}

async function open(
  adapter: AdapterName,
  path: string,
  database: DatabaseMode,
  synchronous: SynchronousMode,
  entries: AdapterEntries | undefined,
): Promise<{ db: Connection; storage: Storage }> {
  if (adapter === 'node:sqlite') {
    const { sqlite } = (await import(
      entries?.sqlite ?? '@walq/sqlite'
    )) as typeof import('@walq/sqlite')
    const db = new DatabaseSync(path)

    try {
      configure(db, database, synchronous)

      return { db, storage: sqlite(db) }
    } catch (error) {
      db.close()
      throw error
    }
  }

  // Never load the native addon when running the built-in driver on Bun or Deno.
  const [{ default: Database }, { betterSqlite3 }] = await Promise.all([
    import('better-sqlite3'),
    import(entries?.betterSqlite3 ?? '@walq/better-sqlite3') as Promise<
      typeof import('@walq/better-sqlite3')
    >,
  ])
  const db = new Database(path)

  try {
    configure(db, database, synchronous)

    return { db, storage: betterSqlite3(db) }
  } catch (error) {
    db.close()
    throw error
  }
}

function configure(db: Connection, database: DatabaseMode, synchronous: SynchronousMode): void {
  db.exec(
    `PRAGMA page_size = 4096; PRAGMA journal_mode = ${database === 'wal' ? 'WAL' : 'MEMORY'}; PRAGMA ${synchronousPragma(synchronous)}; PRAGMA busy_timeout = 5000; PRAGMA cache_size = -2000; PRAGMA wal_autocheckpoint = 1000`,
  )
}

function inputs(scenario: AdapterScenario): EnqueueInput[] {
  return Array.from({ length: scenario.jobs }, (_, index) => ({
    queue: scenario.operation === 'claimQueues-grouped' ? `bench-${index % queues}` : 'bench',
    name: 'job',
    data: '{"value":1}',
    now: 10,
    availableAt: 10,
    priority: 0,
    attempts: 2,
    ...(scenario.operation === 'claimQueues-grouped'
      ? {
          group: { id: `group-${Math.floor(index / queues) % groups}`, concurrency: scenario.jobs },
        }
      : {}),
  }))
}

export async function runAdapterScenario(
  scenario: AdapterScenario,
  synchronous: SynchronousMode,
  entries?: AdapterEntries,
): Promise<AdapterOutcome> {
  if (
    !Number.isSafeInteger(scenario.jobs) ||
    scenario.jobs < 1 ||
    !Number.isSafeInteger(scenario.batch) ||
    scenario.batch < 1
  ) {
    throw new Error('Adapter scenarios require positive safe-integer jobs and batch sizes')
  }

  const directory = mkdtempSync(join(tmpdir(), 'walq-adapters-bench-'))
  let db: Connection | undefined
  const deadline = guard(120_000, `Timed out: ${scenarioName(scenario)}`)

  try {
    const connection = await open(
      scenario.adapter,
      scenario.database === 'memory' ? ':memory:' : join(directory, 'queue.sqlite'),
      scenario.database,
      synchronous,
      entries,
    )
    db = connection.db
    const storage = connection.storage
    const journal = db.prepare('PRAGMA journal_mode').get() as { journal_mode: string }
    const durability = db.prepare('PRAGMA synchronous').get() as { synchronous: number }
    const page = db.prepare('PRAGMA page_size').get() as { page_size: number }
    if (
      journal.journal_mode !== (scenario.database === 'wal' ? 'wal' : 'memory') ||
      durability.synchronous !== (synchronous === 'full' ? 2 : 1) ||
      page.page_size !== 4096
    ) {
      throw new Error('SQLite did not apply the requested durability settings')
    }
    const version = db.prepare('SELECT sqlite_version() AS version').get() as { version: string }
    const pending = inputs(scenario)
    const batches: EnqueueInput[][] = []
    for (let index = 0; index < pending.length; index += scenario.batch)
      batches.push(pending.slice(index, index + scenario.batch))
    const request = { queue: 'bench', now: 10, limit: scenario.batch, leaseDuration: 1_000_000 }
    const latency: number[] = []
    let processed = 0
    const claimed: ClaimedJob[] = []

    async function measure<Result>(operation: () => Promise<Result>): Promise<Result> {
      deadline.check()
      const started = performance.now()
      const result = await operation()
      latency.push(performance.now() - started)

      return result
    }

    if (scenario.operation !== 'enqueueMany' && scenario.operation !== 'lifecycle') {
      for (const batch of batches) {
        deadline.check()
        await storage.enqueueMany(batch)
      }
    }

    if (scenario.operation === 'cleanup') {
      while (processed < scenario.jobs) {
        deadline.check()
        const jobs = await storage.claim(request)
        if (jobs.length === 0) throw new Error('Cleanup setup stopped before all jobs were claimed')
        for (const job of jobs) {
          const result = await storage.complete({ id: job.id, leaseToken: job.leaseToken, now: 11 })
          if (result !== 'applied') throw new Error('Cleanup setup lost a lease')
        }
        processed += jobs.length
      }
      processed = 0
      const cleanup = {
        queue: 'bench',
        now: 12,
        limit: scenario.batch,
        retention: { completed: { count: 0, maxAge: null }, failed: { count: null, maxAge: null } },
      }
      while (processed < scenario.jobs) {
        const result = await measure(() => storage.cleanup(cleanup))
        if (result.removed === 0)
          throw new Error('Cleanup stopped before all terminal jobs were removed')
        processed += result.removed
      }
    } else if (scenario.operation === 'enqueueMany' || scenario.operation === 'lifecycle') {
      for (const batch of batches) {
        const added = await measure(() => storage.enqueueMany(batch))
        if (added.length !== batch.length) throw new Error('Enqueue returned an incomplete batch')
        processed += added.length
        if (scenario.operation === 'lifecycle') {
          const claim = { ...request, limit: batch.length }
          const jobs = await measure(() => storage.claim(claim))
          claimed.push(...jobs)
          if (jobs.length !== batch.length)
            throw new Error('Lifecycle claim returned an incomplete batch')
          for (const job of jobs) {
            const complete = { id: job.id, leaseToken: job.leaseToken, now: 11 }
            const result = await measure(() => storage.complete(complete))
            if (result !== 'applied') throw new Error('Lifecycle lost a lease')
          }
        }
      }
    } else {
      const requests = Array.from({ length: queues }, (_, index) => ({
        ...request,
        queue: `bench-${index}`,
      }))
      const sweep = { requests }
      while (processed < scenario.jobs) {
        const jobs =
          scenario.operation === 'claim'
            ? await measure(() => storage.claim(request))
            : (await measure(() => storage.claimQueues!(sweep))).flat()
        if (jobs.length === 0) throw new Error('Claims stopped before all jobs were acquired')
        claimed.push(...jobs)
        processed += jobs.length
      }
    }

    // Correctness checks and SQL inspection are outside the recorded operation timings.
    if (processed !== scenario.jobs)
      throw new Error(`Processed ${processed}, expected ${scenario.jobs}`)
    if (
      claimed.length > 0 &&
      (new Set(claimed.map((job) => job.id)).size !== scenario.jobs ||
        new Set(claimed.map((job) => job.leaseToken)).size !== scenario.jobs)
    ) {
      throw new Error('Duplicate or missing jobs/leases in measured work')
    }
    const expected =
      scenario.operation === 'enqueueMany'
        ? 'pending'
        : scenario.operation === 'lifecycle'
          ? 'completed'
          : 'active'
    const counts = db
      .prepare(`SELECT count(*) AS count, sum(status = '${expected}') AS matching FROM walq_jobs`)
      .get() as { count: number; matching: number | null }
    if (
      scenario.operation === 'cleanup'
        ? counts.count !== 0
        : counts.count !== scenario.jobs || counts.matching !== scenario.jobs
    ) {
      throw new Error('Final database state does not match the measured workload')
    }
    const duration = latency.reduce((total, value) => total + value, 0)
    if (!Number.isFinite(duration) || duration <= 0) throw new Error('Invalid measured duration')

    return { jobs: processed, duration, latency, sqlite: version.version }
  } finally {
    deadline.dispose()
    db?.close()
    rmSync(directory, { recursive: true, force: true })
  }
}

export function summarizeAdapterRuns(
  scenario: AdapterScenario,
  collected: Collected<AdapterOutcome>,
): AdapterResult {
  const runs = collected.outcomes
  const rates = runs.map((run) => (run.jobs / run.duration) * 1000)
  const latency = summarizePerRunMicros(runs.map((run) => run.latency))
  const jobs = runs.reduce((total, run) => total + run.jobs, 0)
  const duration = runs.reduce((total, run) => total + run.duration, 0)
  const notes = [...collected.failures]
  if (runs.length === 0) notes.push('No measured runs')
  if (
    runs.some(
      (run) =>
        run.jobs !== scenario.jobs ||
        !Number.isFinite(run.duration) ||
        run.duration <= 0 ||
        run.latency.length === 0 ||
        run.duration !== run.latency.reduce((total, value) => total + value, 0) ||
        run.latency.some((value) => !Number.isFinite(value) || value < 0),
    )
  )
    notes.push('Invalid measured work')

  return {
    suite: 'adapters',
    scenario: scenarioName(scenario),
    params: { ...scenario },
    metrics: {
      'jobs/sec': duration > 0 ? (jobs / duration) * 1000 : 0,
      'spread (%)': spread(rates),
      'call p95 (µs)': latency.p95,
      'call samples': latency.count,
      'measured runs': runs.length,
      SQLite: runs[0]?.sqlite ?? '',
    },
    samples: runs.map((run) => ({
      jobs: run.jobs,
      duration: run.duration,
      calls: run.latency.length,
      'jobs/sec': (run.jobs / run.duration) * 1000,
    })),
    notes,
    ok: notes.length === 0,
    runs,
  }
}

export function defineAdapterScenario(
  scenario: AdapterScenario,
  synchronous: SynchronousMode,
  entries?: AdapterEntries,
): ScenarioDefinition {
  return defineScenario({
    suite: 'adapters',
    scenario: scenarioName(scenario),
    jobs: scenario.jobs,
    run: () => runAdapterScenario(scenario, synchronous, entries),
    summarize: (collected) => summarizeAdapterRuns(scenario, collected),
  })
}
