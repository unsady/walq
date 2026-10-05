import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Worker } from 'node:worker_threads'

import { betterSqlite3 } from '@walq/better-sqlite3'
import Database from 'better-sqlite3'

import { synchronousPragma, type SynchronousMode } from './bench-options.js'
import type {
  ContentionWorkerInput,
  DrainReport,
  EnqueueReport,
  WorkerReport,
} from './fixtures/contention-worker.js'
import {
  deferred,
  distribute,
  guard,
  median,
  spread,
  summarizePerRunMicros,
  type BenchmarkResult,
  type Collected,
  type Deferred,
} from './harness.js'
import { defineScenario, type ScenarioDefinition } from './scenario.js'

export interface ContentionGrid {
  threads: number[]
  batches: number[]
}

export interface ContentionScenario {
  threads: number
  batch: number
}

export const quickContentionGrid: ContentionGrid = {
  threads: [1, 4],
  batches: [16],
}

export const fullContentionGrid: ContentionGrid = {
  threads: [1, 4],
  batches: [1, 16],
}

const runTimeout = 60_000

interface Channel {
  worker: Worker
  ready: Deferred<void>
  enqueue: Deferred<EnqueueReport>
  drain: Deferred<DrainReport>
  exited: Deferred<number>
}

export interface ContentionRunOutcome {
  enqueueElapsed: number
  drainElapsed: number
  enqueued: number
  completed: number
  lostLeases: number
  duplicates: number
  claims: number
  emptyClaims: number
  errors: number
  aborted: boolean
  firstError: string | null
  enqueueSamples: number[]
  claimSamples: number[]
  completeSamples: number[]
  emptyClaimSamples: number[]
  loopSamples: number[]
  busy: number
  timeouts: number
}

export function scenarioName(scenario: ContentionScenario): string {
  return `shared / ${scenario.threads} threads / batch ${scenario.batch}`
}

export function contentionScenarios(grid: ContentionGrid): ContentionScenario[] {
  const scenarios: ContentionScenario[] = []
  for (const threads of grid.threads) {
    for (const batch of grid.batches) {
      scenarios.push({ threads, batch })
    }
  }

  return scenarios
}

/** Create the schema once so that workers never race during startup. */
async function prepareDatabase(
  path: string,
  synchronous: SynchronousMode,
  journal: 'WAL' | 'DELETE',
  jobs = 0,
): Promise<void> {
  const db = new Database(path)
  try {
    db.pragma(`journal_mode = ${journal}`)
    db.pragma(synchronousPragma(synchronous))
    db.pragma('busy_timeout = 2000')
    const storage = betterSqlite3(db)
    if (jobs > 0) {
      const now = Date.now()
      await storage.enqueueMany(
        Array.from({ length: jobs }, () => ({
          queue: 'bench',
          name: 'bench',
          data: '{}',
          now,
          availableAt: now,
          priority: 0,
          attempts: 1,
        })),
      )
    }
  } finally {
    db.close()
  }
}

function startWorker(input: ContentionWorkerInput): Channel {
  const worker = new Worker(new URL('./fixtures/contention-worker.js', import.meta.url), {
    workerData: input,
  })
  const ready = deferred<void>()
  const enqueue = deferred<EnqueueReport>()
  const drain = deferred<DrainReport>()
  const exited = deferred<number>()
  function fail(error: unknown): void {
    ready.reject(error)
    enqueue.reject(error)
    drain.reject(error)
    exited.reject(error)
  }

  worker.on('message', (report: WorkerReport) => {
    if (report.phase === 'ready') ready.resolve()
    else if (report.phase === 'enqueue') enqueue.resolve(report)
    else drain.resolve(report)
  })
  worker.on('error', fail)
  worker.on('exit', (code) => {
    if (code === 0) exited.resolve(0)
    else fail(new Error(`contention worker exited with code ${code}`))
  })

  return { worker, ready, enqueue, drain, exited }
}

function release(gate: SharedArrayBuffer, phase: number): void {
  const counter = new Int32Array(gate)
  Atomics.store(counter, 0, phase)
  Atomics.notify(counter, 0)
}

function total<Value>(reports: Value[], pick: (report: Value) => number): number {
  return reports.reduce((sum, report) => sum + pick(report), 0)
}

function span(reports: { startedAt: number; finishedAt: number }[]): number {
  const startedAt = Math.min(...reports.map((report) => report.startedAt))
  const finishedAt = Math.max(...reports.map((report) => report.finishedAt))

  return finishedAt - startedAt
}

/** Successful enqueues are the measured samples; failures never produce one. */
export function aggregateReports(
  enqueues: EnqueueReport[],
  drains: DrainReport[],
): ContentionRunOutcome {
  const firstError =
    enqueues.find((report) => report.firstError !== null)?.firstError ??
    drains.find((report) => report.firstError !== null)?.firstError ??
    null
  const completed = total(drains, (report) => report.completed)
  // Ids arrive per thread and per file; only the parent can spot a cross-thread repeat.
  const completedIds = new Set(drains.flatMap((report) => report.completedIds))

  return {
    enqueueElapsed: enqueues.length === 0 ? 0 : span(enqueues),
    drainElapsed: span(drains),
    enqueued: total(enqueues, (report) => report.samples.length),
    completed,
    lostLeases: total(drains, (report) => report.lostLeases),
    duplicates: completed - completedIds.size,
    claims: total(drains, (report) => report.claims),
    emptyClaims: total(drains, (report) => report.emptyClaims),
    errors: total([...enqueues, ...drains], (report) => report.errors),
    aborted: [...enqueues, ...drains].some((report) => report.aborted),
    firstError,
    enqueueSamples: enqueues.flatMap((report) => report.samples),
    claimSamples: drains.flatMap((report) => report.claimSamples),
    completeSamples: drains.flatMap((report) => report.completeSamples),
    emptyClaimSamples: drains.flatMap((report) => report.emptyClaimSamples),
    loopSamples: drains.flatMap((report) => report.loopSamples),
    busy: total(drains, (report) => report.busy),
    timeouts: total(drains, (report) => report.timeouts),
  }
}

export async function executeRun(
  scenario: ContentionScenario,
  jobs: number,
  synchronous: SynchronousMode,
  journal: 'WAL' | 'DELETE' = 'WAL',
  drainOnly = false,
): Promise<ContentionRunOutcome> {
  const directory = mkdtempSync(join(tmpdir(), 'walq-bench-'))
  const gate = new SharedArrayBuffer(4)
  const timeout = guard(runTimeout, 'run timed out')
  const counts = distribute(jobs, scenario.threads)
  const path = join(directory, 'walq.sqlite')
  const channels: Channel[] = []

  try {
    await prepareDatabase(path, synchronous, journal, drainOnly ? jobs : 0)

    for (let index = 0; index < scenario.threads; index += 1) {
      channels.push(
        startWorker({
          path,
          queue: 'bench',
          jobs: counts[index] ?? 0,
          batch: scenario.batch,
          timeout: runTimeout,
          gate,
          synchronous,
          journal,
          drainOnly,
        }),
      )
    }

    await Promise.race([
      Promise.all(channels.map((channel) => channel.ready.promise)),
      timeout.promise,
    ])
    let enqueues: EnqueueReport[] = []
    if (!drainOnly) {
      release(gate, 1)
      enqueues = await Promise.race([
        Promise.all(channels.map((channel) => channel.enqueue.promise)),
        timeout.promise,
      ])
    }
    release(gate, 2)
    const drains = await Promise.race([
      Promise.all(channels.map((channel) => channel.drain.promise)),
      timeout.promise,
    ])
    await Promise.race([
      Promise.all(channels.map((channel) => channel.exited.promise)),
      timeout.promise,
    ])

    const outcome = aggregateReports(enqueues, drains)
    if (drainOnly) {
      outcome.enqueued = jobs
      outcome.enqueueElapsed = 0
    }

    return outcome
  } finally {
    timeout.dispose()
    // Terminating is the only guaranteed stop, including for a thread stuck in a barrier.
    await Promise.all(channels.map((channel) => channel.worker.terminate()))
    rmSync(directory, { recursive: true, force: true })
  }
}

/** Reason why a run must stay out of the metrics, or undefined when it is trustworthy. */
export function invalidReason(outcome: ContentionRunOutcome, jobs: number): string | undefined {
  if (outcome.aborted) return 'run aborted'
  if (outcome.enqueued !== jobs) return `enqueued ${outcome.enqueued} of ${jobs} jobs`
  if (outcome.completed !== jobs) return `confirmed ${outcome.completed} of ${jobs} jobs`
  if (outcome.lostLeases > 0) return `${outcome.lostLeases} leases lost`
  if (outcome.duplicates !== 0) return `${outcome.duplicates} duplicate completions`
  if (outcome.errors > 0) return `${outcome.errors} storage errors`

  return undefined
}

export function summarizeRuns(
  scenario: ContentionScenario,
  jobs: number,
  collected: Collected<ContentionRunOutcome>,
): BenchmarkResult {
  const valid: ContentionRunOutcome[] = []
  const invalid: string[] = []
  for (const outcome of collected.outcomes) {
    const reason = invalidReason(outcome, jobs)
    if (reason === undefined) valid.push(outcome)
    else invalid.push(reason)
  }

  const runs = valid.length
  const enqueueRates = valid.map((outcome) => (outcome.enqueued / outcome.enqueueElapsed) * 1000)
  const drainRates = valid.map((outcome) => (outcome.completed / outcome.drainElapsed) * 1000)
  const claim = summarizePerRunMicros(valid.map((outcome) => outcome.claimSamples))
  const complete = summarizePerRunMicros(valid.map((outcome) => outcome.completeSamples))
  const completed = total(valid, (outcome) => outcome.completed)
  const claims = total(valid, (outcome) => outcome.claims)
  const errors = total(collected.outcomes, (outcome) => outcome.errors)
  const notes: string[] = []

  if (collected.failures.length > 0) {
    notes.push(
      `${collected.failures.length} of ${collected.outcomes.length + collected.failures.length} runs failed: ${collected.failures[0]}`,
    )
  }
  if (invalid.length > 0) {
    notes.push(`${invalid.length} of ${collected.outcomes.length} runs are invalid: ${invalid[0]}`)
  }

  return {
    suite: 'contention',
    scenario: scenarioName(scenario),
    params: {
      files: 1,
      threads: scenario.threads,
      batch: scenario.batch,
      jobs,
    },
    metrics: {
      'enqueue jobs/sec': median(enqueueRates),
      'drain jobs/sec': median(drainRates),
      'spread (%)': spread(drainRates),
      'jobs/claim': claims === 0 ? 0 : completed / claims,
      'empty claims': runs === 0 ? 0 : total(valid, (outcome) => outcome.emptyClaims) / runs,
      'claim p50 (µs)': claim.p50,
      'claim p95 (µs)': claim.p95,
      'claim p99 (µs)': claim.p99,
      'complete p95 (µs)': complete.p95,
      'drain (ms)': median(valid.map((outcome) => outcome.drainElapsed)),
      // Diagnostic across every run, including the excluded ones.
      errors,
    },
    samples: valid.map((outcome) => ({
      'enqueue jobs/sec': (outcome.enqueued / outcome.enqueueElapsed) * 1000,
      'drain jobs/sec': (outcome.completed / outcome.drainElapsed) * 1000,
      completed: outcome.completed,
      errors: outcome.errors,
      'drain (ms)': outcome.drainElapsed,
    })),
    notes,
    ok: notes.length === 0,
  }
}

/** Bind one workload to its result aggregation. */
export function defineContentionScenario(
  scenario: ContentionScenario,
  jobs: number,
  synchronous: SynchronousMode,
): ScenarioDefinition {
  return defineScenario({
    suite: 'contention',
    scenario: scenarioName(scenario),
    jobs,
    run: () => executeRun(scenario, jobs, synchronous),
    summarize: (collected) => summarizeRuns(scenario, jobs, collected),
  })
}
