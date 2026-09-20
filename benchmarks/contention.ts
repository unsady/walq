import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Worker } from 'node:worker_threads'

import { betterSqlite3 } from '@walq/better-sqlite3'
import Database from 'better-sqlite3'

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
  numeric,
  spread,
  summarizeMicros,
  type BenchmarkResult,
  type Collected,
  type Deferred,
} from './harness.js'
import { defineScenario, type ScenarioDefinition } from './scenario.js'

export type ContentionPlacement = 'shared' | 'per-thread'

export type ContentionGrid = {
  threads: number[]
  batches: number[]
  placements: ContentionPlacement[]
}

export type ContentionScenario = {
  threads: number
  batch: number
  placement: ContentionPlacement
}

export const quickContentionGrid: ContentionGrid = {
  threads: [1, 4],
  batches: [1, 16],
  placements: ['shared', 'per-thread'],
}

export const fullContentionGrid: ContentionGrid = {
  threads: [1, 2, 4, 8],
  batches: [1, 4, 16, 64],
  placements: ['shared', 'per-thread'],
}

const runTimeout = 60_000

type Channel = {
  worker: Worker
  ready: Deferred<void>
  enqueue: Deferred<EnqueueReport>
  drain: Deferred<DrainReport>
  exited: Deferred<number>
}

export type ContentionRunOutcome = {
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
}

export function scenarioName(scenario: ContentionScenario): string {
  return `${scenario.placement} / ${scenario.threads} threads / batch ${scenario.batch}`
}

export function contentionScenarios(grid: ContentionGrid): ContentionScenario[] {
  const scenarios: ContentionScenario[] = []
  for (const threads of grid.threads) {
    for (const batch of grid.batches) {
      for (const placement of grid.placements) {
        // A single thread owns a single file either way.
        if (threads === 1 && placement === 'per-thread') continue
        scenarios.push({ threads, batch, placement })
      }
    }
  }

  return scenarios
}

/** Create the schema once so that workers never race during startup. */
function prepareDatabase(path: string): void {
  const db = new Database(path)
  try {
    db.pragma('journal_mode = WAL')
    db.pragma('synchronous = NORMAL')
    db.pragma('busy_timeout = 2000')
    betterSqlite3(db)
  } finally {
    db.close()
  }
}

function startWorker(input: ContentionWorkerInput): Channel {
  const worker = new Worker(new URL('./fixtures/contention-worker.ts', import.meta.url), {
    workerData: input,
  })
  const ready = deferred<void>()
  const enqueue = deferred<EnqueueReport>()
  const drain = deferred<DrainReport>()
  const exited = deferred<number>()
  const fail = (error: unknown): void => {
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
    enqueueElapsed: span(enqueues),
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
  }
}

async function executeRun(
  scenario: ContentionScenario,
  jobs: number,
): Promise<ContentionRunOutcome> {
  const directory = mkdtempSync(join(tmpdir(), 'walq-bench-'))
  const gate = new SharedArrayBuffer(4)
  const timeout = guard(runTimeout, 'run timed out')
  const counts = distribute(jobs, scenario.threads)
  const paths = Array.from({ length: scenario.threads }, (_, index) =>
    join(directory, `walq-${scenario.placement === 'shared' ? 0 : index}.sqlite`),
  )
  const channels: Channel[] = []

  try {
    for (const path of new Set(paths)) prepareDatabase(path)
    for (const [index, path] of paths.entries()) {
      channels.push(
        startWorker({
          path,
          queue: 'bench',
          jobs: counts[index] ?? 0,
          batch: scenario.batch,
          timeout: runTimeout,
          gate,
        }),
      )
    }

    await Promise.race([
      Promise.all(channels.map((channel) => channel.ready.promise)),
      timeout.promise,
    ])
    release(gate, 1)
    const enqueues = await Promise.race([
      Promise.all(channels.map((channel) => channel.enqueue.promise)),
      timeout.promise,
    ])
    release(gate, 2)
    const drains = await Promise.race([
      Promise.all(channels.map((channel) => channel.drain.promise)),
      timeout.promise,
    ])
    await Promise.race([
      Promise.all(channels.map((channel) => channel.exited.promise)),
      timeout.promise,
    ])

    return aggregateReports(enqueues, drains)
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
  const claimSamples = valid.flatMap((outcome) => outcome.claimSamples)
  const completeSamples = valid.flatMap((outcome) => outcome.completeSamples)
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
      files: scenario.placement === 'shared' ? 1 : scenario.threads,
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
      'claim p50 (µs)': summarizeMicros(claimSamples).p50,
      'claim p95 (µs)': summarizeMicros(claimSamples).p95,
      'claim p99 (µs)': summarizeMicros(claimSamples).p99,
      'complete p95 (µs)': summarizeMicros(completeSamples).p95,
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

/** Adapter used by the Vitest benchmark files. */
export function defineContentionScenario(
  scenario: ContentionScenario,
  jobs: number,
): ScenarioDefinition {
  return defineScenario({
    suite: 'contention',
    scenario: scenarioName(scenario),
    jobs,
    run: () => executeRun(scenario, jobs),
    summarize: (collected) => summarizeRuns(scenario, jobs, collected),
    throughput: (result) => numeric(result.metrics['drain jobs/sec']),
    latency: (result) => result.samples.map((sample) => numeric(sample['drain (ms)'])),
  })
}
