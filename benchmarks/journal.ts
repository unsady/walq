import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Worker } from 'node:worker_threads'

import { betterSqlite3 } from '@walq/better-sqlite3'
import Database from 'better-sqlite3'

import { synchronousPragma, type SynchronousMode } from './bench-options.js'
import type { DrainReport, JournalWorkerInput, WorkerReport } from './fixtures/journal-worker.js'
import { deferred, guard, type Deferred } from './harness.js'

export interface JournalScenario {
  batch: number
  journal: 'WAL' | 'DELETE'
  synchronous: SynchronousMode
}

export interface JournalRunOutcome extends Omit<
  DrainReport,
  'phase' | 'completedIds' | 'startedAt' | 'finishedAt'
> {
  drainElapsed: number
  duplicates: number
}

interface Channel {
  worker: Worker
  ready: Deferred<void>
  drain: Deferred<DrainReport>
  exited: Deferred<number>
}

const runTimeout = 60_000

async function prepareDatabase(
  path: string,
  scenario: JournalScenario,
  jobs: number,
): Promise<void> {
  const db = new Database(path)

  try {
    db.pragma(`journal_mode = ${scenario.journal}`)
    db.pragma(synchronousPragma(scenario.synchronous))
    db.pragma('busy_timeout = 2000')
    const storage = betterSqlite3(db)
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
  } finally {
    db.close()
  }
}

function startWorker(input: JournalWorkerInput): Channel {
  const worker = new Worker(new URL('./fixtures/journal-worker.js', import.meta.url), {
    workerData: input,
  })
  const ready = deferred<void>()
  const drain = deferred<DrainReport>()
  const exited = deferred<number>()

  function fail(error: unknown): void {
    ready.reject(error)
    drain.reject(error)
    exited.reject(error)
  }

  worker.on('message', (report: WorkerReport) => {
    if (report.phase === 'ready') ready.resolve()
    else drain.resolve(report)
  })
  worker.on('error', fail)
  worker.on('exit', (code) => {
    if (code === 0) exited.resolve(0)
    else fail(new Error(`journal worker exited with code ${code}`))
  })

  return { worker, ready, drain, exited }
}

export function outcomeFromReport(report: DrainReport): JournalRunOutcome {
  const { phase: _phase, completedIds, startedAt, finishedAt, ...outcome } = report

  return {
    ...outcome,
    drainElapsed: finishedAt - startedAt,
    duplicates: report.completed - new Set(completedIds).size,
  }
}

export async function executeRun(
  scenario: JournalScenario,
  jobs: number,
): Promise<JournalRunOutcome> {
  const directory = mkdtempSync(join(tmpdir(), 'walq-journal-bench-'))
  const gate = new SharedArrayBuffer(4)
  const timeout = guard(runTimeout, 'run timed out')
  const path = join(directory, 'walq.sqlite')
  let channel: Channel | undefined

  try {
    await prepareDatabase(path, scenario, jobs)
    channel = startWorker({
      path,
      queue: 'bench',
      batch: scenario.batch,
      timeout: runTimeout,
      gate,
      synchronous: scenario.synchronous,
      journal: scenario.journal,
    })

    await Promise.race([channel.ready.promise, timeout.promise])
    const control = new Int32Array(gate)
    Atomics.store(control, 0, 1)
    Atomics.notify(control, 0)
    const report = await Promise.race([channel.drain.promise, timeout.promise])
    await Promise.race([channel.exited.promise, timeout.promise])

    return outcomeFromReport(report)
  } finally {
    timeout.dispose()
    // Guaranteed stop, including when the worker is stuck in the barrier.
    await channel?.worker.terminate()
    rmSync(directory, { recursive: true, force: true })
  }
}

/** Reason why a run must stay out of the metrics. */
export function invalidReason(outcome: JournalRunOutcome, jobs: number): string | undefined {
  if (outcome.aborted) return 'run aborted'
  if (outcome.completed !== jobs) return `confirmed ${outcome.completed} of ${jobs} jobs`
  if (outcome.lostLeases > 0) return `${outcome.lostLeases} leases lost`
  if (outcome.duplicates !== 0) return `${outcome.duplicates} duplicate completions`
  if (outcome.errors > 0) return `${outcome.errors} storage errors`
  if (!Number.isFinite(outcome.drainElapsed) || outcome.drainElapsed <= 0)
    return 'invalid drain duration'

  return undefined
}
