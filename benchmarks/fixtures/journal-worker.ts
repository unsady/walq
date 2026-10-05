import { performance } from 'node:perf_hooks'
import { parentPort, workerData } from 'node:worker_threads'

import { betterSqlite3 } from '@walq/better-sqlite3'
import type { ClaimedJob } from '@walq/core/storage'
import Database from 'better-sqlite3'

import type { SynchronousMode } from '../bench-options.js'

export interface JournalWorkerInput {
  path: string
  queue: string
  batch: number
  timeout: number
  gate: SharedArrayBuffer
  synchronous: SynchronousMode
  journal: 'WAL' | 'DELETE'
}

export interface DrainReport {
  phase: 'drain'
  startedAt: number
  finishedAt: number
  completed: number
  lostLeases: number
  /** Ids of every applied completion, so the parent can check global uniqueness. */
  completedIds: string[]
  claims: number
  emptyClaims: number
  claimSamples: number[]
  completeSamples: number[]
  emptyClaimSamples: number[]
  loopSamples: number[]
  busy: number
  timeouts: number
  errors: number
  aborted: boolean
  firstError: string | null
}

export type WorkerReport = { phase: 'ready' } | DrainReport

const errorLimit = 20
const leaseDuration = 30_000
const warmupJobs = 50

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/** Monotonic clock shared by every thread of the process. */
function clock(): number {
  return performance.timeOrigin + performance.now()
}

const input = workerData as JournalWorkerInput
const gate = new Int32Array(input.gate)
const db = new Database(input.path)
db.pragma(`journal_mode = ${input.journal}`)
db.pragma(`synchronous = ${input.synchronous.toUpperCase()}`)
db.pragma('busy_timeout = 2000')
const storage = betterSqlite3(db)
if (db.pragma('journal_mode', { simple: true }) !== input.journal.toLowerCase()) {
  throw new Error('unexpected journal mode')
}
if (db.pragma('synchronous', { simple: true }) !== (input.synchronous === 'full' ? 2 : 1)) {
  throw new Error('unexpected synchronous mode')
}

function awaitPhase(phase: number): void {
  for (;;) {
    const current = Atomics.load(gate, 0)
    if (current >= phase) return
    Atomics.wait(gate, 0, current, 100)
  }
}

/** Warm this thread's JIT before the barrier, so repeats are not cold starts. */
async function warmup(): Promise<void> {
  const db = new Database(':memory:')
  try {
    const storage = betterSqlite3(db)
    for (let index = 0; index < warmupJobs; index += 1) {
      const timestamp = Date.now()
      await storage.enqueue({
        queue: 'warmup',
        name: 'warmup',
        data: '{}',
        now: timestamp,
        availableAt: timestamp,
        priority: 0,
        attempts: 1,
      })
      const claimed = await storage.claim({
        queue: 'warmup',
        limit: 1,
        now: Date.now(),
        leaseDuration,
      })
      const job = claimed[0]
      if (job === undefined) throw new Error('warmup claim returned no job')
      await storage.complete({ id: job.id, leaseToken: job.leaseToken, now: Date.now() })
    }
  } finally {
    db.close()
  }
}

async function runDrain(): Promise<DrainReport> {
  const claimSamples: number[] = []
  const completeSamples: number[] = []
  const completedIds: string[] = []
  const emptyClaimSamples: number[] = []
  const loopSamples: number[] = []
  let busy = 0
  let timeouts = 0
  let completed = 0
  let lostLeases = 0
  let claims = 0
  let emptyClaims = 0
  let errors = 0
  let aborted = false
  let firstError: string | null = null
  const startedAt = clock()
  const emptyLimit = 128
  let previousTurn = performance.now()

  function recordError(error: unknown): void {
    errors += 1
    firstError ??= errorMessage(error)
    const code = error instanceof Error ? ((error as Error & { code?: string }).code ?? '') : ''
    if (code.startsWith('SQLITE_BUSY')) busy += 1
    if (code === 'SQLITE_BUSY_TIMEOUT') timeouts += 1
  }

  for (;;) {
    await new Promise<void>((resolve) => setImmediate(resolve))
    const turn = performance.now()
    loopSamples.push(turn - previousTurn)
    previousTurn = turn

    if (errors >= errorLimit || clock() - startedAt > input.timeout) {
      aborted = true
      break
    }

    const claimStarted = performance.now()
    let jobs: ClaimedJob[]
    try {
      jobs = await storage.claim({
        queue: input.queue,
        limit: input.batch,
        now: Date.now(),
        leaseDuration,
      })
    } catch (error) {
      recordError(error)
      continue
    }

    claims += 1
    const claimDuration = performance.now() - claimStarted
    if (jobs.length === 0) {
      emptyClaimSamples.push(claimDuration)
      emptyClaims += 1
      if (emptyClaims >= emptyLimit) break
      continue
    }
    claimSamples.push(claimDuration)

    for (const job of jobs) {
      const completeStarted = performance.now()
      try {
        const result = await storage.complete({
          id: job.id,
          leaseToken: job.leaseToken,
          now: Date.now(),
        })
        completeSamples.push(performance.now() - completeStarted)
        if (result === 'applied') {
          completed += 1
          completedIds.push(job.id)
        } else {
          lostLeases += 1
        }
      } catch (error) {
        recordError(error)
      }
    }
  }

  if (aborted && clock() - startedAt > input.timeout) timeouts += 1

  return {
    phase: 'drain',
    startedAt,
    finishedAt: clock(),
    completed,
    lostLeases,
    completedIds,
    claims,
    emptyClaims,
    claimSamples,
    completeSamples,
    emptyClaimSamples,
    loopSamples,
    busy,
    timeouts,
    errors,
    aborted,
    firstError,
  }
}

try {
  await warmup()
  parentPort!.postMessage({ phase: 'ready' })
  awaitPhase(1)
  parentPort!.postMessage(await runDrain())
} finally {
  db.close()
}
