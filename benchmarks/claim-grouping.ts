import { randomUUID } from 'node:crypto'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { performance } from 'node:perf_hooks'
import { Worker } from 'node:worker_threads'

import { betterSqlite3 } from '@walq/better-sqlite3'
import type { ClaimedJob, ClaimInput, Storage } from '@walq/core/storage'
import Database from 'better-sqlite3'

import { synchronousPragma, type SynchronousMode } from './bench-options.js'
import type {
  ClaimCompetitorInput,
  ClaimCompetitorReport,
} from './fixtures/claim-competitor-worker.js'
import {
  deferred,
  distribute,
  guard,
  median,
  spread,
  summarizePerRunMicros,
  type BenchmarkResult,
  type Collected,
} from './harness.js'
import { defineScenario, type ScenarioDefinition } from './scenario.js'

export type ClaimGroupingPlacement = 'solo' | 'competing'

/** One whole production `claimQueues` call, which may span multiple transactions. */
export interface CallSample {
  duration: number
  jobs: number
}

/**
 * Minimum number of enqueue and complete samples the competing writer must produce before its
 * latency percentiles are trustworthy. Every competitor operation emits one of each, so this is
 * also the number of operations `executeRun` waits for.
 */
export const minimumCompetitorSamples = 20

export interface ClaimGroupingGrid {
  queues: number[]
  limits: number[]
  placements: ClaimGroupingPlacement[]
}

export interface ClaimGroupingScenario {
  queues: number
  limit: number
  placement: ClaimGroupingPlacement
}

export const quickClaimGroupingGrid: ClaimGroupingGrid = {
  queues: [32],
  limits: [16],
  placements: ['solo', 'competing'],
}

export const fullClaimGroupingGrid: ClaimGroupingGrid = {
  queues: [1, 32],
  limits: [1, 16],
  placements: ['solo', 'competing'],
}

const runTimeout = 60_000

type ClaimRequest = Pick<ClaimInput, 'queue' | 'limit' | 'now' | 'leaseDuration'>

export interface ClaimGroupingOutcome {
  elapsed: number
  claimed: number
  duplicates: number
  calls: CallSample[]
  eventLoopSamples: number[]
  competitor: ClaimCompetitorReport
}

function emptyCompetitor(): ClaimCompetitorReport {
  return { enqueueSamples: [], completeSamples: [], operations: 0, errors: 0, firstError: null }
}

export function claimGroupingScenarios(grid: ClaimGroupingGrid): ClaimGroupingScenario[] {
  return grid.queues.flatMap((queues) =>
    grid.limits.flatMap((limit) =>
      grid.placements.map((placement) => ({ queues, limit, placement })),
    ),
  )
}

/** Optional override that replaces the queue tiers, for example `BENCH_CLAIM_QUEUES=32,64,128`. */
export function claimQueueOverride(value: string | undefined): number[] | undefined {
  return csvNumbers(value, 'BENCH_CLAIM_QUEUES')
}

/** Optional override that replaces the claim limits, for example `BENCH_CLAIM_LIMITS=16`. */
export function claimLimitOverride(value: string | undefined): number[] | undefined {
  return csvNumbers(value, 'BENCH_CLAIM_LIMITS')
}

function csvNumbers(value: string | undefined, label: string): number[] | undefined {
  if (value === undefined || value === '') return undefined

  return value.split(',').map((part) => {
    const parsed = Number(part.trim())
    if (!Number.isInteger(parsed) || parsed <= 0) {
      throw new Error(
        `${label} must be a comma-separated list of positive integers, received "${value}"`,
      )
    }
    return parsed
  })
}

/** Apply queue and limit overrides without mutating the source grid. */
export function withClaimGroupingTiers(
  grid: ClaimGroupingGrid,
  overrides: {
    queues: number[] | undefined
    limits: number[] | undefined
  },
): ClaimGroupingGrid {
  return {
    ...grid,
    queues: overrides.queues ?? grid.queues,
    limits: overrides.limits ?? grid.limits,
  }
}

export function claimGroupingScenarioName(scenario: ClaimGroupingScenario): string {
  return `production / ${scenario.placement} / ${scenario.queues} queues / limit ${scenario.limit}`
}

export function prepareJobs(
  db: Database.Database,
  queues: string[],
  jobs: number,
  data = '{}',
): void {
  const insert = db.prepare(`
    INSERT INTO walq_jobs (
      id, queue, name, data, status, createdAt, availableAt, priority, attemptsMade, attempts
    ) VALUES (@id, @queue, 'job', @data, 'pending', @now, @now, 0, 0, 1)
  `)
  const counts = distribute(jobs, queues.length)
  const now = Date.now()
  const populate = db.transaction(() => {
    for (const [index, queue] of queues.entries()) {
      for (let count = 0; count < (counts[index] ?? 0); count += 1) {
        insert.run({ id: randomUUID(), queue, now, data })
      }
    }
  })
  populate.immediate()
}

function nextTurn(): Promise<number> {
  const started = performance.now()
  return new Promise((resolve) => setImmediate(() => resolve(performance.now() - started)))
}

/** Observe every event-loop turn throughout a complete claim round. */
export async function measureClaimRound<Value>(
  run: () => Value | Promise<Value>,
  samples: number[],
): Promise<{ value: Value; duration: number }> {
  let active = true
  const probe = (async () => {
    while (active) samples.push(await nextTurn())
  })()
  const started = performance.now()

  try {
    const value = await run()

    return { value, duration: performance.now() - started }
  } finally {
    active = false
    await probe
  }
}

/**
 * Shipped path: one `claimQueues` call per round, with the adapter's internal chunking. The
 * sample covers the whole call, which may open several transactions, so this is API call
 * latency, not transaction latency; `summarizeRuns` labels it accordingly.
 */
async function claimProduction(
  storage: Storage,
  requests: ClaimRequest[],
  calls: CallSample[],
): Promise<ClaimedJob[]> {
  const claimQueues = storage.claimQueues
  if (claimQueues === undefined) throw new Error('storage does not implement claimQueues')
  const started = performance.now()
  const results = await claimQueues.call(storage, { requests })
  const jobs = results.flat()
  calls.push({ duration: performance.now() - started, jobs: jobs.length })
  return jobs
}

export function startCompetitor(
  path: string,
  synchronous: SynchronousMode,
  preparedLeases?: number,
  production = false,
): {
  start: () => Promise<void>
  waitForSamples: (minimum: number) => Promise<void>
  stop: () => Promise<ClaimCompetitorReport>
  terminate: () => Promise<number>
} {
  const controlBuffer = new SharedArrayBuffer(12)
  const control = new Int32Array(controlBuffer)
  const input: ClaimCompetitorInput = {
    path,
    control: controlBuffer,
    synchronous,
    production,
    ...(preparedLeases === undefined ? {} : { preparedLeases }),
  }
  const worker = new Worker(new URL('./fixtures/claim-competitor-worker.js', import.meta.url), {
    workerData: input,
  })
  const ready = deferred<void>()
  const result = deferred<ClaimCompetitorReport>()

  worker.on('message', (message: unknown) => {
    const value = message as {
      phase: 'ready' | 'result' | 'error'
      report?: ClaimCompetitorReport
      error?: string
    }
    if (value.phase === 'ready') ready.resolve()
    else if (value.phase === 'result' && value.report !== undefined) result.resolve(value.report)
    else if (value.phase === 'error') {
      const error = new Error(value.error ?? 'competitor failed')
      ready.reject(error)
      result.reject(error)
    }
  })
  worker.on('error', (error) => {
    ready.reject(error)
    result.reject(error)
  })
  worker.on('exit', (code) => {
    if (code !== 0) result.reject(new Error(`claim competitor exited with code ${code}`))
  })

  return {
    start: async () => {
      await ready.promise
      Atomics.store(control, 0, 1)
      Atomics.notify(control, 0)
    },
    waitForSamples: async (minimum) => {
      // Every competitor operation emits one enqueue and one complete sample.
      for (let attempt = 0; attempt < 40 && Atomics.load(control, 2) < minimum; attempt += 1) {
        await new Promise((resolve) => setTimeout(resolve, 3))
      }
    },
    stop: async () => {
      Atomics.store(control, 1, 1)
      Atomics.notify(control, 1)
      return result.promise
    },
    terminate: () => worker.terminate(),
  }
}

async function executeRun(
  scenario: ClaimGroupingScenario,
  jobs: number,
  synchronous: SynchronousMode,
): Promise<ClaimGroupingOutcome> {
  const directory = mkdtempSync(join(tmpdir(), 'walq-claim-grouping-'))
  const path = join(directory, 'walq.sqlite')
  const db = new Database(path)
  db.pragma('journal_mode = WAL')
  db.pragma(synchronousPragma(synchronous))
  db.pragma('busy_timeout = 2000')
  const storage = betterSqlite3(db)
  const queues = Array.from({ length: scenario.queues }, (_, index) => `queue-${index}`)
  prepareJobs(db, queues, jobs)
  const competitor =
    scenario.placement === 'competing' ? startCompetitor(path, synchronous) : undefined
  const calls: CallSample[] = []
  const eventLoopSamples: number[] = []
  const claimedIds = new Set<string>()
  let claimed = 0
  let elapsed = 0
  let round = 0
  const expectedRounds = Math.ceil(Math.ceil(jobs / scenario.queues) / scenario.limit)
  const pauseEvery = Math.max(1, Math.floor(expectedRounds / 32))
  const timeout = guard(runTimeout, 'claim grouping run timed out')

  try {
    if (competitor !== undefined) await Promise.race([competitor.start(), timeout.promise])
    while (claimed < jobs) {
      timeout.check()

      const now = Date.now()
      const requests = queues.map((queue) => ({
        queue,
        limit: scenario.limit,
        now,
        leaseDuration: 60_000,
      }))
      const measured = await measureClaimRound(
        () => claimProduction(storage, requests, calls),
        eventLoopSamples,
      )
      const jobsInRound = measured.value
      elapsed += measured.duration
      timeout.check()

      for (const job of jobsInRound) claimedIds.add(job.id)
      claimed += jobsInRound.length
      if (jobsInRound.length === 0) throw new Error(`claiming stopped after ${claimed} jobs`)

      round += 1
      if (competitor !== undefined && round % pauseEvery === 0) {
        await new Promise((resolve) => setTimeout(resolve, 3))
      }
    }
    if (competitor !== undefined) {
      await Promise.race([competitor.waitForSamples(minimumCompetitorSamples), timeout.promise])
    }
    timeout.check()

    const competitorReport =
      competitor === undefined
        ? emptyCompetitor()
        : await Promise.race([competitor.stop(), timeout.promise])

    return {
      elapsed,
      claimed,
      duplicates: claimed - claimedIds.size,
      calls,
      eventLoopSamples,
      competitor: competitorReport,
    }
  } finally {
    timeout.dispose()
    await competitor?.terminate()
    db.close()
    rmSync(directory, { recursive: true, force: true })
  }
}

/** Reason why a run must stay out of the metrics, or undefined when it is trustworthy. */
export function invalidReason(
  outcome: ClaimGroupingOutcome,
  jobs: number,
  placement: ClaimGroupingPlacement,
): string | undefined {
  if (outcome.claimed !== jobs) return `claimed ${outcome.claimed} of ${jobs} jobs`
  if (outcome.duplicates !== 0) return `${outcome.duplicates} duplicate claims`
  if (outcome.competitor.errors !== 0) {
    return `${outcome.competitor.errors} competitor errors: ${outcome.competitor.firstError ?? 'unknown'}`
  }
  if (placement === 'competing') {
    const enqueue = outcome.competitor.enqueueSamples.length
    const complete = outcome.competitor.completeSamples.length
    if (enqueue < minimumCompetitorSamples || complete < minimumCompetitorSamples) {
      return `competitor produced ${enqueue} enqueue and ${complete} complete samples, need ${minimumCompetitorSamples} of each`
    }
  }

  return undefined
}

export function summarizeRuns(
  scenario: ClaimGroupingScenario,
  jobs: number,
  collected: Collected<ClaimGroupingOutcome>,
): BenchmarkResult {
  const valid = collected.outcomes.filter(
    (outcome) => invalidReason(outcome, jobs, scenario.placement) === undefined,
  )
  const invalid = collected.outcomes
    .map((outcome) => invalidReason(outcome, jobs, scenario.placement))
    .filter((reason): reason is string => reason !== undefined)
  const rates = valid.map((outcome) => (outcome.claimed / outcome.elapsed) * 1000)
  const callSummary = summarizePerRunMicros(
    valid.map((outcome) => outcome.calls.map((sample) => sample.duration)),
  )
  const callJobs = valid.flatMap((outcome) => outcome.calls.map((sample) => sample.jobs))
  const callCounts = valid.map((outcome) => outcome.calls.length)
  const jobsPerCall =
    callJobs.length === 0 ? 0 : callJobs.reduce((total, jobs) => total + jobs, 0) / callJobs.length
  const eventLoop = summarizePerRunMicros(valid.map((outcome) => outcome.eventLoopSamples))
  const enqueue = summarizePerRunMicros(valid.map((outcome) => outcome.competitor.enqueueSamples))
  const complete = summarizePerRunMicros(valid.map((outcome) => outcome.competitor.completeSamples))
  const notes = [...collected.failures]
  if (invalid.length > 0) notes.push(...invalid)

  return {
    suite: 'claim-grouping',
    scenario: claimGroupingScenarioName(scenario),
    params: {
      mode: 'production',
      placement: scenario.placement,
      queues: scenario.queues,
      limit: scenario.limit,
      chunk: 'internal',
      jobs,
      'event loop probe': 'setImmediate throughout each claim round',
    },
    metrics: {
      'jobs/sec': median(rates),
      'spread (%)': spread(rates),
      'claim call p50 (µs)': callSummary.p50,
      'claim call p95 (µs)': callSummary.p95,
      'claim call p99 (µs)': callSummary.p99,
      'jobs/claim call': jobsPerCall,
      'claim calls': median(callCounts),
      'event loop samples': eventLoop.count,
      'event loop p95 (µs)': eventLoop.p95,
      'event loop p99 (µs)': eventLoop.p99,
      'enqueue p95 (µs)': enqueue.p95,
      'enqueue p99 (µs)': enqueue.p99,
      'complete p95 (µs)': complete.p95,
      'complete p99 (µs)': complete.p99,
      'competitor ops': median(valid.map((outcome) => outcome.competitor.operations)),
    },
    samples: valid.map((outcome) => ({
      'jobs/sec': (outcome.claimed / outcome.elapsed) * 1000,
      'elapsed (ms)': outcome.elapsed,
      claimed: outcome.claimed,
      duplicates: outcome.duplicates,
      calls: outcome.calls.length,
      'competitor ops': outcome.competitor.operations,
      'event loop samples': outcome.eventLoopSamples.length,
      'event loop p95 (µs)': summarizePerRunMicros([outcome.eventLoopSamples]).p95,
      'event loop p99 (µs)': summarizePerRunMicros([outcome.eventLoopSamples]).p99,
    })),
    notes,
    ok: notes.length === 0,
  }
}

export function defineClaimGroupingScenario(
  scenario: ClaimGroupingScenario,
  jobs: number,
  synchronous: SynchronousMode,
): ScenarioDefinition {
  return defineScenario({
    suite: 'claim-grouping',
    scenario: claimGroupingScenarioName(scenario),
    jobs,
    run: () => executeRun(scenario, jobs, synchronous),
    summarize: (collected) => summarizeRuns(scenario, jobs, collected),
  })
}
