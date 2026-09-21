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
  numeric,
  spread,
  summarizeMicros,
  type BenchmarkResult,
  type Collected,
} from './harness.js'
import { defineScenario, type ScenarioDefinition } from './scenario.js'

export type ClaimGroupingMode = 'current' | 'grouped'
export type ClaimGroupingPlacement = 'solo' | 'competing'

/** One `BEGIN IMMEDIATE ... COMMIT` executed by the Walq side, with how many jobs it claimed. */
export type TransactionSample = { micros: number; jobs: number }

export type ClaimGroupingGrid = {
  queues: number[]
  limits: number[]
  modes: ClaimGroupingMode[]
  placements: ClaimGroupingPlacement[]
  /** Queues per transaction for `grouped`; `undefined` means one transaction per whole round. */
  chunks: (number | undefined)[]
}

export type ClaimGroupingScenario = {
  queues: number
  limit: number
  mode: ClaimGroupingMode
  placement: ClaimGroupingPlacement
  chunkSize: number | undefined
  chunksConfigured: boolean
}

export const quickClaimGroupingGrid: ClaimGroupingGrid = {
  queues: [1, 8, 32],
  limits: [1, 16],
  modes: ['current', 'grouped'],
  placements: ['solo', 'competing'],
  chunks: [undefined],
}

export const fullClaimGroupingGrid: ClaimGroupingGrid = {
  queues: [1, 4, 8, 32],
  limits: [1, 4, 16],
  modes: ['current', 'grouped'],
  placements: ['solo', 'competing'],
  chunks: [undefined],
}

const metadata =
  'id, queue, name, data, status, createdAt, availableAt, attemptsMade, attempts, error'
const runTimeout = 60_000

type ClaimRequest = Pick<ClaimInput, 'queue' | 'limit' | 'now' | 'leaseDuration'>

type ClaimGroupingOutcome = {
  elapsed: number
  claimed: number
  duplicates: number
  transactions: TransactionSample[]
  eventLoopSamples: number[]
  competitor: ClaimCompetitorReport
}

class GroupedClaimer {
  readonly #recover: Database.Statement
  readonly #select: Database.Statement
  readonly #acquire: Database.Statement
  readonly #transaction: Database.Transaction<(requests: ClaimRequest[]) => ClaimedJob[]>

  constructor(db: Database.Database) {
    this.#recover = db
      .prepare(
        `
          UPDATE walq_jobs SET
            status = CASE WHEN attemptsMade < attempts THEN 'pending' ELSE 'failed' END,
            availableAt = CASE WHEN attemptsMade < attempts THEN expiresAt ELSE availableAt END,
            finishedAt = CASE WHEN attemptsMade < attempts THEN NULL ELSE @now END,
            leaseToken = NULL, expiresAt = NULL
          WHERE queue = @queue AND status = 'active' AND expiresAt <= @now
        `,
      )
      .safeIntegers(false)
    this.#select = db
      .prepare(
        `
          SELECT id FROM walq_jobs
          WHERE queue = @queue AND status = 'pending' AND availableAt <= @now
            AND attemptsMade < attempts
          ORDER BY availableAt, id COLLATE BINARY LIMIT @limit
        `,
      )
      .safeIntegers(false)
    this.#acquire = db
      .prepare(
        `
          UPDATE walq_jobs SET status = 'active', attemptsMade = attemptsMade + 1,
            leaseToken = @leaseToken, expiresAt = @expiresAt
          WHERE id = @id
          RETURNING ${metadata}, leaseToken, expiresAt
        `,
      )
      .safeIntegers(false)
    this.#transaction = db.transaction((requests: ClaimRequest[]): ClaimedJob[] => {
      const claimed: ClaimedJob[] = []
      for (const request of requests) {
        const expiresAt = request.now + request.leaseDuration
        this.#recover.run(request)
        const jobs = this.#select.all(request) as { id: string }[]
        for (const { id } of jobs) {
          claimed.push(this.#acquire.get({ id, leaseToken: randomUUID(), expiresAt }) as ClaimedJob)
        }
      }
      return claimed
    })
  }

  claim(
    requests: ClaimRequest[],
    chunkSize: number | undefined,
    transactions: TransactionSample[],
  ): ClaimedJob[] {
    const claimed: ClaimedJob[] = []
    for (const chunk of chunkRequests(requests, chunkSize)) {
      const started = performance.now()
      const jobs = this.#transaction.immediate(chunk)
      transactions.push({ micros: performance.now() - started, jobs: jobs.length })
      claimed.push(...jobs)
    }
    return claimed
  }
}

function chunkRequests(requests: ClaimRequest[], chunkSize: number | undefined): ClaimRequest[][] {
  if (chunkSize === undefined || chunkSize >= requests.length) return [requests]

  const chunks: ClaimRequest[][] = []
  for (let index = 0; index < requests.length; index += chunkSize) {
    chunks.push(requests.slice(index, index + chunkSize))
  }
  return chunks
}

function emptyCompetitor(): ClaimCompetitorReport {
  return { enqueueSamples: [], completeSamples: [], operations: 0, errors: 0, firstError: null }
}

export function claimGroupingScenarios(grid: ClaimGroupingGrid): ClaimGroupingScenario[] {
  const chunksConfigured = grid.chunks.length > 1 || grid.chunks[0] !== undefined
  const scenarios: ClaimGroupingScenario[] = []
  for (const queues of grid.queues) {
    for (const limit of grid.limits) {
      for (const placement of grid.placements) {
        for (const mode of grid.modes) {
          const chunks = mode === 'grouped' ? grid.chunks : [undefined]
          for (const chunkSize of chunks) {
            scenarios.push({
              queues,
              limit,
              mode,
              placement,
              chunkSize,
              chunksConfigured: mode === 'grouped' && chunksConfigured,
            })
          }
        }
      }
    }
  }
  return scenarios
}

/** Optional override that replaces the queue tiers, for example `BENCH_CLAIM_QUEUES=32,64,128`. */
export function claimQueueOverride(value: string | undefined): number[] | undefined {
  return csvNumbers(value, 'BENCH_CLAIM_QUEUES')
}

/** Optional override that replaces the claim limits, for example `BENCH_CLAIM_LIMITS=16`. */
export function claimLimitOverride(value: string | undefined): number[] | undefined {
  return csvNumbers(value, 'BENCH_CLAIM_LIMITS')
}

/**
 * Optional override that replaces the grouped chunk tiers. Accepts positive integers and `all`
 * (one transaction for the whole round), for example `BENCH_CLAIM_CHUNKS=all,16,32,64`.
 */
export function claimChunkOverride(value: string | undefined): (number | undefined)[] | undefined {
  if (value === undefined || value === '') return undefined

  return value.split(',').map((part) => {
    const token = part.trim().toLowerCase()
    if (token === 'all') return undefined
    const parsed = Number(token)
    if (!Number.isInteger(parsed) || parsed <= 0) {
      throw new Error(
        `BENCH_CLAIM_CHUNKS must be a comma-separated list of positive integers or "all", received "${value}"`,
      )
    }
    return parsed
  })
}

/** Optional override that replaces the claim modes, for example `BENCH_CLAIM_MODES=grouped`. */
export function claimModeOverride(value: string | undefined): ClaimGroupingMode[] | undefined {
  if (value === undefined || value === '') return undefined

  return value.split(',').map((part) => {
    const token = part.trim()
    if (token !== 'current' && token !== 'grouped') {
      throw new Error(
        `BENCH_CLAIM_MODES must be a comma-separated list of current/grouped, received "${value}"`,
      )
    }
    return token
  })
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

/** Apply the queue/limit/mode/chunk overrides without mutating the source grid. */
export function withClaimGroupingTiers(
  grid: ClaimGroupingGrid,
  overrides: {
    queues: number[] | undefined
    limits: number[] | undefined
    modes?: ClaimGroupingMode[] | undefined
    chunks?: (number | undefined)[] | undefined
  },
): ClaimGroupingGrid {
  return {
    ...grid,
    queues: overrides.queues ?? grid.queues,
    limits: overrides.limits ?? grid.limits,
    modes: overrides.modes ?? grid.modes,
    chunks: overrides.chunks ?? grid.chunks,
  }
}

export function claimGroupingScenarioName(scenario: ClaimGroupingScenario): string {
  const base = `${scenario.mode} / ${scenario.placement} / ${scenario.queues} queues / limit ${scenario.limit}`
  if (!scenario.chunksConfigured) return base
  return `${base} / chunk ${scenario.chunkSize ?? 'all'}`
}

function prepareJobs(db: Database.Database, queues: string[], jobs: number): void {
  const insert = db.prepare(`
    INSERT INTO walq_jobs (
      id, queue, name, data, status, createdAt, availableAt, attemptsMade, attempts
    ) VALUES (@id, @queue, 'job', '{}', 'pending', @now, @now, 0, 1)
  `)
  const counts = distribute(jobs, queues.length)
  const now = Date.now()
  const populate = db.transaction(() => {
    for (const [index, queue] of queues.entries()) {
      for (let count = 0; count < (counts[index] ?? 0); count += 1) {
        insert.run({ id: randomUUID(), queue, now })
      }
    }
  })
  populate.immediate()
}

function nextTurn(): Promise<number> {
  const started = performance.now()
  return new Promise((resolve) => setImmediate(() => resolve(performance.now() - started)))
}

async function claimCurrent(
  storage: Storage,
  requests: ClaimRequest[],
  transactions: TransactionSample[],
): Promise<ClaimedJob[]> {
  const claimed: ClaimedJob[] = []
  for (const request of requests) {
    const started = performance.now()
    const jobs = await storage.claim(request)
    transactions.push({ micros: performance.now() - started, jobs: jobs.length })
    claimed.push(...jobs)
  }
  return claimed
}

function claimGrouped(
  claimer: GroupedClaimer,
  requests: ClaimRequest[],
  chunkSize: number | undefined,
  transactions: TransactionSample[],
): ClaimedJob[] {
  return claimer.claim(requests, chunkSize, transactions)
}

function startCompetitor(
  path: string,
  synchronous: SynchronousMode,
): {
  start: () => Promise<void>
  waitForSamples: (minimum: number) => Promise<void>
  stop: () => Promise<ClaimCompetitorReport>
  terminate: () => Promise<number>
} {
  const controlBuffer = new SharedArrayBuffer(12)
  const control = new Int32Array(controlBuffer)
  const input: ClaimCompetitorInput = { path, control: controlBuffer, synchronous }
  const worker = new Worker(new URL('./fixtures/claim-competitor-worker.ts', import.meta.url), {
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
  const grouped = new GroupedClaimer(db)
  const competitor =
    scenario.placement === 'competing' ? startCompetitor(path, synchronous) : undefined
  const transactions: TransactionSample[] = []
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
      const now = Date.now()
      const requests = queues.map((queue) => ({
        queue,
        limit: scenario.limit,
        now,
        leaseDuration: 60_000,
      }))
      const turn = nextTurn()
      const roundStarted = performance.now()
      const jobsInRound =
        scenario.mode === 'current'
          ? await claimCurrent(storage, requests, transactions)
          : claimGrouped(grouped, requests, scenario.chunkSize, transactions)
      elapsed += performance.now() - roundStarted
      eventLoopSamples.push(await turn)
      for (const job of jobsInRound) claimedIds.add(job.id)
      claimed += jobsInRound.length
      if (jobsInRound.length === 0) throw new Error(`claiming stopped after ${claimed} jobs`)

      round += 1
      if (competitor !== undefined && round % pauseEvery === 0) {
        await new Promise((resolve) => setTimeout(resolve, 3))
      }
    }
    if (competitor !== undefined) await competitor.waitForSamples(20)
    const competitorReport =
      competitor === undefined
        ? emptyCompetitor()
        : await Promise.race([competitor.stop(), timeout.promise])

    return {
      elapsed,
      claimed,
      duplicates: claimed - claimedIds.size,
      transactions,
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

function invalidReason(outcome: ClaimGroupingOutcome, jobs: number): string | undefined {
  if (outcome.claimed !== jobs) return `claimed ${outcome.claimed} of ${jobs} jobs`
  if (outcome.duplicates !== 0) return `${outcome.duplicates} duplicate claims`
  if (outcome.competitor.errors !== 0) {
    return `${outcome.competitor.errors} competitor errors: ${outcome.competitor.firstError ?? 'unknown'}`
  }
  return undefined
}

function summarizeRuns(
  scenario: ClaimGroupingScenario,
  jobs: number,
  collected: Collected<ClaimGroupingOutcome>,
): BenchmarkResult {
  const valid = collected.outcomes.filter((outcome) => invalidReason(outcome, jobs) === undefined)
  const invalid = collected.outcomes
    .map((outcome) => invalidReason(outcome, jobs))
    .filter((reason): reason is string => reason !== undefined)
  const rates = valid.map((outcome) => (outcome.claimed / outcome.elapsed) * 1000)
  const transaction = summarizeMicros(
    valid.flatMap((outcome) => outcome.transactions.map((sample) => sample.micros)),
  )
  const transactionJobs = valid.flatMap((outcome) =>
    outcome.transactions.map((sample) => sample.jobs),
  )
  const commits = valid.map((outcome) => outcome.transactions.length)
  const jobsPerTransaction =
    transactionJobs.length === 0
      ? 0
      : transactionJobs.reduce((total, jobs) => total + jobs, 0) / transactionJobs.length
  const eventLoop = summarizeMicros(valid.flatMap((outcome) => outcome.eventLoopSamples))
  const enqueue = summarizeMicros(valid.flatMap((outcome) => outcome.competitor.enqueueSamples))
  const complete = summarizeMicros(valid.flatMap((outcome) => outcome.competitor.completeSamples))
  const notes = [...collected.failures]
  if (invalid.length > 0) notes.push(...invalid)
  if (
    scenario.placement === 'competing' &&
    valid.some((outcome) => outcome.competitor.operations === 0)
  ) {
    notes.push('competitor completed no operations')
  }

  return {
    suite: 'claim-grouping',
    scenario: claimGroupingScenarioName(scenario),
    params: {
      mode: scenario.mode,
      placement: scenario.placement,
      queues: scenario.queues,
      limit: scenario.limit,
      chunk: scenario.chunkSize ?? 'all',
      jobs,
    },
    metrics: {
      'jobs/sec': median(rates),
      'spread (%)': spread(rates),
      'transaction p50 (µs)': transaction.p50,
      'transaction p95 (µs)': transaction.p95,
      'transaction p99 (µs)': transaction.p99,
      'jobs/transaction': jobsPerTransaction,
      commits: median(commits),
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
      commits: outcome.transactions.length,
      'competitor ops': outcome.competitor.operations,
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
    throughput: (result) => numeric(result.metrics['jobs/sec']),
    latency: (result) => result.samples.map((sample) => numeric(sample['elapsed (ms)'])),
  })
}
