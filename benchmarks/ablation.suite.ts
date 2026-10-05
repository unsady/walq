import { mkdtempSync, rmSync } from 'node:fs'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'

import { Queue } from '@walq/core'
import type { ClaimedJob, ClaimInput, Storage } from '@walq/core/storage'
import Database from 'better-sqlite3'

import type { Connection } from '../packages/sqlite-common/src/driver.js'
import type { BenchEnvironment, SynchronousMode } from './bench-options.js'
import { measureClaimRound, prepareJobs, startCompetitor } from './claim-grouping.js'
import type { ClaimCompetitorReport } from './fixtures/claim-competitor-worker.js'
import {
  deferred,
  guard,
  matches,
  median,
  spread,
  summarizePerRunMicros,
  type BenchmarkResult,
  type Collected,
} from './harness.js'
import { defineScenario, type ScenarioDefinition } from './scenario.js'

export interface Variant {
  stage: number
  synchronous: SynchronousMode
  concurrency: number
  stress: boolean
  handler: 'shared-turn' | 'independent-turn'
}

interface ClaimStep {
  input: ClaimInput
  expiresAt: number
}

/** Benchmark-only access to the shipped transaction and validation, not alternate SQL. */
interface Internals {
  assertAutocommit: () => void
  prepareClaim: (input: ClaimInput) => ClaimStep
  claimTransaction: { immediate: (steps: ClaimStep[]) => ClaimedJob[][] }
}

const payload = JSON.stringify({ event: 'delivery', customerId: 42, body: 'x'.repeat(256) })

interface Call {
  duration: number
  jobs: number
  requests: number
  grouped: boolean
}

interface Outcome {
  elapsed: number
  jobs: number
  calls: Call[]
  transactions: Array<{ duration: number; jobs: number; claim: boolean }>
  eventLoop: number[]
  competitor: ClaimCompetitorReport
  sqlite: string
  driver: string
  pragmas: unknown[]
}

export function variants(stress: boolean, handler: Variant['handler'] = 'shared-turn'): Variant[] {
  return (stress ? [16] : [1, 16]).flatMap((concurrency) =>
    (stress ? [4, 5] : [1, 2, 3, 4, 5]).flatMap((stage) =>
      (['normal', 'full'] as const).map((synchronous) => ({
        stage,
        synchronous,
        concurrency,
        stress,
        handler,
      })),
    ),
  )
}

export function variantName(v: Variant): string {
  return `${v.stress ? 'stress' : 'process'} / ${v.stage} / ${v.stage === 1 ? 'DELETE' : 'WAL'} / ${v.synchronous} / concurrency ${v.concurrency}${v.stress ? '' : ` / ${v.handler}`}`
}

export async function execute(v: Variant, jobs: number): Promise<Outcome> {
  const directory = mkdtempSync(join(tmpdir(), 'walq-ablation-'))
  const db = new Database(join(directory, 'queue.sqlite'))
  db.pragma(`journal_mode = ${v.stage === 1 ? 'DELETE' : 'WAL'}`)
  db.pragma(`synchronous = ${v.synchronous.toUpperCase()}`)
  db.pragma('busy_timeout = 2000')
  const transactions: Outcome['transactions'] = []
  let measuring = false
  const connection: Connection = {
    get inTransaction() {
      return db.inTransaction
    },
    exec: (sql) => db.exec(sql),
    prepare: (sql) => db.prepare(sql).safeIntegers(false),
    transaction: (callback) => {
      const transaction = db.transaction(callback)

      return {
        immediate: (...args) => {
          const started = performance.now()
          const result = transaction.immediate(...args)
          if (measuring) {
            // Only claim transactions return nested arrays. Schedule transactions return numbers.
            const claim = Array.isArray(result) && result.every(Array.isArray)
            const claimed = claim
              ? result.reduce((total: number, batch: unknown[]) => total + batch.length, 0)
              : 0
            transactions.push({ duration: performance.now() - started, jobs: claimed, claim })
          }

          return result
        },
      }
    },
  }
  const module = (await import(
    pathToFileURL(resolve('packages/sqlite-common/dist/index.js')).href
  )) as typeof import('../packages/sqlite-common/src/index.js')
  const storage = module.createStorage(connection)
  const internal = storage as unknown as Internals
  if (typeof internal.prepareClaim !== 'function' || internal.claimTransaction === undefined) {
    throw new Error('Production internals changed; review benchmark-only unchunked path')
  }
  const names = Array.from({ length: v.stress ? 256 : 32 }, (_, i) => `queue-${i}`)
  prepareJobs(db, names, jobs, payload)
  const calls: Call[] = []
  async function timedClaim(input: ClaimInput): Promise<ClaimedJob[]> {
    const started = performance.now()
    const result = await storage.claim(input)
    calls.push({
      duration: performance.now() - started,
      jobs: result.length,
      requests: 1,
      grouped: false,
    })

    return result
  }
  const facade = Object.create(storage) as Storage
  facade.claim = async (input) => {
    if (v.stage >= 3) return timedClaim(input)

    const result: ClaimedJob[] = []
    for (let i = 0; i < input.limit; i += 1) {
      const batch = await timedClaim({ ...input, limit: 1 })
      result.push(...batch)
      if (batch.length === 0) break
    }

    return result
  }
  if (v.stage < 4) Object.defineProperty(facade, 'claimQueues', { value: undefined })
  else
    facade.claimQueues = async ({ requests }) => {
      const started = performance.now()
      let result: ClaimedJob[][]
      if (v.stage === 4) {
        internal.assertAutocommit()
        const steps = requests.map((request) => internal.prepareClaim({ ...request }))
        result = steps.length === 0 ? [] : internal.claimTransaction.immediate(steps)
      } else {
        result = await storage.claimQueues!({ requests })
      }
      calls.push({
        duration: performance.now() - started,
        jobs: result.reduce((total, batch) => total + batch.length, 0),
        requests: requests.length,
        grouped: true,
      })

      return result
    }
  let turn: Promise<void> | undefined
  function handlerTurn(): Promise<void> {
    if (v.handler === 'independent-turn')
      return new Promise((resolveTurn) => setImmediate(resolveTurn))
    if (turn === undefined)
      turn = new Promise((resolveTurn) =>
        setImmediate(() => {
          turn = undefined
          resolveTurn()
        }),
      )

    return turn
  }

  const done = deferred<void>()
  const ids = new Set<string>()
  let completed = 0
  facade.complete = async (input) => {
    const result = await storage.complete(input)
    if (result !== 'applied') throw new Error('Lost completion lease')
    completed += 1
    if (completed === jobs) done.resolve()

    return result
  }
  const queues = names.map(
    (name) =>
      new Queue(name, {
        storage: facade,
        retention: { completed: null, failed: null },
        onError: (error) => done.reject(error),
      }),
  )
  const workers: Array<{ close: () => Promise<void> }> = []
  const competitor = v.stress ? startCompetitor(db.name, v.synchronous, 16384, true) : undefined
  const eventLoop: number[] = []
  const deadline = guard(120_000, 'ablation deadline exceeded')
  let competitorReport: ClaimCompetitorReport = {
    enqueueSamples: [],
    completeSamples: [],
    operations: 0,
    errors: 0,
    firstError: null,
  }

  try {
    if (competitor !== undefined) await Promise.race([competitor.start(), deadline.promise])
    measuring = true
    const measured = await measureClaimRound(async () => {
      if (v.stress) {
        let claimed = 0
        while (claimed < jobs) {
          deadline.check()
          const results = await facade.claimQueues!({
            requests: names.map((queue) => ({
              queue,
              limit: v.concurrency,
              now: Date.now(),
              leaseDuration: 120_000,
            })),
          })
          for (const job of results.flat()) {
            if (ids.has(job.id)) throw new Error('Duplicate claim')
            ids.add(job.id)
            claimed += 1
          }
          if (results.every((batch) => batch.length === 0))
            throw new Error('Incomplete claim workload')
          await new Promise<void>((resolveTurn) => setImmediate(resolveTurn))
        }
      } else {
        for (const queue of queues)
          workers.push(
            queue.process(
              async (_data, context) => {
                if (ids.has(context.jobId)) throw new Error('Duplicate handler')
                ids.add(context.jobId)
                await handlerTurn()
              },
              { concurrency: v.concurrency },
            ),
          )
        await Promise.race([done.promise, deadline.promise])
      }
    }, eventLoop)
    measuring = false
    competitorReport = (await competitor?.stop()) ?? competitorReport
    if (ids.size !== jobs || (!v.stress && completed !== jobs))
      throw new Error('Incomplete workload')
    if (
      competitorReport.errors > 0 ||
      (v.stress && (competitorReport.operations < 20 || competitorReport.operations >= 16384))
    )
      throw new Error('Insufficient or failed competing writer samples')
    const terminal = db
      .prepare(`SELECT count(*) AS count FROM walq_jobs WHERE queue LIKE 'queue-%' AND status = ?`)
      .get(v.stress ? 'active' : 'completed') as { count: number }
    if (terminal.count !== jobs) throw new Error('Incorrect final storage state')

    return {
      elapsed: measured.duration,
      jobs,
      calls,
      transactions,
      eventLoop,
      competitor: competitorReport,
      driver: createRequire(import.meta.url)('better-sqlite3/package.json').version as string,
      sqlite: (db.prepare('select sqlite_version() AS version').get() as { version: string })
        .version,
      pragmas: [
        'journal_mode',
        'synchronous',
        'page_size',
        'cache_size',
        'wal_autocheckpoint',
        'busy_timeout',
      ].map((name) => ({ name, value: db.pragma(name, { simple: true }) })),
    }
  } finally {
    measuring = false
    deadline.dispose()
    await Promise.all(workers.map((worker) => worker.close()))
    await competitor?.terminate()
    db.close()
    rmSync(directory, { recursive: true, force: true })
  }
}

function summarize(v: Variant, jobs: number, collected: Collected<Outcome>): BenchmarkResult {
  const runs = collected.outcomes
  const rates = runs.map((run) => run.jobs / (run.elapsed / 1000))
  const metrics: BenchmarkResult['metrics'] = {
    'jobs/sec':
      runs.reduce((total, run) => total + run.jobs, 0) /
      (runs.reduce((total, run) => total + run.elapsed, 0) / 1000),
    'elapsed (s)': median(runs.map((run) => run.elapsed / 1000)),
    'spread (%)': spread(rates),
    'min jobs/sec': rates.length ? Math.min(...rates) : 0,
    'max jobs/sec': rates.length ? Math.max(...rates) : 0,
    'measured runs': runs.length,
    'claim calls': median(runs.map((run) => run.calls.length)),
    'grouped calls': median(runs.map((run) => run.calls.filter((call) => call.grouped).length)),
    'requests/grouped call': median(
      runs.map((run) => {
        const grouped = run.calls.filter((call) => call.grouped)
        return grouped.length
          ? grouped.reduce((sum, call) => sum + call.requests, 0) / grouped.length
          : 0
      }),
    ),
    'explicit transactions': median(runs.map((run) => run.transactions.length)),
    'claim transactions': median(
      runs.map((run) => run.transactions.filter((tx) => tx.claim).length),
    ),
    'completion autocommits': v.stress ? 0 : jobs,
    'jobs/claim transaction': median(
      runs.map((run) => jobs / run.transactions.filter((tx) => tx.claim).length),
    ),
    SQLite: runs[0]?.sqlite ?? '',
  }
  for (const [name, select] of [
    ['claim', (run: Outcome) => run.calls.map((call) => call.duration)],
    [
      'transaction',
      (run: Outcome) => run.transactions.filter((tx) => tx.claim).map((tx) => tx.duration),
    ],
    ['event-loop', (run: Outcome) => run.eventLoop],
    ['writer enqueue', (run: Outcome) => run.competitor.enqueueSamples],
    ['writer complete', (run: Outcome) => run.competitor.completeSamples],
  ] as const) {
    const summary = summarizePerRunMicros(runs.map(select))
    metrics[`${name} p50 (µs)`] = summary.count ? summary.p50 : 'N/A'
    metrics[`${name} p95 (µs)`] = summary.count ? summary.p95 : 'N/A'
    metrics[`${name} samples`] = summary.count
  }

  return {
    suite: 'ablation',
    scenario: variantName(v),
    params: {
      jobs,
      queues: v.stress ? 256 : 32,
      stage: v.stage,
      synchronous: v.synchronous,
      concurrency: v.concurrency,
      'payload bytes': Buffer.byteLength(payload),
      handler: v.stress ? 'none' : v.handler,
    },
    metrics,
    samples: runs.map((run) => ({
      jobs: run.jobs,
      elapsed: run.elapsed,
      rate: run.jobs / (run.elapsed / 1000),
    })),
    notes: collected.failures,
    ok: runs.length > 0 && collected.failures.length === 0,
    raw: runs,
  }
}

export function definitions(environment: BenchEnvironment): ScenarioDefinition[] {
  const stress = process.env.BENCH_ABLATION_WORKLOAD === 'stress'
  if (
    process.env.BENCH_ABLATION_WORKLOAD !== undefined &&
    !['process', 'stress'].includes(process.env.BENCH_ABLATION_WORKLOAD)
  )
    throw new Error('BENCH_ABLATION_WORKLOAD must be process or stress')
  const jobs = environment.jobs ?? (stress ? 131072 : 8192)

  const handler = process.env.BENCH_ABLATION_HANDLER ?? 'shared-turn'
  if (handler !== 'shared-turn' && handler !== 'independent-turn')
    throw new Error('BENCH_ABLATION_HANDLER must be shared-turn or independent-turn')

  return variants(stress, handler)
    .filter((v) => matches(variantName(v), environment.only))
    .map((v) =>
      defineScenario({
        suite: 'ablation',
        scenario: variantName(v),
        jobs,
        run: () => execute(v, jobs),
        summarize: (collected) => summarize(v, jobs, collected),
      }),
    )
}
