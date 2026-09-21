import { performance } from 'node:perf_hooks'

import { betterSqlite3 } from '@walq/better-sqlite3'
import { Queue, type WorkerHandle } from '@walq/core'
import type { LeaseMutationResult, Storage } from '@walq/core/storage'
import Database from 'better-sqlite3'

import {
  deferred,
  distribute,
  guard,
  median,
  numeric,
  settledWithin,
  spread,
  SuiteAbortError,
  summarizePerRunMicros,
  type BenchmarkResult,
  type Collected,
  type RunSample,
} from './harness.js'
import { defineScenario, type ScenarioDefinition } from './scenario.js'

export type CoordinatorMode = 'shared' | 'isolated'
export type CoordinatorProfile = 'saturated' | 'sparse' | 'bursty'

export interface CoordinatorGrid {
  modes: CoordinatorMode[]
  queues: number[]
  profiles: CoordinatorProfile[]
}

export interface CoordinatorScenario {
  mode: CoordinatorMode
  queues: number
  profile: CoordinatorProfile
}

export const quickCoordinatorGrid: CoordinatorGrid = {
  modes: ['shared', 'isolated'],
  queues: [1, 8],
  profiles: ['saturated', 'sparse', 'bursty'],
}

export const fullCoordinatorGrid: CoordinatorGrid = {
  modes: ['shared', 'isolated'],
  queues: [1, 4, 16, 64],
  profiles: ['saturated', 'sparse', 'bursty'],
}

const runTimeout = 60_000
const closeGrace = 1_000

/** Storage traffic of one run, observed around the adapter. */
interface Tracker {
  claims: number
  emptyClaims: number
  confirmed: number
  lostLeases: number
  duplicates: number
  groupedCalls: number
  groupedRequests: number
  claimSamples: number[]
  completeSamples: number[]
  completedIds: Set<string>
  onIdle: () => void
  onOperation: () => void
  onSettled: (failure?: unknown) => void
}

export interface CoordinatorRunOutcome {
  elapsed: number
  firstHandler: number | undefined
  claims: number
  emptyClaims: number
  confirmed: number
  lostLeases: number
  duplicates: number
  groupedCalls: number
  groupedRequests: number
  claimSamples: number[]
  completeSamples: number[]
}

export function scenarioName(scenario: CoordinatorScenario): string {
  return `${scenario.mode} / ${scenario.queues} queues / ${scenario.profile}`
}

/** Number of queues that actually receive jobs under one profile. */
function activeQueues(queues: number, profile: CoordinatorProfile): number {
  return profile === 'sparse' ? Math.max(1, Math.ceil(queues / 4)) : queues
}

/**
 * Signature of the work a scenario performs. Variants that run identically must not be
 * measured twice: one queue shares a single coordinator either way, and `sparse` cannot thin a
 * one-queue workload. `bursty` keeps its profile because its enqueue strategy, not the queue
 * count, is what distinguishes it.
 */
function workloadKey(scenario: CoordinatorScenario): string {
  const mode = scenario.queues === 1 ? 'single-queue' : scenario.mode
  const profile =
    scenario.profile === 'bursty'
      ? scenario.profile
      : activeQueues(scenario.queues, scenario.profile) === scenario.queues
        ? 'preloaded'
        : scenario.profile

  return `${mode}|${scenario.queues}|${profile}`
}

/**
 * Expand the grid into distinct workloads, deduplicating the degenerate single-queue variants.
 * Shared and isolated variants of one configuration stay adjacent.
 */
export function coordinatorScenarios(grid: CoordinatorGrid): CoordinatorScenario[] {
  const scenarios: CoordinatorScenario[] = []
  const seen = new Set<string>()
  for (const queues of grid.queues) {
    for (const profile of grid.profiles) {
      for (const mode of grid.modes) {
        const candidate = { mode, queues, profile }
        const key = workloadKey(candidate)
        if (seen.has(key)) continue
        seen.add(key)
        scenarios.push(candidate)
      }
    }
  }

  return scenarios
}

/** Count storage traffic and let the caller observe an idle poller. */
function instrument(inner: Storage, tracker: Tracker): Storage {
  const wrapped: Storage = {
    async enqueue(input) {
      tracker.onOperation()

      return inner.enqueue(input)
    },
    async claim(input) {
      tracker.onOperation()
      const started = performance.now()
      const jobs = await inner.claim(input)
      tracker.claimSamples.push(performance.now() - started)
      tracker.claims += 1
      if (jobs.length === 0) {
        tracker.emptyClaims += 1
        tracker.onIdle()
      }
      return jobs
    },
    async complete(input) {
      tracker.onOperation()
      const started = performance.now()
      let result: LeaseMutationResult
      try {
        result = await inner.complete(input)
      } catch (error) {
        tracker.completeSamples.push(performance.now() - started)
        tracker.onSettled(error)
        throw error
      }

      tracker.completeSamples.push(performance.now() - started)
      if (result === 'applied') {
        tracker.confirmed += 1
        if (tracker.completedIds.has(input.id)) tracker.duplicates += 1
        tracker.completedIds.add(input.id)
      } else {
        tracker.lostLeases += 1
      }
      tracker.onSettled()

      return result
    },
    async fail(input) {
      tracker.onOperation()
      const result = await inner.fail(input)
      tracker.onSettled(new Error(`job ${input.id} was failed`))

      return result
    },
    async heartbeat(input) {
      tracker.onOperation()

      return inner.heartbeat(input)
    },
    async cleanup(input) {
      tracker.onOperation()

      return inner.cleanup(input)
    },
  }

  // Forward the optional grouped capability; without this the wrapper would
  // silently downgrade the coordinator to per-queue claim() calls.
  const claimQueues = inner.claimQueues
  if (claimQueues !== undefined) {
    wrapped.claimQueues = async (input) => {
      tracker.onOperation()
      const started = performance.now()
      const results = await claimQueues.call(inner, input)
      tracker.claimSamples.push(performance.now() - started)
      tracker.claims += input.requests.length
      tracker.groupedCalls += 1
      tracker.groupedRequests += input.requests.length
      let empty = 0
      for (const jobs of results) if (jobs.length === 0) empty += 1
      tracker.emptyClaims += empty
      if (empty === input.requests.length && input.requests.length > 0) tracker.onIdle()
      return results
    }
  }

  return wrapped
}

async function enqueueJobs(storage: Storage, queue: string, count: number): Promise<void> {
  const now = Date.now()
  for (let index = 0; index < count; index += 1) {
    await storage.enqueue({ queue, name: 'bench', data: '{}', now, availableAt: now, attempts: 1 })
  }
}

async function executeRun(
  scenario: CoordinatorScenario,
  jobs: number,
): Promise<CoordinatorRunOutcome> {
  const db = new Database(':memory:')
  const timeout = guard(runTimeout, 'run timed out')
  const workers: WorkerHandle[] = []
  try {
    const tracker: Tracker = {
      claims: 0,
      emptyClaims: 0,
      confirmed: 0,
      lostLeases: 0,
      duplicates: 0,
      groupedCalls: 0,
      groupedRequests: 0,
      claimSamples: [],
      completeSamples: [],
      completedIds: new Set(),
      onIdle: () => {},
      onOperation: () => {},
      onSettled: () => {},
    }
    const base = betterSqlite3(db)
    // Distinct wrapper objects always produce distinct coordinators.
    const shared = scenario.mode === 'shared' ? instrument(base, tracker) : undefined
    const counts = distribute(jobs, activeQueues(scenario.queues, scenario.profile))
    const entries = Array.from({ length: scenario.queues }, (_, index) => {
      const name = `bench-${index}`
      const storage = shared ?? instrument(base, tracker)

      return { name, storage, count: counts[index] ?? 0, queue: new Queue(name, { storage }) }
    })

    // The run ends when storage confirms the last job, not when its handler returns.
    const drained = deferred<void>()
    const idle = deferred<void>()
    const budgetFrom = performance.now()
    let handled = 0
    let settled = 0
    let firstHandler: number | undefined
    tracker.onIdle = () => idle.resolve()
    tracker.onOperation = () => {
      if (performance.now() - budgetFrom <= runTimeout) return

      // The run may never wait on a timer, so the timer-based guard cannot fire on its own.
      drained.reject(new Error('run timed out'))
      // Throwing stops the caller too, so a long enqueue phase cannot outlive its budget.
      throw new Error('run timed out')
    }
    tracker.onSettled = (failure) => {
      settled += 1
      if (failure !== undefined) drained.reject(failure)
      else if (tracker.confirmed === jobs) drained.resolve()
      else if (settled === jobs && handled === jobs) {
        drained.reject(new Error(`confirmed ${tracker.confirmed} of ${jobs} jobs`))
      }
    }
    async function processor(): Promise<void> {
      firstHandler ??= performance.now()
      handled += 1
    }

    let startedAt = 0
    async function closeWorkers(): Promise<void> {
      try {
        const stopped = await settledWithin(
          Promise.all(workers.map((worker) => worker.close())),
          closeGrace,
        )
        if (!stopped) throw new SuiteAbortError(`workers did not stop within ${closeGrace}ms`)
      } catch (error) {
        if (error instanceof SuiteAbortError) throw error
        throw new SuiteAbortError('workers failed to stop', { cause: error })
      }
    }
    try {
      if (scenario.profile === 'bursty') {
        workers.push(...entries.map((entry) => entry.queue.process(processor)))
        // Wait until the poller has nothing to do, then wake it with a single burst of adds.
        await Promise.race([idle.promise, timeout.promise])
        startedAt = performance.now()
        await Promise.race([
          Promise.all(
            entries.flatMap((entry) =>
              Array.from({ length: entry.count }, () => entry.queue.add({})),
            ),
          ),
          timeout.promise,
        ])
      } else {
        await Promise.race([
          Promise.all(entries.map((entry) => enqueueJobs(entry.storage, entry.name, entry.count))),
          timeout.promise,
        ])
        startedAt = performance.now()
        workers.push(...entries.map((entry) => entry.queue.process(processor)))
      }

      // The run ends when storage confirms the last job, not when its handler returns.
      await Promise.race([drained.promise, timeout.promise])
      const elapsed = performance.now() - startedAt
      return {
        elapsed,
        firstHandler: firstHandler === undefined ? undefined : firstHandler - startedAt,
        claims: tracker.claims,
        emptyClaims: tracker.emptyClaims,
        confirmed: tracker.confirmed,
        lostLeases: tracker.lostLeases,
        duplicates: tracker.duplicates,
        groupedCalls: tracker.groupedCalls,
        groupedRequests: tracker.groupedRequests,
        claimSamples: tracker.claimSamples,
        completeSamples: tracker.completeSamples,
      }
    } finally {
      // Workers must stop even when the run failed, or the poller keeps the process alive.
      await closeWorkers()
    }
  } finally {
    timeout.dispose()
    db.close()
  }
}

function sampleOf(outcome: CoordinatorRunOutcome, jobs: number): RunSample {
  const sample: RunSample = {
    'jobs/sec': (jobs / outcome.elapsed) * 1000,
    'elapsed (ms)': outcome.elapsed,
    claims: outcome.claims,
    'empty claims': outcome.emptyClaims,
    'grouped calls': outcome.groupedCalls,
  }
  if (outcome.firstHandler !== undefined) sample['first handler (ms)'] = outcome.firstHandler

  return sample
}

/** Reason why a run must stay out of the metrics, or undefined when it is trustworthy. */
export function invalidReason(outcome: CoordinatorRunOutcome, jobs: number): string | undefined {
  if (outcome.confirmed !== jobs) return `confirmed ${outcome.confirmed} of ${jobs} jobs`
  if (outcome.lostLeases > 0) return `${outcome.lostLeases} leases lost`
  if (outcome.duplicates > 0) return `${outcome.duplicates} duplicate completions`

  return undefined
}

export function summarizeRuns(
  scenario: CoordinatorScenario,
  jobs: number,
  collected: Collected<CoordinatorRunOutcome>,
): BenchmarkResult {
  const valid: CoordinatorRunOutcome[] = []
  const invalid: string[] = []
  for (const outcome of collected.outcomes) {
    const reason = invalidReason(outcome, jobs)
    if (reason === undefined) valid.push(outcome)
    else invalid.push(reason)
  }

  const runs = valid.length
  const rates = valid.map((outcome) => (jobs / outcome.elapsed) * 1000)
  const claim = summarizePerRunMicros(valid.map((outcome) => outcome.claimSamples))
  const complete = summarizePerRunMicros(valid.map((outcome) => outcome.completeSamples))
  const firstHandlers = valid.flatMap((outcome) =>
    outcome.firstHandler === undefined ? [] : [outcome.firstHandler],
  )
  function sum(pick: (outcome: CoordinatorRunOutcome) => number): number {
    return valid.reduce((total, outcome) => total + pick(outcome), 0)
  }
  const metrics: Record<string, number> = {
    'jobs/sec': median(rates),
    'spread (%)': spread(rates),
    'claims/job': runs === 0 ? 0 : sum((outcome) => outcome.claims) / (jobs * runs),
    'empty claims': runs === 0 ? 0 : sum((outcome) => outcome.emptyClaims) / runs,
    'claim p50 (µs)': claim.p50,
    'claim p95 (µs)': claim.p95,
    'complete p95 (µs)': complete.p95,
    'elapsed (ms)': median(valid.map((outcome) => outcome.elapsed)),
  }
  const groupedCalls = sum((outcome) => outcome.groupedCalls)
  if (groupedCalls > 0) {
    const groupedRequests = sum((outcome) => outcome.groupedRequests)
    metrics['grouped calls'] = runs === 0 ? 0 : groupedCalls / runs
    metrics['requests/grouped call'] = groupedRequests / groupedCalls
  }
  if (firstHandlers.length > 0) metrics['first handler (ms)'] = median(firstHandlers)

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
    suite: 'coordinator',
    scenario: scenarioName(scenario),
    params: {
      mode: scenario.mode,
      queues: scenario.queues,
      profile: scenario.profile,
      jobs,
    },
    metrics,
    samples: valid.map((outcome) => sampleOf(outcome, jobs)),
    notes,
    ok: notes.length === 0,
  }
}

/** Adapter used by the Vitest benchmark files. */
export function defineCoordinatorScenario(
  scenario: CoordinatorScenario,
  jobs: number,
): ScenarioDefinition {
  return defineScenario({
    suite: 'coordinator',
    scenario: scenarioName(scenario),
    jobs,
    run: () => executeRun(scenario, jobs),
    summarize: (collected) => summarizeRuns(scenario, jobs, collected),
    throughput: (result) => numeric(result.metrics['jobs/sec']),
    latency: (result) => result.samples.map((sample) => numeric(sample['elapsed (ms)'])),
  })
}
