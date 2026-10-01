import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { performance } from 'node:perf_hooks'

import { betterSqlite3 } from '@walq/better-sqlite3'
import type { ClaimedJob } from '@walq/core/storage'
import Database from 'better-sqlite3'

import { synchronousPragma, type SynchronousMode } from './bench-options.js'
import { median, numeric, spread, type BenchmarkResult, type Collected } from './harness.js'
import { defineScenario, type ScenarioDefinition } from './scenario.js'

export type GroupScenarioName =
  | 'ready'
  | 'saturated'
  | 'future-groups'
  | 'future-ready-group'
  | 'blocked-future'
  | 'mixed'
  | 'heavy-fairness'
  | 'multiple-saturated-due-groups'

export interface GroupScenario {
  name: GroupScenarioName
  jobs: number
  groups: number
  futureGroups: number
}

export interface GroupRunOutcome {
  first: number
  next: number
  claims: number
  claimed: number
  duration: number
  servedGroups: number
}

const schedulingNames: GroupScenarioName[] = [
  'ready',
  'saturated',
  'future-groups',
  'future-ready-group',
  'blocked-future',
  'mixed',
]

export function groupScenarios(
  jobs: number,
  groups: number,
  futureGroups: number,
): GroupScenario[] {
  return [
    ...schedulingNames.map((name) => ({ name, jobs, groups, futureGroups })),
    { name: 'heavy-fairness', jobs, groups, futureGroups: 8 },
    { name: 'multiple-saturated-due-groups', jobs, groups, futureGroups: 8 },
  ]
}

export function positiveSetting(
  value: string | undefined,
  fallback: number,
  label: string,
): number {
  if (value === undefined || value === '') return fallback
  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed) || parsed < 1) {
    throw new Error(`${label} must be a positive safe integer, received "${value}"`)
  }
  return parsed
}

export async function runGroupScenario(
  scenario: GroupScenario,
  synchronous: SynchronousMode = 'normal',
): Promise<GroupRunOutcome> {
  const directory = mkdtempSync(join(tmpdir(), 'walq-groups-bench-'))
  const db = new Database(join(directory, 'jobs.db'))

  try {
    db.pragma('journal_mode = WAL')
    db.pragma(synchronousPragma(synchronous))
    const storage = betterSqlite3(db)
    const addGroup = db.prepare(
      "INSERT INTO walq_groups (queue, id, concurrency) VALUES ('bench', ?, ?)",
    )
    const addJob = db.prepare(`
      INSERT INTO walq_jobs (
        id, queue, name, data, status, createdAt, availableAt, priority,
        groupId, attemptsMade, attempts
      ) VALUES (?, 'bench', 'job', '{}', 'pending', 0, ?, ?, ?, 0, 2)
    `)

    db.transaction(() => {
      if (scenario.name === 'saturated') addGroup.run('full', 1)
      if (scenario.name === 'blocked-future') addGroup.run('z-blocked', 1)

      if (scenario.name === 'mixed') {
        for (let index = 0; index < 64; index += 1) addGroup.run(`group-${index}`, 1)
      }

      if (
        scenario.name === 'future-groups' ||
        scenario.name === 'future-ready-group' ||
        scenario.name === 'blocked-future'
      ) {
        for (let index = 0; index < scenario.futureGroups; index += 1) {
          addGroup.run(`future-${index}`, 1)
        }
        if (scenario.name === 'future-ready-group') addGroup.run('zz-ready', 1)
      }

      if (scenario.name === 'heavy-fairness') {
        for (let group = 0; group < 64; group += 1) {
          const id = `group-${String(group).padStart(4, '0')}`
          addGroup.run(id, 256)
          for (let job = 0; job < 64; job += 1) addJob.run(`${id}-${job}`, 0, 0, id)
        }
      } else if (scenario.name === 'multiple-saturated-due-groups') {
        for (let index = 0; index < scenario.groups; index += 1) {
          const id = `group-${String(index).padStart(6, '0')}`
          addGroup.run(id, 1)
          addJob.run(`${id}-active`, 0, 0, id)
          addJob.run(`${id}-pending`, 0, 0, id)
        }
        for (let index = 0; index < scenario.futureGroups; index += 1) {
          const id = `future-${String(index).padStart(4, '0')}`
          addGroup.run(id, 1)
          addJob.run(`${id}-future`, 2000, 0, id)
        }
        addGroup.run('zz-ready', 1)
        addJob.run('ready', 0, 0, 'zz-ready')
      } else {
        const total =
          scenario.name === 'mixed'
            ? 512 + 64
            : scenario.name === 'future-groups' || scenario.name === 'future-ready-group'
              ? scenario.futureGroups + 1
              : scenario.name === 'blocked-future'
                ? scenario.jobs + scenario.futureGroups
                : scenario.jobs
        for (let index = 0; index < total; index += 1) {
          const saturated =
            (scenario.name === 'saturated' && index < scenario.jobs - 1) ||
            (scenario.name === 'blocked-future' && index < scenario.jobs)
          const mixedGroup = scenario.name === 'mixed' && index >= 512
          const futureGroup =
            ((scenario.name === 'future-groups' || scenario.name === 'future-ready-group') &&
              index < scenario.futureGroups) ||
            (scenario.name === 'blocked-future' && index >= scenario.jobs)
          const groupId = saturated
            ? scenario.name === 'blocked-future'
              ? 'z-blocked'
              : 'full'
            : mixedGroup
              ? `group-${index - 512}`
              : futureGroup
                ? `future-${scenario.name === 'blocked-future' ? index - scenario.jobs : index}`
                : scenario.name === 'future-ready-group'
                  ? 'zz-ready'
                  : null
          const priority =
            saturated || futureGroup ? 10 : mixedGroup ? 0 : scenario.name === 'mixed' ? 10 : 0
          addJob.run(`job-${index}`, futureGroup ? 2000 : 0, priority, groupId)
        }
      }

      if (scenario.name === 'saturated' || scenario.name === 'blocked-future') {
        db.prepare(`
          UPDATE walq_jobs SET status = 'active', attemptsMade = 1,
            leaseToken = 'preexisting', expiresAt = 2000000 WHERE id = 'job-0'
        `).run()
      }

      if (scenario.name === 'multiple-saturated-due-groups') {
        const activate = db.prepare(`
          UPDATE walq_jobs SET status = 'active', attemptsMade = 1,
            leaseToken = 'preexisting', expiresAt = 2000000 WHERE id = ?
        `)
        for (let index = 0; index < scenario.groups; index += 1) {
          activate.run(`group-${String(index).padStart(6, '0')}-active`)
        }
      }
    }).immediate()

    const request = { queue: 'bench', now: 1000, leaseDuration: 1_000_000, limit: 1 }
    const durations: number[] = []
    let claimed = 0
    let servedGroups = 0
    const calls =
      scenario.name === 'heavy-fairness'
        ? 8
        : scenario.name === 'mixed'
          ? 64
          : scenario.name === 'multiple-saturated-due-groups'
            ? 1
            : scenario.name === 'ready'
              ? 16
              : 11

    for (let index = 0; index < calls; index += 1) {
      const started = performance.now()
      const claimLimit =
        scenario.name === 'heavy-fairness' ? 256 : scenario.name === 'ready' ? 16 : 1
      const jobs: ClaimedJob[] = await storage.claim({ ...request, limit: claimLimit })
      durations.push(performance.now() - started)
      claimed += jobs.length

      if (scenario.name === 'heavy-fairness') {
        if (jobs.length !== 256)
          throw new Error(`heavy fairness claimed ${jobs.length}, expected 256`)
        const counts = new Map<string, number>()
        for (const job of jobs) {
          const id = job.id.slice(0, 10)
          counts.set(id, (counts.get(id) ?? 0) + 1)
        }
        if (counts.size !== 64 || [...counts.values()].some((count) => count !== 4)) {
          throw new Error('heavy fairness did not claim four jobs from each of 64 groups')
        }
      } else if (scenario.name === 'ready') {
        if (jobs.length !== 16) throw new Error(`ready claim returned ${jobs.length}, expected 16`)
      } else if (scenario.name === 'mixed') {
        if (jobs.length !== 1) throw new Error(`mixed claim returned ${jobs.length} jobs`)
        const id = Number(jobs[0]?.id.slice('job-'.length))
        if (id >= 512 && id < 576) servedGroups += 1
      } else if (
        scenario.name === 'blocked-future' ||
        scenario.name === 'saturated' ||
        scenario.name === 'future-groups' ||
        scenario.name === 'future-ready-group'
      ) {
        const expected =
          scenario.name === 'saturated'
            ? `job-${scenario.jobs - 1}`
            : scenario.name === 'future-groups' || scenario.name === 'future-ready-group'
              ? `job-${scenario.futureGroups}`
              : undefined
        if (index === 0 && expected !== undefined && jobs[0]?.id !== expected) {
          throw new Error(
            `${scenario.name} claimed ${jobs[0]?.id ?? 'nothing'}, expected ${expected}`,
          )
        }
        if (
          index === 0 &&
          (scenario.name === 'blocked-future' || expected === undefined) &&
          jobs.length !== 0
        ) {
          throw new Error(`${scenario.name} expected an empty claim`)
        }
        if (index > 0 && jobs.length !== 0)
          throw new Error(`${scenario.name} expected empty follow-up claims`)
      } else if (scenario.name === 'multiple-saturated-due-groups') {
        if (index === 0 && jobs[0]?.id !== 'ready') {
          throw new Error(
            `multiple saturated groups claimed ${jobs[0]?.id ?? 'nothing'}, expected ready`,
          )
        }
        if (index > 0 && jobs.length !== 0) throw new Error('expected empty follow-up claims')
      }
    }

    const next = scenario.name === 'mixed' ? median(durations) : median(durations.slice(1))
    return {
      first: durations[0] ?? 0,
      next,
      claims: calls,
      claimed,
      duration: durations.reduce((total, value) => total + value, 0),
      servedGroups,
    }
  } finally {
    db.close()
    rmSync(directory, { recursive: true, force: true })
  }
}

export function summarizeGroupRuns(
  scenario: GroupScenario,
  collected: Collected<GroupRunOutcome>,
): BenchmarkResult {
  const outcomes = collected.outcomes
  const first = outcomes.map((outcome) => outcome.first)
  const next = outcomes.map((outcome) => outcome.next)
  const rates = outcomes.map((outcome) => (outcome.claims / outcome.duration) * 1000)
  const notes = [...collected.failures]
  return {
    suite: 'groups',
    scenario: scenario.name,
    params: { jobs: scenario.jobs, groups: scenario.groups, futureGroups: scenario.futureGroups },
    metrics: {
      'claims/sec': median(rates),
      'spread (%)': spread(rates),
      'first claim (µs)': median(first) * 1000,
      'follow-up claim (µs)': median(next) * 1000,
      'claimed jobs': median(outcomes.map((outcome) => outcome.claimed)),
      'served groups': median(outcomes.map((outcome) => outcome.servedGroups)),
    },
    samples: outcomes.map((outcome) => ({
      'claims/sec': (outcome.claims / outcome.duration) * 1000,
      'claim duration (ms)': outcome.duration,
      'first claim (ms)': outcome.first,
      'follow-up claim (ms)': outcome.next,
      claimed: outcome.claimed,
      'served groups': outcome.servedGroups,
    })),
    notes,
    ok: notes.length === 0,
  }
}

export function defineGroupScenario(
  scenario: GroupScenario,
  synchronous: SynchronousMode,
): ScenarioDefinition {
  return defineScenario({
    suite: 'groups',
    scenario: scenario.name,
    jobs: scenario.jobs,
    run: () => runGroupScenario(scenario, synchronous),
    summarize: (collected) => summarizeGroupRuns(scenario, collected),
    throughput: (result) => numeric(result.metrics['claims/sec']),
    latency: (result) => result.samples.map((sample) => numeric(sample['first claim (ms)'])),
  })
}
