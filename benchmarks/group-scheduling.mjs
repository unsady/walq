// Run after pnpm build: node benchmarks/group-scheduling.mjs
// Works with both the v10 and v12 production adapters; comparisons must run on the same host.
import assert from 'node:assert/strict'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { performance } from 'node:perf_hooks'

import { betterSqlite3 } from '@walq/better-sqlite3'
import Database from 'better-sqlite3'

const jobs = Number(process.env.BENCH_JOBS ?? 20000)
const repeats = Number(process.env.BENCH_REPEATS ?? 3)
const groupCount = 64
const futureGroups = Number(process.env.BENCH_FUTURE_GROUPS ?? 2000)
const batch = 16
const rounds = 16
const request = { queue: 'bench', now: 1000, leaseDuration: 1_000_000, limit: 1 }

for (const [label, value] of Object.entries({ jobs, repeats, futureGroups })) {
  if (!Number.isSafeInteger(value) || value < 1) throw new Error(`Invalid ${label}: ${value}`)
}
if (jobs < batch * rounds + 1) throw new Error('BENCH_JOBS must exceed 256')

function median(values) {
  const sorted = [...values].sort((a, b) => a - b)
  return sorted[Math.floor(sorted.length / 2)]
}

function timed(action) {
  const start = performance.now()
  const result = action()
  return { result, duration: performance.now() - start }
}

async function run(scenario) {
  const directory = mkdtempSync(join(tmpdir(), 'walq-groups-bench-'))
  const db = new Database(join(directory, 'jobs.db'))

  try {
    db.pragma('journal_mode = WAL')
    db.pragma('synchronous = NORMAL')
    const storage = betterSqlite3(db)
    const addGroup = db.prepare(
      "INSERT INTO walq_groups (queue, id, concurrency) VALUES ('bench', ?, 1)",
    )
    const addJob = db.prepare(`
      INSERT INTO walq_jobs (
        id, queue, name, data, status, createdAt, availableAt, priority,
        groupId, attemptsMade, attempts
      ) VALUES (?, 'bench', 'job', '{}', 'pending', 0, ?, ?, ?, 0, 2)
    `)

    db.transaction(() => {
      if (scenario === 'saturated') addGroup.run('full')
      if (scenario === 'blocked-future') addGroup.run('z-blocked')
      if (scenario === 'mixed') {
        for (let index = 0; index < groupCount; index++) addGroup.run(`group-${index}`)
      }
      if (
        scenario === 'future-groups' ||
        scenario === 'future-ready-group' ||
        scenario === 'blocked-future'
      ) {
        for (let index = 0; index < futureGroups; index++) addGroup.run(`future-${index}`)
        if (scenario === 'future-ready-group') addGroup.run('zz-ready')
      }

      const total =
        scenario === 'mixed'
          ? 512 + groupCount
          : scenario === 'future-groups' || scenario === 'future-ready-group'
            ? futureGroups + 1
            : scenario === 'blocked-future'
              ? jobs + futureGroups
              : jobs
      for (let index = 0; index < total; index++) {
        const saturated =
          (scenario === 'saturated' && index < jobs - 1) ||
          (scenario === 'blocked-future' && index < jobs)
        const mixedGroup = scenario === 'mixed' && index >= 512
        const futureGroup =
          ((scenario === 'future-groups' || scenario === 'future-ready-group') &&
            index < futureGroups) ||
          (scenario === 'blocked-future' && index >= jobs)
        const groupId = saturated
          ? scenario === 'blocked-future'
            ? 'z-blocked'
            : 'full'
          : mixedGroup
            ? `group-${index - 512}`
            : futureGroup
              ? `future-${scenario === 'blocked-future' ? index - jobs : index}`
              : scenario === 'future-ready-group'
                ? 'zz-ready'
                : null
        const priority =
          saturated || futureGroup ? 10 : mixedGroup ? 0 : scenario === 'mixed' ? 10 : 0
        addJob.run(`job-${index}`, futureGroup ? 2000 : 0, priority, groupId)
      }

      if (scenario === 'saturated' || scenario === 'blocked-future') {
        db.prepare(`
          UPDATE walq_jobs SET status = 'active', attemptsMade = 1,
            leaseToken = 'preexisting', expiresAt = 2_000_000 WHERE id = 'job-0'
        `).run()
      }
    }).immediate()

    if (scenario === 'ready') {
      const first = timed(() => storage.claim({ ...request, limit: batch }))
      const durations = []
      for (let index = 1; index < rounds; index++) {
        const call = timed(() => storage.claim({ ...request, limit: batch }))
        assert.equal((await call.result).length, batch)
        durations.push(call.duration)
      }
      assert.equal((await first.result).length, batch)
      return { first: first.duration, next: median(durations), groups: 0 }
    }

    if (scenario === 'blocked-future') {
      const first = timed(() => storage.claim(request))
      assert.deepEqual(await first.result, [])

      const empty = []
      for (let index = 0; index < 10; index++) {
        const call = timed(() => storage.claim(request))
        assert.deepEqual(await call.result, [])
        empty.push(call.duration)
      }
      return { first: first.duration, next: median(empty), groups: 0 }
    }

    if (
      scenario === 'saturated' ||
      scenario === 'future-groups' ||
      scenario === 'future-ready-group'
    ) {
      const first = timed(() => storage.claim(request))
      const expectedId = scenario === 'saturated' ? `job-${jobs - 1}` : `job-${futureGroups}`
      assert.equal((await first.result)[0]?.id, expectedId)

      const empty = []
      for (let index = 0; index < 10; index++) {
        const call = timed(() => storage.claim(request))
        assert.deepEqual(await call.result, [])
        empty.push(call.duration)
      }
      return { first: first.duration, next: median(empty), groups: 0 }
    }

    const grouped = new Set(Array.from({ length: groupCount }, (_, index) => `job-${index + 512}`))
    const durations = []
    let claimedGroups = 0
    for (let index = 0; index < groupCount; index++) {
      const call = timed(() => storage.claim(request))
      const [result] = await call.result
      assert.ok(result)
      if (grouped.has(result.id)) claimedGroups++
      durations.push(call.duration)
    }
    return { first: durations[0], next: median(durations), groups: claimedGroups }
  } finally {
    db.close()
    rmSync(directory, { recursive: true, force: true })
  }
}

const scenarios = [
  'ready',
  'saturated',
  'future-groups',
  'future-ready-group',
  'blocked-future',
  'mixed',
]
const results = []
for (const scenario of scenarios) {
  const samples = []
  for (let repeat = -1; repeat < repeats; repeat++) {
    const sample = await run(scenario)
    if (repeat >= 0) samples.push(sample)
  }
  results.push({
    scenario,
    first: median(samples.map((sample) => sample.first)),
    next: median(samples.map((sample) => sample.next)),
    groups: median(samples.map((sample) => sample.groups)),
    samples,
  })
}

console.log(`jobs=${jobs}, repeats=${repeats} + one warmup, file WAL/NORMAL; claim durations in ms`)
console.log('scenario              first    next¹    groups/64²')
for (const result of results) {
  console.log(
    `${result.scenario.padEnd(18)} ${result.first.toFixed(3).padStart(7)} ${result.next.toFixed(3).padStart(8)} ${String(result.groups).padStart(11)}`,
  )
}
console.log(
  '¹ median of 15 more batch claims, 10 empty claims, or 64 single claims; ² groups served among 64 mixed claims',
)
if (process.env.BENCH_JSON !== undefined) {
  writeFileSync(
    process.env.BENCH_JSON,
    JSON.stringify({ jobs, repeats, futureGroups, results }, null, 2),
  )
}
