// Run after pnpm build: BENCH_REPEATS=5 node benchmarks/group-stress.mjs
import assert from 'node:assert/strict'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { performance } from 'node:perf_hooks'

import { betterSqlite3 } from '@walq/better-sqlite3'
import Database from 'better-sqlite3'

const repeats = Number(process.env.BENCH_REPEATS ?? 5)
const groups = Number(process.env.BENCH_GROUPS ?? 2000)
const futureGroups = 8
const request = { queue: 'bench', now: 1000, leaseDuration: 1_000_000, limit: 256 }
if (!Number.isSafeInteger(repeats) || repeats < 1 || !Number.isSafeInteger(groups) || groups < 1) {
  throw new Error('BENCH_REPEATS and BENCH_GROUPS must be positive integers')
}

function median(values) {
  const sorted = [...values].sort((a, b) => a - b)
  return sorted[Math.floor(sorted.length / 2)]
}

async function run(scenario) {
  const directory = mkdtempSync(join(tmpdir(), 'walq-group-stress-'))
  const db = new Database(join(directory, 'jobs.db'))

  try {
    db.pragma('journal_mode = WAL')
    db.pragma('synchronous = NORMAL')
    const storage = betterSqlite3(db)
    const addGroup = db.prepare(
      "INSERT INTO walq_groups (queue, id, concurrency) VALUES ('bench', ?, ?)",
    )
    const addJob = db.prepare(`
      INSERT INTO walq_jobs (id, queue, name, data, status, createdAt, availableAt,
        priority, groupId, attemptsMade, attempts)
      VALUES (?, 'bench', 'job', '{}', 'pending', 0, ?, 0, ?, 0, 2)
    `)
    const activate = db.prepare(`
      UPDATE walq_jobs SET status = 'active', attemptsMade = 1,
        leaseToken = 'preexisting', expiresAt = 2000000 WHERE id = ?
    `)

    db.transaction(() => {
      if (scenario === 'heavy') {
        for (let group = 0; group < 64; group++) {
          const id = `group-${String(group).padStart(4, '0')}`
          addGroup.run(id, 256)
          for (let job = 0; job < 64; job++) addJob.run(`${id}-${job}`, 0, id)
        }
      } else {
        // Eight eligible future-only groups force a switch from nextGroup to nextDueGroup.
        for (let group = 0; group < futureGroups; group++) {
          const id = `future-${String(group).padStart(4, '0')}`
          addGroup.run(id, 1)
          addJob.run(`${id}-future`, 2000, id)
        }

        for (let group = 0; group < groups; group++) {
          const id = `group-${String(group).padStart(6, '0')}`
          addGroup.run(id, 1)
          addJob.run(`${id}-active`, 0, id)
          addJob.run(`${id}-pending`, 0, id)
          activate.run(`${id}-active`)
        }
        addGroup.run('zz-ready', 1)
        addJob.run('ready', 0, 'zz-ready')
      }
    }).immediate()

    const durations = []
    for (let index = 0; index < (scenario === 'heavy' ? 8 : 1); index++) {
      const start = performance.now()
      const claimed = await storage.claim({ ...request, limit: scenario === 'heavy' ? 256 : 1 })
      durations.push(performance.now() - start)
      assert.equal(claimed.length, scenario === 'heavy' ? 256 : 1)

      if (scenario === 'heavy') {
        const counts = new Map()
        for (const job of claimed) {
          const groupId = job.id.slice(0, 10)
          counts.set(groupId, (counts.get(groupId) ?? 0) + 1)
        }
        assert.equal(counts.size, 64)
        assert.ok([...counts.values()].every((count) => count === 4))
      } else {
        assert.equal(claimed[0].id, 'ready')
      }
    }

    return { first: durations[0], next: scenario === 'heavy' ? median(durations.slice(1)) : null }
  } finally {
    db.close()
    rmSync(directory, { recursive: true, force: true })
  }
}

for (const scenario of ['heavy', 'blocked']) {
  const samples = []
  for (let index = -1; index < repeats; index++) {
    const sample = await run(scenario)
    if (index >= 0) samples.push(sample)
  }
  console.log(
    `${scenario}: first=${median(samples.map((s) => s.first)).toFixed(3)} ms` +
      (scenario === 'heavy'
        ? `, subsequent=${median(samples.map((s) => s.next)).toFixed(3)} ms, 256 jobs/claim`
        : `, ${futureGroups} future-only groups then ${groups} saturated due groups before ready`),
  )
  console.log('  first samples:', samples.map((s) => s.first.toFixed(3)).join(', '))
}
