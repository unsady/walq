// Run after pnpm build: node benchmarks/claim-write-cost.mjs
// Compare v10, v12 with the former active-group index, and current v12 using identical SQL writes.
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { performance } from 'node:perf_hooks'

import { betterSqlite3 } from '@walq/better-sqlite3'
import Database from 'better-sqlite3'

const jobs = Number(process.env.BENCH_JOBS ?? 20000)
const singles = Number(process.env.BENCH_SINGLES ?? 1000)
const cycles = Number(process.env.BENCH_CYCLES ?? 2000)
const repeats = Number(process.env.BENCH_REPEATS ?? 3)
const payload = JSON.stringify({ payload: 'a'.repeat(100) })
const scenarios = [0, 1, 64]
const modes = ['old', 'with-active-group', 'new']
const settings = ['NORMAL', 'FULL']

for (const [name, value] of Object.entries({ jobs, singles, cycles, repeats })) {
  if (!Number.isSafeInteger(value) || value < 1) throw new Error(`Invalid ${name}: ${value}`)
}
if (jobs < cycles * 2) throw new Error('BENCH_JOBS must be at least twice BENCH_CYCLES')

const ids = Array.from({ length: jobs + singles }, () => randomUUID())

function median(values) {
  const sorted = [...values].sort((a, b) => a - b)
  return sorted[Math.floor(sorted.length / 2)]
}

function timed(action) {
  const start = performance.now()
  action()
  return performance.now() - start
}

function revertSchema(db) {
  db.exec(`
    DROP TRIGGER walq_group_counts_insert;
    DROP TRIGGER walq_group_counts_delete;
    DROP TRIGGER walq_group_counts_update;
    DROP INDEX walq_pending;
    DROP INDEX walq_pending_grouped;
    DROP INDEX walq_groups_eligible;
    DROP TABLE walq_group_cursor;
    ALTER TABLE walq_groups DROP COLUMN pendingCount;
    ALTER TABLE walq_groups DROP COLUMN activeCount;
    CREATE INDEX walq_pending ON walq_jobs (queue, priority DESC, availableAt, seq)
      WHERE status = 'pending';
    CREATE INDEX walq_active_group ON walq_jobs (queue, groupId)
      WHERE status = 'active' AND groupId IS NOT NULL;
    UPDATE walq_schema SET version = 10;
  `)
}

function run(mode, groupCount, synchronous) {
  const dir = mkdtempSync(join(tmpdir(), 'walq-write-cost-'))
  const db = new Database(join(dir, 'queue.db'))

  try {
    db.pragma('journal_mode = WAL')
    db.pragma(`synchronous = ${synchronous}`)
    betterSqlite3(db)
    if (mode === 'old') revertSchema(db)
    if (mode === 'with-active-group') {
      db.exec(`CREATE INDEX walq_active_group ON walq_jobs (queue, groupId)
        WHERE status = 'active' AND groupId IS NOT NULL`)
    }

    const groupIds = Array.from({ length: groupCount }, (_, index) => `group-${index}`)
    const insertGroup = db.prepare(
      "INSERT INTO walq_groups (queue, id, concurrency) VALUES ('bench', ?, 1)",
    )
    for (const groupId of groupIds) insertGroup.run(groupId)

    const insert = db.prepare(`
      INSERT INTO walq_jobs (
        id, queue, name, data, status, createdAt, availableAt, priority,
        groupId, attemptsMade, attempts
      ) VALUES (?, 'bench', 'job', ?, 'pending', 0, 0, 0, ?, 0, 2)
    `)
    function insertAt(index) {
      insert.run(ids[index], payload, groupCount === 0 ? null : groupIds[index % groupCount])
    }
    const activate = db.prepare(`
      UPDATE walq_jobs SET status = 'active', attemptsMade = attemptsMade + 1,
        leaseToken = 'token', expiresAt = 1000 WHERE id = ? AND status = 'pending'
    `)
    const complete = db.prepare(`
      UPDATE walq_jobs SET status = 'completed', finishedAt = 0,
        leaseToken = NULL, expiresAt = NULL WHERE id = ? AND status = 'active'
    `)
    const reschedule = db.prepare(`
      UPDATE walq_jobs SET availableAt = 100 WHERE id = ? AND status = 'pending'
    `)

    const bulk = timed(() => {
      db.transaction(() => {
        for (let index = 0; index < jobs; index++) insertAt(index)
      }).immediate()
    })
    const individual = timed(() => {
      for (let index = jobs; index < jobs + singles; index++) insertAt(index)
    })
    const transitions = timed(() => {
      for (let index = 0; index < cycles; index++) {
        assert.equal(activate.run(ids[index]).changes, 1)
        assert.equal(complete.run(ids[index]).changes, 1)
      }
    })
    const updates = timed(() => {
      for (let index = cycles; index < cycles * 2; index++) {
        assert.equal(reschedule.run(ids[index]).changes, 1)
      }
    })

    if (mode !== 'old' && groupCount > 0) {
      const counts = db
        .prepare('SELECT sum(pendingCount) AS pending, sum(activeCount) AS active FROM walq_groups')
        .get()
      assert.deepEqual(counts, { pending: jobs + singles - cycles, active: 0 })
    }

    return { bulk, individual, transitions, updates }
  } finally {
    db.close()
    rmSync(dir, { recursive: true, force: true })
  }
}

console.log(
  `jobs=${jobs}, individual inserts=${singles}, status cycles=${cycles}, reschedules=${cycles}; median of ${repeats} measured runs, one warmup; ms`,
)
console.log('sync    groups  mode                   bulk    insert  activate+complete  reschedule')
for (const synchronous of settings) {
  for (const groupCount of scenarios) {
    const results = new Map(modes.map((mode) => [mode, []]))
    for (let repeat = -1; repeat < repeats; repeat++) {
      for (const mode of repeat % 2 === 0 ? modes : [...modes].reverse()) {
        const result = run(mode, groupCount, synchronous)
        if (repeat >= 0) results.get(mode).push(result)
      }
    }

    for (const mode of modes) {
      const samples = results.get(mode)
      const cells = ['bulk', 'individual', 'transitions', 'updates'].map((key) =>
        median(samples.map((result) => result[key]))
          .toFixed(1)
          .padStart(key === 'transitions' ? 18 : 9),
      )
      console.log(
        `${synchronous.padEnd(6)}  ${String(groupCount).padStart(6)}  ${mode.padEnd(21)} ${cells.join(' ')}`,
      )
    }
  }
}
