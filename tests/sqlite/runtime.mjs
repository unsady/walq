import assert from 'node:assert/strict'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'

import { Queue } from '../../packages/core/dist/index.js'
import { sqlite } from '../../packages/sqlite/dist/index.js'

// Exercise the built package, not a runtime-specific implementation or shim.
const directory = mkdtempSync(join(tmpdir(), 'walq-runtime-'))
const path = join(directory, 'queue.sqlite')
const input = {
  queue: 'email',
  name: 'send',
  data: '{"value":"ok"}',
  now: 10,
  availableAt: 10,
  priority: 0,
  attempts: 2,
}
const claim = { queue: input.queue, now: 10, limit: 10, leaseDuration: 20 }
let db
let other

try {
  db = new DatabaseSync(path)
  db.exec('PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL; PRAGMA busy_timeout = 5000')
  let storage = sqlite(db)

  const jobs = await storage.enqueueMany([
    { ...input, dedupe: 'same', group: { id: 'shared', concurrency: 1 } },
    { ...input, dedupe: 'same', group: { id: 'shared', concurrency: 1 } },
    { ...input, group: { id: 'shared', concurrency: 1 } },
  ])
  assert.equal(jobs[0].id, jobs[1].id)
  assert.equal((await storage.count({ queue: input.queue })).pending, 2)

  await assert.rejects(
    storage.enqueueMany([
      { ...input, group: { id: 'rollback', concurrency: 1 } },
      { ...input, group: { id: 'rollback', concurrency: 2 } },
    ]),
    /already uses concurrency/,
  )
  assert.equal(db.isTransaction, false)
  assert.equal((await storage.count({ queue: input.queue })).pending, 2)

  db.exec('BEGIN')
  await assert.rejects(storage.enqueue(input), /inside a transaction/)
  assert.throws(() => sqlite(db), /inside a transaction/)
  assert.equal(db.isTransaction, true)
  db.exec('ROLLBACK')

  await storage.pause({ queue: input.queue })
  db.close()
  db = new DatabaseSync(path)
  storage = sqlite(db)
  assert.deepEqual(await storage.claim(claim), [])
  await storage.resume({ queue: input.queue })

  other = new DatabaseSync(path)
  other.exec('PRAGMA busy_timeout = 5000')
  const second = sqlite(other)
  const [job] = await storage.claim(claim)
  assert.equal(job.id, jobs[0].id)
  assert.deepEqual(await second.claim(claim), [])
  assert.equal(await second.complete({ ...job, leaseToken: 'stale', now: 11 }), 'lease_lost')
  assert.equal(await second.heartbeat({ ...job, now: 11, leaseDuration: 30 }), 'applied')
  assert.equal(await storage.complete({ ...job, now: 12 }), 'applied')
  assert.equal((await second.inspect({ queue: input.queue, id: job.id })).status, 'completed')
  assert.equal((await second.claim(claim)).length, 1)

  await storage.upsertSchedule({ queue: 'scheduled', id: 'repeat', data: '{}', every: 10, now: 0 })
  assert.equal(await second.materializeSchedules({ queue: 'scheduled', now: 10, attempts: 2 }), 1)
  assert.equal(await storage.materializeSchedules({ queue: 'scheduled', now: 10, attempts: 2 }), 0)

  // More than one chunk: also exercises node:timers/promises yielding.
  const requests = Array.from({ length: 3 }, (_, index) => ({
    ...claim,
    queue: `chunk-${index}`,
    limit: 512,
  }))
  await storage.enqueueMany(requests.map((request) => ({ ...input, queue: request.queue })))
  assert.deepEqual(
    (await storage.claimQueues({ requests })).map((batch) => batch.length),
    [1, 1, 1],
  )

  assert.deepEqual(
    await storage.cleanup({
      queue: input.queue,
      now: 20,
      limit: 10,
      retention: { completed: { count: 0, maxAge: null }, failed: { count: null, maxAge: null } },
    }),
    { removed: 1, more: false },
  )

  const queue = new Queue('runtime-worker', { storage })
  let resolveHandled
  let timeout
  const handled = new Promise((resolve, reject) => {
    resolveHandled = resolve
    timeout = setTimeout(() => reject(new Error('Worker timed out')), 5000)
  })
  const worker = queue.process((data) => resolveHandled(data.value))

  try {
    await queue.add({ value: 'ok' })
    assert.equal(await handled, 'ok')
  } finally {
    clearTimeout(timeout)
    await worker.close()
  }

  console.log('node:sqlite runtime checks passed')
} finally {
  if (other?.isOpen) other.close()
  if (db?.isOpen) db.close()
  rmSync(directory, { recursive: true, force: true })
}
