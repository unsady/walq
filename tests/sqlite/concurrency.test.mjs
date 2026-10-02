import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createInterface } from 'node:readline'
import { DatabaseSync } from 'node:sqlite'
import { it } from 'node:test'
import { fileURLToPath } from 'node:url'

import { sqlite } from '../../packages/sqlite/dist/index.js'

const input = {
  queue: 'email',
  name: 'send',
  data: '{}',
  now: 10,
  availableAt: 10,
  priority: 0,
  attempts: 2,
}
const claim = { queue: input.queue, now: 10, limit: 10, leaseDuration: 20 }

function child(path, operation) {
  const file = fileURLToPath(new URL('./fixtures/process-worker.mjs', import.meta.url))
  const permissions = process.versions.deno
    ? ['run', '--allow-read', '--allow-write', '--allow-env']
    : []
  const worker = spawn(
    process.execPath,
    [...permissions, file, path, operation.method, JSON.stringify(operation.input)],
    { stdio: ['pipe', 'pipe', 'pipe'] },
  )
  let resolveReady
  let rejectReady
  let resolveResult
  let rejectResult
  let result
  let received = false
  let stderr = ''
  const ready = new Promise((resolve, reject) => {
    resolveReady = resolve
    rejectReady = reject
  })
  const done = new Promise((resolve, reject) => {
    resolveResult = resolve
    rejectResult = reject
  })
  // Initialization may fail before the parent starts waiting for the result.
  void ready.catch(() => {})
  void done.catch(() => {})

  function fail(error) {
    rejectReady(error)
    rejectResult(error)
  }

  const timer = setTimeout(() => {
    fail(new Error(`Timed out waiting for ${operation.method}: ${stderr}`))
    worker.kill()
  }, 10_000)
  worker.stderr.on('data', (chunk) => {
    stderr += chunk
  })
  worker.on('error', fail)
  const lines = createInterface({ input: worker.stdout })
  lines.on('line', (line) => {
    if (line === 'ready') {
      resolveReady()
      return
    }

    try {
      result = JSON.parse(line)
      received = true
    } catch (error) {
      fail(error)
      worker.kill()
    }
  })
  worker.on('close', (code) => {
    clearTimeout(timer)
    lines.close()
    if (code !== 0 || !received) fail(new Error(`Worker failed (${code}): ${stderr}`))
    else resolveResult(result)
  })

  return { worker, ready, done }
}

async function race(path, operations) {
  const children = operations.map((operation) => child(path, operation))

  try {
    await Promise.all(children.map((entry) => entry.ready))
    // All independent processes have initialized their own connection before the gate opens.
    for (const entry of children) entry.worker.stdin.end('go\n')

    return await Promise.all(children.map((entry) => entry.done))
  } finally {
    for (const entry of children) {
      if (entry.worker.exitCode === null) entry.worker.kill()
    }
    await Promise.allSettled(children.map((entry) => entry.done))
  }
}

async function withDatabase(test) {
  const directory = mkdtempSync(join(tmpdir(), 'walq-process-race-'))
  const path = join(directory, 'queue.sqlite')
  const db = new DatabaseSync(path)

  try {
    db.exec('PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000')
    await test(path, sqlite(db))
  } finally {
    db.close()
    rmSync(directory, { recursive: true, force: true })
  }
}

it('independent runtime processes never issue duplicate live leases', async () => {
  await withDatabase(async (path, storage) => {
    await storage.enqueueMany(Array.from({ length: 30 }, () => ({ ...input })))
    const results = await race(
      path,
      Array.from({ length: 4 }, () => ({ method: 'claim', input: claim })),
    )
    const jobs = results.flat()

    assert.equal(jobs.length, 30)
    assert.equal(new Set(jobs.map((job) => job.id)).size, 30)
    assert.equal(new Set(jobs.map((job) => job.leaseToken)).size, 30)
    assert.equal((await storage.count({ queue: input.queue })).active, 30)
  })
})

it('independent runtime processes deduplicate inserts and enforce group concurrency', async () => {
  await withDatabase(async (path, storage) => {
    const results = await race(
      path,
      Array.from({ length: 4 }, () => ({
        method: 'enqueue',
        input: { ...input, dedupe: 'same', group: { id: 'account', concurrency: 2 } },
      })),
    )
    assert.equal(new Set(results.map((job) => job.id)).size, 1)
    await storage.enqueueMany(
      Array.from({ length: 8 }, () => ({ ...input, group: { id: 'account', concurrency: 2 } })),
    )

    const claims = await race(
      path,
      Array.from({ length: 4 }, () => ({ method: 'claim', input: claim })),
    )
    assert.equal(claims.flat().length, 2)
    assert.equal((await storage.count({ queue: input.queue })).active, 2)
  })
})

it('independent runtime processes materialize each scheduled occurrence only once', async () => {
  await withDatabase(async (path, storage) => {
    await storage.upsertSchedule({
      queue: input.queue,
      id: 'repeat',
      data: '{}',
      every: 10,
      now: 0,
    })
    const results = await race(
      path,
      Array.from({ length: 4 }, () => ({
        method: 'materializeSchedules',
        input: { queue: input.queue, now: 10, attempts: 2 },
      })),
    )

    assert.equal(
      results.reduce((total, value) => total + value, 0),
      1,
    )
    assert.equal((await storage.count({ queue: input.queue })).pending, 1)
    assert.equal((await storage.getSchedule({ queue: input.queue, id: 'repeat' })).nextRunAt, 20)
  })
})
