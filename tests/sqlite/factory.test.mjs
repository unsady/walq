import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, it } from 'node:test'

import {
  runCleanupConformance,
  runGroupedClaimConformance,
  runStorageConformance,
} from '../storage/conformance.ts'

// Dedicated threads are currently verified only on Node.js.
const isNode = !process.versions.bun && !process.versions.deno
const { expect } = process.versions.bun ? await import('bun:test') : await import('expect')
const factories = {
  sqlite: (await import('../../packages/sqlite/dist/index.js')).createStorage,
  ...(isNode && {
    'better-sqlite3': (await import('../../packages/better-sqlite3/dist/index.js')).createStorage,
  }),
}
const hooks = { describe, it, beforeEach, afterEach, expect }
const input = {
  queue: 'test',
  name: 'job',
  data: '{}',
  now: 10,
  availableAt: 10,
  priority: 0,
  attempts: 1,
}

if (isNode) {
  it('preserves causes and custom diagnostics through a real storage worker', async () => {
    const module = new URL('../../packages/sqlite-common/dist/index.js', import.meta.url)
    const { createWorkerStorage } = await import(module.href)
    const source = `
import { serveStorage } from ${JSON.stringify(module.href)}
serveStorage({
  count: async ({ queue }) => {
    const cause = Object.assign(new TypeError('query failed'), {
      code: 'ERR_SQLITE_ERROR', errcode: 1811, errstr: 'constraint failed',
    })
    const error = new Error('operation failed', { cause })
    if (queue === 'primitive') error.cause = { reason: 'query failed' }
    if (queue === 'circular') error.cause = error
    if (queue === 'deep') {
      for (let index = 0; index < 100; index++) error.cause = new Error('nested', { cause: error.cause })
    }
    throw error
  },
  close: async () => {},
})
`
    const storage = await createWorkerStorage(
      new URL(`data:text/javascript,${encodeURIComponent(source)}`),
      { filename: ':memory:' },
    )

    try {
      for (const queue of ['normal', 'primitive', 'circular', 'deep']) {
        await assert.rejects(storage.count({ queue }), (error) => {
          assert.equal(error.message, 'operation failed')
          assert.match(error.stack, /Error: operation failed/)
          if (queue === 'normal') {
            assert.ok(error.cause instanceof TypeError)
            assert.equal(error.cause.message, 'query failed')
            assert.match(error.cause.stack, /TypeError: query failed/)
            assert.equal(error.cause.code, 'ERR_SQLITE_ERROR')
            assert.equal(error.cause.errcode, 1811)
            assert.equal(error.cause.errstr, 'constraint failed')
          } else if (queue === 'primitive') {
            assert.deepEqual(error.cause, { reason: 'query failed' })
          } else {
            let depth = 0
            for (let current = error; current instanceof Error; current = current.cause) depth++
            assert.ok(depth < 100)
          }
          return true
        })
      }
    } finally {
      await storage.close()
    }
  })
}

for (const [name, createStorage] of Object.entries(factories)) {
  for (const worker of isNode ? [false, true] : [false]) {
    describe(`${name} factory (worker=${worker})`, () => {
      const instances = []

      async function open() {
        const storage = await createStorage({ filename: ':memory:', worker })
        instances.push(storage)

        return storage
      }

      async function cleanup() {
        await Promise.all(instances.splice(0).map((storage) => storage.close()))
      }

      afterEach(cleanup)

      runStorageConformance(open, cleanup, hooks)
      runGroupedClaimConformance(open, cleanup, hooks)
      runCleanupConformance(open, cleanup, hooks)

      it('drains accepted calls and rejects calls after an idempotent close', async () => {
        const storage = await open()
        const inserted = storage.enqueueMany([input, input])
        const closed = storage.close()
        const closedAgain = storage.close()
        await assert.rejects(storage.count({ queue: 'test' }), /closed/)
        assert.equal((await inserted).length, 2)
        await Promise.all([closed, closedAgain])
      })

      it('preserves validation errors and continues after a failed operation', async () => {
        const storage = await open()
        await assert.rejects(storage.enqueue({ ...input, attempts: 0 }), TypeError)
        assert.ok((await storage.enqueue(input)).id)
      })

      it('drains failed calls during close', async () => {
        const storage = await open()
        const rejected = assert.rejects(storage.enqueue({ ...input, attempts: 0 }), TypeError)
        const closed = storage.close()

        await rejected
        await closed
      })

      it('supports schedule operations through the facade', async () => {
        const storage = await open()
        await storage.upsertSchedule({
          queue: 'test',
          id: 'repeat',
          data: '{}',
          now: 10,
          every: 100,
        })
        assert.equal((await storage.getSchedule({ queue: 'test', id: 'repeat' })).every, 100)
        assert.equal(
          await storage.materializeSchedules({ queue: 'test', now: 110, attempts: 1 }),
          1,
        )
        assert.equal(await storage.removeSchedule({ queue: 'test', id: 'repeat' }), true)
      })

      if (isNode) {
        it('preserves SQLite error diagnostics in both execution modes', async () => {
          const directory = await mkdtemp(join(tmpdir(), 'walq-errors-'))
          const filename = join(directory, 'queue.sqlite')
          let storage

          try {
            storage = await createStorage({ filename })
            await storage.close()
            storage = await createStorage({
              filename,
              worker,
              initialization:
                "CREATE TRIGGER forced_failure BEFORE INSERT ON walq_jobs BEGIN SELECT RAISE(ABORT, 'forced failure'); END;",
            })
            await assert.rejects(storage.enqueue(input), (error) => {
              assert.equal(
                error.code,
                name === 'sqlite' ? 'ERR_SQLITE_ERROR' : 'SQLITE_CONSTRAINT_TRIGGER',
              )
              if (name === 'sqlite') {
                assert.equal(error.errcode, 1811)
                assert.equal(error.errstr, 'constraint failed')
              }
              return true
            })
            assert.equal((await storage.count({ queue: input.queue })).pending, 0)
          } finally {
            await storage?.close()
            await rm(directory, { recursive: true, force: true })
          }
        })
      }

      if (worker) {
        it('keeps main-thread timers running while SQLite waits for a write lock', async () => {
          const directory = await mkdtemp(join(tmpdir(), 'walq-lock-'))
          const Database =
            name === 'sqlite'
              ? (await import('node:sqlite')).DatabaseSync
              : (await import('better-sqlite3')).default
          let storage
          let db
          let timer

          try {
            const filename = join(directory, 'queue.sqlite')
            storage = await createStorage({ filename, worker })
            db = new Database(filename)
            db.exec('BEGIN IMMEDIATE')
            let released = false
            timer = setTimeout(() => {
              db.exec('ROLLBACK')
              released = true
            }, 50)

            await storage.enqueue(input)
            assert.equal(released, true)
          } finally {
            clearTimeout(timer)
            if (db?.isTransaction ?? db?.inTransaction) db.exec('ROLLBACK')
            db?.close()
            await storage?.close()
            await rm(directory, { recursive: true, force: true })
          }
        })
      }

      it('rejects initialization failures', async () => {
        await assert.rejects(
          createStorage({ filename: ':memory:', worker, initialization: 'INVALID SQL' }),
        )
      })

      it('persists data and honors initialization SQL', async () => {
        const directory = await mkdtemp(join(tmpdir(), 'walq-factory-'))
        let storage

        try {
          const filename = join(directory, 'queue.sqlite')
          storage = await createStorage({
            filename,
            worker,
            initialization: 'PRAGMA user_version = 123',
          })
          const job = await storage.enqueue(input)
          await storage.close()
          storage = await createStorage({ filename, worker })
          assert.equal((await storage.inspect({ queue: input.queue, id: job.id })).id, job.id)

          const { DatabaseSync } = await import('node:sqlite')
          const db = new DatabaseSync(filename)
          try {
            assert.equal(db.prepare('PRAGMA user_version').get().user_version, 123)
          } finally {
            db.close()
          }
        } finally {
          await storage?.close()
          await rm(directory, { recursive: true, force: true })
        }
      })
    })
  }
}
