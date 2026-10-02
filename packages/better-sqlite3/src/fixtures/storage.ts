import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Worker } from 'node:worker_threads'

import type { ClaimedJob, LeaseMutationResult, StoredJob } from '@walq/core/storage'
import Database from 'better-sqlite3'
import { afterEach, expect } from 'vitest'

import { betterSqlite3 } from '../index.js'

export const databases: Database.Database[] = []
const directories: string[] = []
export const input = {
  queue: 'email',
  name: 'send',
  data: '{"to":"a"}',
  now: 10,
  availableAt: 10,
  priority: 0,
  attempts: 2,
}
export const claimInput = { queue: 'email', now: 10, limit: 10, leaseDuration: 20 }

export function open(filename = ':memory:') {
  const db = new Database(filename)
  databases.push(db)
  return { db, storage: betterSqlite3(db) }
}

export function filename() {
  const directory = mkdtempSync(join(tmpdir(), 'walq-'))
  directories.push(directory)
  return join(directory, 'queue.sqlite')
}

interface SchemaConnection {
  exec(sql: string): unknown
  prepare(sql: string): { get(): unknown }
}

export function revertGroupScheduling(db: SchemaConnection): void {
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
  `)
}

export function expectPartialDedupeIndex(db: SchemaConnection): void {
  const index = db
    .prepare("SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'walq_dedupe'")
    .get() as { sql: string } | undefined
  expect(index?.sql).toMatch(/WHERE dedupe IS NOT NULL$/)
}

const raceTimeout = 4000

export async function race(
  path: string,
  operations: { method: string; input: object }[],
  driver: 'better-sqlite3' | 'node:sqlite' = 'better-sqlite3',
) {
  const gate = new SharedArrayBuffer(4)
  const workers: Worker[] = []
  const timers: NodeJS.Timeout[] = []
  try {
    const tasks = operations.map(
      (operation) =>
        new Promise<ClaimedJob[] | StoredJob | LeaseMutationResult | boolean | number>(
          (resolve, reject) => {
            let settled = false
            const timer = setTimeout(() => {
              settled = true
              reject(new Error(`Timed out waiting for worker ${operation.method}`))
            }, raceTimeout)
            timers.push(timer)
            const worker = new Worker(new URL('./claim-worker.ts', import.meta.url), {
              workerData: { path, gate, driver, count: operations.length, ...operation },
            })
            workers.push(worker)
            worker.on(
              'message',
              (
                message:
                  | {
                      ready: true
                    }
                  | { result: ClaimedJob[] | StoredJob | LeaseMutationResult | boolean | number },
              ) => {
                if (!('result' in message)) {
                  Atomics.add(new Int32Array(gate), 0, 1)
                  if (Atomics.load(new Int32Array(gate), 0) === operations.length)
                    Atomics.notify(new Int32Array(gate), 0)
                } else if (!settled) {
                  settled = true
                  clearTimeout(timer)
                  resolve(message.result)
                }
              },
            )
            worker.on('error', (error) => {
              if (!settled) {
                settled = true
                clearTimeout(timer)
                reject(error)
              }
            })
            worker.on('exit', (code) => {
              if (!settled) {
                settled = true
                clearTimeout(timer)
                reject(new Error(`Worker exited before result: ${code}`))
              }
            })
          },
        ),
    )
    return await Promise.all(tasks)
  } finally {
    for (const timer of timers) clearTimeout(timer)
    await Promise.all(workers.map((worker) => worker.terminate()))
  }
}

export function setupCleanup(): void {
  afterEach(() => {
    for (const db of databases.splice(0)) if (db.open) db.close()
    for (const directory of directories.splice(0))
      rmSync(directory, { recursive: true, force: true })
  })
}
