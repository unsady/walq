import { randomUUID } from 'node:crypto'
import { performance } from 'node:perf_hooks'
import { parentPort, workerData } from 'node:worker_threads'

import Database from 'better-sqlite3'

import type { SynchronousMode } from '../bench-options.js'

export interface ClaimCompetitorInput {
  path: string
  control: SharedArrayBuffer
  synchronous: SynchronousMode
}

export interface ClaimCompetitorReport {
  enqueueSamples: number[]
  completeSamples: number[]
  operations: number
  errors: number
  firstError: string | null
}

interface Lease {
  id: string
  leaseToken: string
}

const preparedLeases = 512

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

async function main(): Promise<void> {
  const input = workerData as ClaimCompetitorInput
  const control = new Int32Array(input.control)
  const db = new Database(input.path)
  db.pragma('journal_mode = WAL')
  db.pragma(`synchronous = ${input.synchronous.toUpperCase()}`)
  db.pragma('busy_timeout = 2000')
  const insert = db.prepare(`
    INSERT INTO walq_jobs (
      id, queue, name, data, status, createdAt, availableAt, attemptsMade, attempts
    ) VALUES (@id, 'competitor', 'job', '{}', 'pending', @now, @now, 0, 1)
  `)
  const insertActive = db.prepare(`
    INSERT INTO walq_jobs (
      id, queue, name, data, status, createdAt, availableAt, attemptsMade, attempts,
      leaseToken, expiresAt
    ) VALUES (
      @id, 'competitor-complete', 'job', '{}', 'active', @now, @now, 1, 1,
      @leaseToken, @expiresAt
    )
  `)
  const complete = db.prepare(`
    UPDATE walq_jobs SET status = 'completed', finishedAt = @now, leaseToken = NULL, expiresAt = NULL
    WHERE id = @id AND status = 'active' AND leaseToken = @leaseToken AND expiresAt > @now
  `)
  const leases: Lease[] = []
  const prepareLeases = db.transaction(() => {
    const now = Date.now()
    for (let index = 0; index < preparedLeases; index += 1) {
      const lease = { id: randomUUID(), leaseToken: randomUUID() }
      insertActive.run({ ...lease, now, expiresAt: now + 120_000 })
      leases.push(lease)
    }
  })
  prepareLeases.immediate()

  const enqueueSamples: number[] = []
  const completeSamples: number[] = []
  let operations = 0
  let errors = 0
  let firstError: string | null = null

  parentPort?.postMessage({ phase: 'ready' })
  Atomics.wait(control, 0, 0)

  try {
    while (Atomics.load(control, 1) === 0 && operations < leases.length) {
      try {
        const lease = leases[operations]
        if (lease === undefined) throw new Error('competitor exhausted its prepared leases')
        function enqueue(): void {
          const started = performance.now()
          insert.run({ id: randomUUID(), now: Date.now() })
          enqueueSamples.push(performance.now() - started)
        }
        function completeLease(): void {
          const started = performance.now()
          const result = complete.run({ ...lease, now: Date.now() })
          completeSamples.push(performance.now() - started)
          if (result.changes !== 1) throw new Error('competitor lost its lease')
        }

        if (operations % 2 === 0) {
          enqueue()
          completeLease()
        } else {
          completeLease()
          enqueue()
        }
        operations += 1
        Atomics.store(control, 2, operations)
        Atomics.wait(control, 1, 0, 1)
      } catch (error) {
        errors += 1
        firstError ??= message(error)
        Atomics.wait(control, 1, 0, 1)
      }
    }
  } finally {
    db.close()
  }

  const report: ClaimCompetitorReport = {
    enqueueSamples,
    completeSamples,
    operations,
    errors,
    firstError,
  }
  parentPort?.postMessage({ phase: 'result', report })
}

void main().catch((error) => {
  parentPort?.postMessage({ phase: 'error', error: message(error) })
})
