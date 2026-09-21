import type { CleanupInput, CleanupResult } from '@walq/core/storage'
import type Database from 'better-sqlite3'

const terminalStatuses = ['completed', 'failed'] as const

type TerminalStatus = (typeof terminalStatuses)[number]

/** The oldest row that a retention count keeps for one status. */
type RetentionBoundary = { finishedAt: number; id: string }

/** Eligibility and deletion for one status, derived from its retention count. */
type StatusPlan = {
  hasMore(): boolean
  deleteEligible(limit: number): number
}

function prepare(db: Database.Database, sql: string): Database.Statement {
  return db.prepare(sql).safeIntegers(false)
}

/**
 * Bounded terminal-job retention for one better-sqlite3 connection.
 *
 * Every statement is an index lookup: locating the retained boundary walks at
 * most `keep` index entries, deletion is bounded by `limit`, and `more` is
 * decided by existence checks. No call performs an unbounded count or delete.
 *
 * Inputs must already be validated at the storage boundary.
 */
export class TerminalCleanup {
  readonly #selectBoundary: Database.Statement
  readonly #hasTerminal: Database.Statement
  readonly #hasOlderTerminal: Database.Statement
  readonly #deleteOldest: Database.Statement
  readonly #deleteOlder: Database.Statement
  readonly #transaction: Database.Transaction<(input: CleanupInput) => CleanupResult>

  constructor(db: Database.Database) {
    this.#selectBoundary = prepare(
      db,
      `
        SELECT finishedAt AS finishedAt, id AS id FROM walq_jobs
        WHERE queue = @queue AND status = @status AND finishedAt IS NOT NULL
        ORDER BY finishedAt DESC, id DESC
        LIMIT 1 OFFSET @offset
      `,
    )
    this.#hasTerminal = prepare(
      db,
      `
        SELECT 1 AS found FROM walq_jobs
        WHERE queue = @queue AND status = @status AND finishedAt IS NOT NULL
        LIMIT 1
      `,
    )
    this.#hasOlderTerminal = prepare(
      db,
      `
        SELECT 1 AS found FROM walq_jobs
        WHERE queue = @queue AND status = @status AND finishedAt IS NOT NULL
          AND (finishedAt, id) < (@finishedAt, @id)
        LIMIT 1
      `,
    )
    this.#deleteOldest = prepare(
      db,
      `
        DELETE FROM walq_jobs
        WHERE rowid IN (
          SELECT rowid FROM walq_jobs
          WHERE queue = @queue AND status = @status AND finishedAt IS NOT NULL
          ORDER BY finishedAt, id
          LIMIT @limit
        )
      `,
    )
    this.#deleteOlder = prepare(
      db,
      `
        DELETE FROM walq_jobs
        WHERE rowid IN (
          SELECT rowid FROM walq_jobs
          WHERE queue = @queue AND status = @status AND finishedAt IS NOT NULL
            AND (finishedAt, id) < (@finishedAt, @id)
          ORDER BY finishedAt, id
          LIMIT @limit
        )
      `,
    )
    this.#transaction = db.transaction((input: CleanupInput): CleanupResult => this.#execute(input))
  }

  run(input: CleanupInput): CleanupResult {
    return this.#transaction.immediate(input)
  }

  #execute(input: CleanupInput): CleanupResult {
    let budget = input.limit
    let removed = 0

    for (const status of terminalStatuses) {
      const keep = input.retention[status]
      if (keep === null) continue

      const plan = this.#plan(input.queue, status, keep)
      if (plan === undefined) continue

      if (budget > 0) {
        const deleted = plan.deleteEligible(budget)
        removed += deleted
        budget -= deleted
      }

      if (plan.hasMore()) return { removed, more: true }
    }

    return { removed, more: false }
  }

  /**
   * A status with no retained rows deletes its oldest rows. Otherwise only rows
   * older than the retained boundary are eligible, so an interrupted drain
   * keeps the newest history. Fewer terminal rows than `keep` leaves nothing.
   */
  #plan(queue: string, status: TerminalStatus, keep: number): StatusPlan | undefined {
    if (keep === 0) {
      return {
        hasMore: () => this.#hasTerminal.get({ queue, status }) !== undefined,
        deleteEligible: (limit) => this.#deleteOldest.run({ queue, status, limit }).changes,
      }
    }

    const boundary = this.#selectBoundary.get({ queue, status, offset: keep - 1 }) as
      | RetentionBoundary
      | undefined
    if (boundary === undefined) return undefined

    return {
      hasMore: () => this.#hasOlderTerminal.get({ queue, status, ...boundary }) !== undefined,
      deleteEligible: (limit) =>
        this.#deleteOlder.run({ queue, status, ...boundary, limit }).changes,
    }
  }
}
