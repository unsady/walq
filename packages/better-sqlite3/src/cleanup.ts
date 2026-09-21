import type Database from 'better-sqlite3'
import type { CleanupInput, CleanupResult, RetentionRule } from 'walq/storage'

const terminalStatuses = ['completed', 'failed'] as const

type TerminalStatus = (typeof terminalStatuses)[number]

/** The newest eligible row of one status, in `(finishedAt, id)` order. */
type RetentionBoundary = { finishedAt: number; id: string }

/** Eligibility and deletion for one status, derived from its retention rule. */
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
 * Every statement is an index lookup: locating the newest eligible row walks at
 * most the count bound plus one index entry, deletion is bounded by `limit`, and
 * `more` is decided by existence checks. No call performs an unbounded count or
 * delete.
 *
 * Inputs must already be validated at the storage boundary.
 */
export class TerminalCleanup {
  readonly #selectFrontier: Database.Statement
  readonly #selectAgeFrontier: Database.Statement
  readonly #hasEligible: Database.Statement
  readonly #deleteEligible: Database.Statement
  readonly #transaction: Database.Transaction<(input: CleanupInput) => CleanupResult>

  constructor(db: Database.Database) {
    // A count bound contributes the row just older than the newest `count` rows;
    // an age bound contributes the newest row finished before the cutoff. The
    // newest of the two is the frontier of the union of both eligible tails.
    this.#selectFrontier = prepare(
      db,
      `
        SELECT finishedAt AS finishedAt, id AS id FROM (
          SELECT finishedAt AS finishedAt, id AS id FROM walq_jobs
          WHERE queue = @queue AND status = @status AND finishedAt IS NOT NULL
          ORDER BY finishedAt DESC, id DESC
          LIMIT 1 OFFSET @offset
        )
        UNION ALL
        SELECT finishedAt AS finishedAt, id AS id FROM (
          SELECT finishedAt AS finishedAt, id AS id FROM walq_jobs
          WHERE queue = @queue AND status = @status AND finishedAt IS NOT NULL
            AND finishedAt < @cutoff
          ORDER BY finishedAt DESC, id DESC
          LIMIT 1
        )
        ORDER BY finishedAt DESC, id DESC
        LIMIT 1
      `,
    )
    this.#selectAgeFrontier = prepare(
      db,
      `
        SELECT finishedAt AS finishedAt, id AS id FROM walq_jobs
        WHERE queue = @queue AND status = @status AND finishedAt IS NOT NULL
          AND finishedAt < @cutoff
        ORDER BY finishedAt DESC, id DESC
        LIMIT 1
      `,
    )
    this.#hasEligible = prepare(
      db,
      `
        SELECT 1 AS found FROM walq_jobs
        WHERE queue = @queue AND status = @status AND finishedAt IS NOT NULL
          AND (finishedAt, id) <= (@finishedAt, @id)
        LIMIT 1
      `,
    )
    this.#deleteEligible = prepare(
      db,
      `
        DELETE FROM walq_jobs
        WHERE rowid IN (
          SELECT rowid FROM walq_jobs
          WHERE queue = @queue AND status = @status AND finishedAt IS NOT NULL
            AND (finishedAt, id) <= (@finishedAt, @id)
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
      const plan = this.#plan(input.queue, status, input.retention[status], input.now)
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
   * Both bounds are upper limits on the oldest tail, so their union is the
   * single tail up to the newest eligible row. Rows exactly at the cutoff are
   * retained: only `finishedAt < cutoff` is eligible.
   */
  #plan(
    queue: string,
    status: TerminalStatus,
    rule: RetentionRule,
    now: number,
  ): StatusPlan | undefined {
    if (rule.count === null && rule.maxAge === null) return undefined

    // A null age bound passes cutoff 0; finishedAt is never negative, so the age
    // predicate matches nothing and only the count bound remains.
    const cutoff = rule.maxAge === null ? 0 : Math.max(0, now - rule.maxAge)
    const frontier =
      rule.count === null
        ? (this.#selectAgeFrontier.get({ queue, status, cutoff }) as RetentionBoundary | undefined)
        : (this.#selectFrontier.get({ queue, status, offset: rule.count, cutoff }) as
            | RetentionBoundary
            | undefined)
    if (frontier === undefined) return undefined

    return {
      hasMore: () => this.#hasEligible.get({ queue, status, ...frontier }) !== undefined,
      deleteEligible: (limit) =>
        this.#deleteEligible.run({ queue, status, ...frontier, limit }).changes,
    }
  }
}
