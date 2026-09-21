import { randomUUID } from 'node:crypto'

import type {
  ClaimedJob,
  ClaimInput,
  ClaimQueuesInput,
  ClaimQueuesResult,
  CleanupInput,
  CleanupResult,
  CompleteInput,
  EnqueueInput,
  FailInput,
  HeartbeatInput,
  LeaseMutationResult,
  Storage,
  StoredJob,
} from '@walq/core/storage'
import type Database from 'better-sqlite3'

import { chunkClaims } from './chunking.js'
import { TerminalCleanup } from './cleanup.js'
import { initialize } from './schema.js'
import { expiry, integer, lease, retentionRule, text } from './validation.js'

const metadata =
  'id, queue, name, data, status, createdAt, availableAt, attemptsMade, attempts, error'
const liveLease = "id = @id AND status = 'active' AND leaseToken = @leaseToken AND expiresAt > @now"

interface ClaimStep {
  input: ClaimInput
  expiresAt: number
}

function prepare(db: Database.Database, sql: string): Database.Statement {
  return db.prepare(sql).safeIntegers(false)
}

class BetterSqlite3Storage implements Storage {
  private readonly db: Database.Database
  private readonly insert: Database.Statement
  private readonly recover: Database.Statement
  private readonly select: Database.Statement
  private readonly acquire: Database.Statement
  private readonly completeStatement: Database.Statement
  private readonly failStatement: Database.Statement
  private readonly heartbeatStatement: Database.Statement
  private readonly claimTransaction: Database.Transaction<(steps: ClaimStep[]) => ClaimedJob[][]>
  private readonly terminalCleanup: TerminalCleanup

  constructor(db: Database.Database) {
    this.db = db
    initialize(db)

    this.insert = prepare(
      db,
      `
        INSERT INTO walq_jobs (id, queue, name, data, status, createdAt, availableAt, attemptsMade, attempts)
        VALUES (@id, @queue, @name, @data, 'pending', @now, @availableAt, 0, @attempts)
        RETURNING ${metadata}
      `,
    )
    this.recover = prepare(
      db,
      `
        UPDATE walq_jobs SET
          status = CASE WHEN attemptsMade < attempts THEN 'pending' ELSE 'failed' END,
          availableAt = CASE WHEN attemptsMade < attempts THEN expiresAt ELSE availableAt END,
          finishedAt = CASE WHEN attemptsMade < attempts THEN NULL ELSE @now END,
          leaseToken = NULL, expiresAt = NULL
        WHERE queue = @queue AND status = 'active' AND expiresAt <= @now
      `,
    )
    this.select = prepare(
      db,
      `
        SELECT id FROM walq_jobs
        WHERE queue = @queue AND status = 'pending' AND availableAt <= @now AND attemptsMade < attempts
        ORDER BY availableAt, id COLLATE BINARY LIMIT @limit
      `,
    )
    this.acquire = prepare(
      db,
      `
        UPDATE walq_jobs SET status = 'active', attemptsMade = attemptsMade + 1,
          leaseToken = @leaseToken, expiresAt = @expiresAt
        WHERE id = @id
        RETURNING ${metadata}, leaseToken, expiresAt
      `,
    )
    this.completeStatement = prepare(
      db,
      `
        UPDATE walq_jobs SET status = 'completed', finishedAt = @now, leaseToken = NULL, expiresAt = NULL
        WHERE ${liveLease}
      `,
    )
    this.failStatement = prepare(
      db,
      `
        UPDATE walq_jobs SET
          status = CASE WHEN @retryAt IS NOT NULL AND attemptsMade < attempts THEN 'pending' ELSE 'failed' END,
          availableAt = CASE WHEN @retryAt IS NOT NULL AND attemptsMade < attempts THEN @retryAt ELSE availableAt END,
          finishedAt = CASE WHEN @retryAt IS NOT NULL AND attemptsMade < attempts THEN NULL ELSE @now END,
          error = @error, leaseToken = NULL, expiresAt = NULL
        WHERE ${liveLease}
      `,
    )
    this.heartbeatStatement = prepare(
      db,
      `
        UPDATE walq_jobs SET expiresAt = max(expiresAt, @expiresAt) WHERE ${liveLease}
      `,
    )
    this.claimTransaction = db.transaction((steps: ClaimStep[]): ClaimedJob[][] =>
      steps.map(({ input, expiresAt }) => this.claimStep(input, expiresAt)),
    )
    this.terminalCleanup = new TerminalCleanup(db)
  }

  private prepareClaim(input: ClaimInput): ClaimStep {
    text(input.queue, 'queue')
    integer(input.limit, 'limit', 1)
    return { input, expiresAt: expiry(input.now, input.leaseDuration) }
  }

  /**
   * Must run inside the shared immediate transaction so the sequence stays
   * atomic per request.
   */
  private claimStep(input: ClaimInput, expiresAt: number): ClaimedJob[] {
    this.recover.run(input)
    const jobs = this.select.all(input) as { id: string }[]
    return jobs.map(
      ({ id }) => this.acquire.get({ id, leaseToken: randomUUID(), expiresAt }) as ClaimedJob,
    )
  }

  private assertAutocommit(): void {
    // Resolving before an outer transaction commits would violate the storage contract.
    if (this.db.inTransaction) throw new Error('Storage operations cannot run inside a transaction')
  }

  async enqueue(input: EnqueueInput): Promise<StoredJob> {
    this.assertAutocommit()
    text(input.queue, 'queue')
    text(input.name, 'name')
    text(input.data, 'data')
    JSON.parse(input.data)
    integer(input.now, 'now')
    integer(input.availableAt, 'availableAt')
    integer(input.attempts, 'attempts', 1)
    return this.insert.get({ ...input, id: randomUUID() }) as StoredJob
  }

  async claim(input: ClaimInput): Promise<ClaimedJob[]> {
    this.assertAutocommit()
    const [jobs = []] = this.claimTransaction.immediate([this.prepareClaim(input)])
    return jobs
  }

  async claimQueues(input: ClaimQueuesInput): Promise<ClaimQueuesResult> {
    this.assertAutocommit()
    if (!Array.isArray(input.requests)) throw new TypeError('requests must be an array')
    // Validate the whole batch before opening a transaction so a bad request
    // cannot mutate a queue that a later request would have touched.
    const steps = input.requests.map((request) => this.prepareClaim(request))
    if (steps.length === 0) return []

    // A sweep can cover many queues, so requests are packed until their summed
    // limits reach `claimBudget` (see `chunking.ts`). Chunks commit in order: a
    // failure in a later chunk keeps the earlier ones, which the storage
    // contract allows.
    return chunkClaims(steps, (step) => step.input.limit).flatMap((chunk) =>
      this.claimTransaction.immediate(chunk),
    )
  }

  async complete(input: CompleteInput): Promise<LeaseMutationResult> {
    this.assertAutocommit()
    lease(input)
    return this.completeStatement.run(input).changes === 1 ? 'applied' : 'lease_lost'
  }

  async fail(input: FailInput): Promise<LeaseMutationResult> {
    this.assertAutocommit()
    lease(input)
    text(input.error, 'error', false)
    if (input.retryAt !== null) integer(input.retryAt, 'retryAt')
    return this.failStatement.run(input).changes === 1 ? 'applied' : 'lease_lost'
  }

  async heartbeat(input: HeartbeatInput): Promise<LeaseMutationResult> {
    this.assertAutocommit()
    lease(input)
    const expiresAt = expiry(input.now, input.leaseDuration)
    return this.heartbeatStatement.run({ ...input, expiresAt }).changes === 1
      ? 'applied'
      : 'lease_lost'
  }

  async cleanup(input: CleanupInput): Promise<CleanupResult> {
    this.assertAutocommit()
    text(input.queue, 'queue')
    integer(input.limit, 'limit', 1)
    integer(input.now, 'now')
    if (input.retention === null || typeof input.retention !== 'object') {
      throw new TypeError('retention must be an object')
    }
    retentionRule(input.retention.completed, 'retention.completed')
    retentionRule(input.retention.failed, 'retention.failed')
    return this.terminalCleanup.run(input)
  }
}

/** The caller owns the connection and configures its durability and busy timeout. */
export function betterSqlite3(db: Database.Database): Storage {
  return new BetterSqlite3Storage(db)
}
