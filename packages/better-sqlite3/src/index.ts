import { randomUUID } from 'node:crypto'

import type {
  CancelInput,
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
  InspectInput,
  JobSnapshot,
  JobStatus,
  LeaseMutationResult,
  ListInput,
  RemoveInput,
  RescheduleInput,
  RetryInput,
  Storage,
  StoredJob,
} from '@walq/core/storage'
import type Database from 'better-sqlite3'

import { chunkClaims } from './chunking.js'
import { TerminalCleanup } from './cleanup.js'
import { initialize } from './schema.js'
import { expiry, integer, lease, retentionRule, text } from './validation.js'

const metadata =
  'id, queue, name, data, status, createdAt, availableAt, priority, attemptsMade, attempts, error'
const snapshotMetadata = `${metadata}, finishedAt`
const liveLease = "id = @id AND status = 'active' AND leaseToken = @leaseToken AND expiresAt > @now"
const maxSafeInteger = Number.MAX_SAFE_INTEGER
const jobStatuses: readonly JobStatus[] = ['pending', 'active', 'completed', 'failed', 'cancelled']

interface ClaimStep {
  input: ClaimInput
  expiresAt: number
}

interface PendingCandidate {
  id: string
  groupId: string | null
  priority: number
  availableAt: number
  seq: number
}

interface GroupCapacity {
  id: string
  concurrency: number
  activeCount: number
}

const maxClaimScanPageSize = 256

function prepare(db: Database.Database, sql: string): Database.Statement {
  return db.prepare(sql).safeIntegers(false)
}

function validateObject(input: unknown, name: string): asserts input is Record<string, unknown> {
  if (typeof input !== 'object' || input === null || Array.isArray(input)) {
    throw new TypeError(`${name} must be an object`)
  }
}

function validateInspect(input: InspectInput): InspectInput {
  validateObject(input, 'input')
  const validated = { queue: input.queue, id: input.id }
  text(validated.queue, 'queue')
  text(validated.id, 'id')
  return validated
}

function validateEnqueue(input: EnqueueInput): EnqueueInput {
  if (typeof input !== 'object' || input === null || Array.isArray(input)) {
    throw new TypeError('enqueue input must be an object')
  }

  const validated = {
    queue: input.queue,
    name: input.name,
    data: input.data,
    now: input.now,
    availableAt: input.availableAt,
    priority: input.priority,
    ...(input.dedupe !== undefined ? { dedupe: input.dedupe } : {}),
    ...(input.group !== undefined ? { group: input.group } : {}),
    attempts: input.attempts,
  }
  text(validated.queue, 'queue')
  text(validated.name, 'name')
  text(validated.data, 'data')
  JSON.parse(validated.data)
  integer(validated.now, 'now')
  integer(validated.availableAt, 'availableAt')
  integer(validated.priority, 'priority', -maxSafeInteger)
  if (validated.dedupe !== undefined) text(validated.dedupe, 'dedupe')
  if (validated.group !== undefined) {
    validateObject(validated.group, 'group')
    const group = { id: validated.group.id, concurrency: validated.group.concurrency }
    text(group.id, 'group.id')
    integer(group.concurrency, 'group.concurrency', 1)
    validated.group = group
  }
  integer(validated.attempts, 'attempts', 1)

  return validated
}

class BetterSqlite3Storage implements Storage {
  private readonly db: Database.Database
  private readonly insert: Database.Statement
  private readonly findDeduplicated: Database.Statement
  private readonly findGroup: Database.Statement
  private readonly insertGroup: Database.Statement
  private readonly enqueueManyTransaction: Database.Transaction<
    (inputs: EnqueueInput[]) => StoredJob[]
  >
  private readonly recover: Database.Statement
  private readonly selectPending: Database.Statement
  private readonly selectPendingAtPriorityAfter: Database.Statement
  private readonly selectPendingBelowPriority: Database.Statement
  private readonly acquire: Database.Statement
  private readonly inspectStatement: Database.Statement
  private readonly listStatements: Record<JobStatus, Database.Statement>
  private readonly retryStatement: Database.Statement
  private readonly retryOverflowStatement: Database.Statement
  private readonly retryTransaction: Database.Transaction<
    (input: InspectInput, now: number) => boolean
  >
  private readonly cancelStatement: Database.Statement
  private readonly rescheduleStatement: Database.Statement
  private readonly removeStatement: Database.Statement
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
        INSERT INTO walq_jobs (
          id, queue, name, data, status, createdAt, availableAt, priority, dedupe, groupId,
          attemptsMade, attempts
        )
        VALUES (
          @id, @queue, @name, @data, 'pending', @now, @availableAt, @priority, @dedupe,
          @groupId, 0, @attempts
        )
        RETURNING ${metadata}
      `,
    )
    this.findDeduplicated = prepare(
      db,
      `SELECT ${metadata} FROM walq_jobs WHERE queue = @queue AND dedupe = @dedupe`,
    )
    this.findGroup = prepare(
      db,
      'SELECT concurrency FROM walq_groups WHERE queue = @queue AND id = @id',
    )
    this.insertGroup = prepare(
      db,
      'INSERT INTO walq_groups (queue, id, concurrency) VALUES (@queue, @id, @concurrency) ON CONFLICT (queue, id) DO NOTHING',
    )
    this.enqueueManyTransaction = db.transaction((inputs: EnqueueInput[]) =>
      inputs.map((input) => this.insertOrGet(input)),
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
    this.selectPending = prepare(
      db,
      `
        SELECT id, groupId, priority, availableAt, seq
        FROM walq_jobs INDEXED BY walq_pending
        WHERE queue = @queue AND status = 'pending' AND availableAt <= @now
          AND attemptsMade < attempts
        ORDER BY priority DESC, availableAt, seq
        LIMIT @limit
      `,
    )
    this.selectPendingAtPriorityAfter = prepare(
      db,
      `
        SELECT id, groupId, priority, availableAt, seq
        FROM walq_jobs INDEXED BY walq_pending
        WHERE queue = @queue AND status = 'pending' AND availableAt <= @now
          AND attemptsMade < attempts AND priority = @priority
          AND (availableAt, seq) > (@availableAt, @seq)
        ORDER BY priority DESC, availableAt, seq
        LIMIT @limit
      `,
    )
    this.selectPendingBelowPriority = prepare(
      db,
      `
        SELECT id, groupId, priority, availableAt, seq
        FROM walq_jobs INDEXED BY walq_pending
        WHERE queue = @queue AND status = 'pending' AND availableAt <= @now
          AND attemptsMade < attempts AND priority < @priority
        ORDER BY priority DESC, availableAt, seq
        LIMIT @limit
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
    this.inspectStatement = prepare(
      db,
      `SELECT ${snapshotMetadata} FROM walq_jobs WHERE queue = @queue AND id = @id`,
    )
    this.listStatements = {
      pending: prepare(
        db,
        `SELECT ${snapshotMetadata} FROM walq_jobs
         WHERE queue = @queue AND status = 'pending'
         ORDER BY availableAt, seq LIMIT @limit`,
      ),
      active: prepare(
        db,
        `SELECT ${snapshotMetadata} FROM walq_jobs
         WHERE queue = @queue AND status = 'active'
         ORDER BY expiresAt, id COLLATE BINARY LIMIT @limit`,
      ),
      completed: prepare(
        db,
        `SELECT ${snapshotMetadata} FROM walq_jobs
         WHERE queue = @queue AND status = 'completed'
         ORDER BY finishedAt DESC, id COLLATE BINARY DESC LIMIT @limit`,
      ),
      failed: prepare(
        db,
        `SELECT ${snapshotMetadata} FROM walq_jobs
         WHERE queue = @queue AND status = 'failed'
         ORDER BY finishedAt DESC, id COLLATE BINARY DESC LIMIT @limit`,
      ),
      cancelled: prepare(
        db,
        `SELECT ${snapshotMetadata} FROM walq_jobs
         WHERE queue = @queue AND status = 'cancelled'
         ORDER BY finishedAt DESC, id COLLATE BINARY DESC LIMIT @limit`,
      ),
    }
    this.retryStatement = prepare(
      db,
      `
        UPDATE walq_jobs SET status = 'pending',
          availableAt = @now,
          attempts = CASE WHEN attemptsMade >= attempts THEN attemptsMade + 1 ELSE attempts END,
          finishedAt = NULL
        WHERE queue = @queue AND id = @id AND status = 'failed'
          AND (attemptsMade < attempts OR attemptsMade < @maxSafeInteger)
      `,
    )
    this.retryOverflowStatement = prepare(
      db,
      `SELECT 1 AS overflow FROM walq_jobs
       WHERE queue = @queue AND id = @id AND status = 'failed'
         AND attemptsMade >= attempts AND attemptsMade >= @maxSafeInteger`,
    )
    this.retryTransaction = db.transaction((validated: InspectInput, now: number) => {
      const changes = this.retryStatement.run({
        ...validated,
        now,
        maxSafeInteger,
      }).changes
      if (changes === 1) return true

      const overflow = this.retryOverflowStatement.get({
        ...validated,
        maxSafeInteger,
      })
      if (overflow !== undefined)
        throw new RangeError('attempts would exceed the safe integer range')
      return false
    })
    this.cancelStatement = prepare(
      db,
      `UPDATE walq_jobs SET status = 'cancelled', finishedAt = @now
       WHERE queue = @queue AND id = @id AND status = 'pending'`,
    )
    this.rescheduleStatement = prepare(
      db,
      `UPDATE walq_jobs SET availableAt = @availableAt
       WHERE queue = @queue AND id = @id AND status = 'pending'`,
    )
    this.removeStatement = prepare(
      db,
      `DELETE FROM walq_jobs WHERE queue = @queue AND id = @id AND status != 'active'`,
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

    const jobs: ClaimedJob[] = []
    const availableSlots = new Map<string, number>()
    let cursor: PendingCandidate | undefined
    let pageSize = Math.min(input.limit, maxClaimScanPageSize)

    while (jobs.length < input.limit) {
      let candidates: PendingCandidate[]
      if (cursor === undefined) {
        candidates = this.selectPending.all({ ...input, limit: pageSize }) as PendingCandidate[]
      } else {
        candidates = this.selectPendingAtPriorityAfter.all({
          ...input,
          ...cursor,
          limit: pageSize,
        }) as PendingCandidate[]
        if (candidates.length < pageSize) {
          candidates.push(
            ...(this.selectPendingBelowPriority.all({
              ...input,
              priority: cursor.priority,
              limit: pageSize - candidates.length,
            }) as PendingCandidate[]),
          )
        }
      }
      if (candidates.length === 0) break

      const uncachedGroups = [
        ...new Set(
          candidates.flatMap(({ groupId }) =>
            groupId !== null && !availableSlots.has(groupId) ? [groupId] : [],
          ),
        ),
      ]
      if (uncachedGroups.length > 0) {
        const placeholders = uncachedGroups.map(() => '?').join(', ')
        const groups = prepare(
          this.db,
          `
            SELECT id, concurrency,
              (
                SELECT count(*) FROM walq_jobs AS active INDEXED BY walq_active_group
                WHERE active.queue = walq_groups.queue AND active.groupId = walq_groups.id
                  AND active.status = 'active' AND active.groupId IS NOT NULL
              ) AS activeCount
            FROM walq_groups
            WHERE queue = ? AND id IN (${placeholders})
          `,
        ).all(input.queue, ...uncachedGroups) as GroupCapacity[]

        for (const { id, concurrency, activeCount } of groups) {
          availableSlots.set(id, Math.max(0, concurrency - activeCount))
        }
        for (const id of uncachedGroups) {
          if (!availableSlots.has(id)) availableSlots.set(id, 0)
        }
      }

      const previousJobCount = jobs.length
      for (const candidate of candidates) {
        cursor = candidate

        if (candidate.groupId !== null) {
          const slots = availableSlots.get(candidate.groupId) ?? 0
          if (slots === 0) continue
          availableSlots.set(candidate.groupId, slots - 1)
        }

        jobs.push(
          this.acquire.get({
            id: candidate.id,
            leaseToken: randomUUID(),
            expiresAt,
          }) as ClaimedJob,
        )
        if (jobs.length === input.limit) break
      }

      if (jobs.length === input.limit || candidates.length < pageSize) break

      const claimedFromPage = jobs.length - previousJobCount
      pageSize =
        claimedFromPage === 0
          ? Math.min(maxClaimScanPageSize, Math.max(pageSize * 2, 32))
          : Math.min(input.limit - jobs.length, maxClaimScanPageSize)
    }

    return jobs
  }

  /** Group registration and deduplication checks must share an immediate transaction. */
  private insertOrGet(input: EnqueueInput): StoredJob {
    if (input.group !== undefined) {
      const existingGroup = this.findGroup.get({
        queue: input.queue,
        id: input.group.id,
      }) as { concurrency: number } | undefined
      if (existingGroup !== undefined && existingGroup.concurrency !== input.group.concurrency) {
        throw new TypeError(
          `Group "${input.group.id}" in queue "${input.queue}" already uses concurrency ${existingGroup.concurrency}`,
        )
      }
    }

    if (input.dedupe !== undefined) {
      const existing = this.findDeduplicated.get({
        queue: input.queue,
        dedupe: input.dedupe,
      }) as StoredJob | undefined
      if (existing !== undefined) return existing
    }

    if (input.group !== undefined) {
      this.insertGroup.run({
        queue: input.queue,
        ...input.group,
      })
    }

    return this.insert.get({
      ...input,
      dedupe: input.dedupe ?? null,
      groupId: input.group?.id ?? null,
      id: randomUUID(),
    }) as StoredJob
  }

  private assertAutocommit(): void {
    // Resolving before an outer transaction commits would violate the storage contract.
    if (this.db.inTransaction) throw new Error('Storage operations cannot run inside a transaction')
  }

  async enqueue(input: EnqueueInput): Promise<StoredJob> {
    this.assertAutocommit()
    const validated = validateEnqueue(input)
    return this.enqueueManyTransaction.immediate([validated])[0]!
  }

  async enqueueMany(inputs: EnqueueInput[]): Promise<StoredJob[]> {
    this.assertAutocommit()
    if (!Array.isArray(inputs)) throw new TypeError('inputs must be an array')

    const validated = Array.from(inputs, validateEnqueue)
    if (validated.length === 0) return []

    return this.enqueueManyTransaction.immediate(validated)
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

  async inspect(input: InspectInput): Promise<JobSnapshot | null> {
    this.assertAutocommit()
    const validated = validateInspect(input)
    return (this.inspectStatement.get(validated) as JobSnapshot | undefined) ?? null
  }

  async list(input: ListInput): Promise<JobSnapshot[]> {
    this.assertAutocommit()
    validateObject(input, 'input')
    text(input.queue, 'queue')
    if (!jobStatuses.includes(input.status)) {
      throw new TypeError('status must be a supported job status')
    }
    integer(input.limit, 'limit', 1)

    return this.listStatements[input.status].all(input) as JobSnapshot[]
  }

  async retry(input: RetryInput): Promise<boolean> {
    this.assertAutocommit()
    const validated = validateInspect(input)
    integer(input.now, 'now')
    return this.retryTransaction.immediate(validated, input.now)
  }

  async cancel(input: CancelInput): Promise<boolean> {
    this.assertAutocommit()
    const validated = validateInspect(input)
    integer(input.now, 'now')
    return this.cancelStatement.run({ ...validated, now: input.now }).changes === 1
  }

  async reschedule(input: RescheduleInput): Promise<boolean> {
    this.assertAutocommit()
    const validated = validateInspect(input)
    integer(input.availableAt, 'availableAt')
    return (
      this.rescheduleStatement.run({
        ...validated,
        availableAt: input.availableAt,
      }).changes === 1
    )
  }

  async remove(input: RemoveInput): Promise<boolean> {
    this.assertAutocommit()
    const validated = validateInspect(input)
    return this.removeStatement.run(validated).changes === 1
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
