# Storage contract

`packages/core/src/storage.ts` defines the adapter boundary. Its interface and
related types are exported from `@walq/core/storage`, separately from the main
package entry point. All operations are asynchronous so local SQLite and remote
adapters can implement the same contract.

## Scope and guarantees

- Delivery is at-least-once, subject to the attempt limit. Execution and external
  side effects are not exactly-once.
- A job has at most one current lease. Concurrent claims must not issue two live
  leases for the same job.
- A worker can continue executing after its lease expires. Tokens protect queue
  state, not external effects; handlers should tolerate repeated execution.
- Successful mutations are persisted before the operation resolves. Database or
  transport failures reject the promise; they are not `lease_lost` results.
- A transport error may leave the caller uncertain whether a mutation committed.
  In particular, retrying enqueue can create another job: idempotent enqueue is
  not provided.

The contract does not include connections, migrations, polling, retry policies,
retention, cancellation, events, or a public Queue API.

## Values and inputs

- All timestamps are finite, nonnegative safe-integer Unix milliseconds. Durations
  are positive safe-integer milliseconds; computed expiry must also be safe.
- Callers provide `now`. Each operation uses that value consistently. Distributed
  callers must use sufficiently synchronized clocks; storage does not establish
  a shared clock or substitute its own time.
- `limit` and `attempts` are positive safe integers.
- Queue and job names are nonempty strings. Queue names are matched exactly.
- Payload is a serialized JSON value. Serialization belongs above storage.
- IDs and lease tokens are opaque strings. Storage generates unique job IDs and
  a fresh token for every successful claim; a job's token must never be reused.
- Adapters must reject invalid inputs without mutation. Error classes and
  messages are not standardized yet.

`StoredJob` is a snapshot of job metadata. `ClaimedJob` additionally carries the
lease credentials. No method is provided for querying arbitrary jobs yet.

## Enqueue

`enqueue` creates and returns a job with:

- a generated ID;
- the supplied queue, name, payload, availableAt, and attempts;
- `createdAt = now`, `status = pending`, `attemptsMade = 0`, and `error = null`.

An availableAt in the past is valid. Every call creates an independent job;
matching names or payloads do not cause deduplication.

## Claim and expiration

`claim` operates only on its specified queue:

1. Recover active jobs whose `expiresAt <= now`. With attempts remaining, they
   become eligible for immediate retry, without backoff. Set `availableAt` to
   the expired lease's expiry. With no attempts remaining, mark them failed.
   Invalidate the old lease in either case. Preserve the last handler error.
2. Select pending jobs with `availableAt <= now` and
   `attemptsMade < attempts`, ordered by availableAt ascending, then ID ascending
   using binary string order as a stable tie-breaker.
3. For each selected job, atomically change status to active, increment attemptsMade
   by one, and issue a fresh token with `expiresAt = now + leaseDuration`.
4. Return the claimed snapshots in selection order, containing the incremented
   attemptsMade and lease credentials.

Return at most limit jobs; an empty result is valid. Concurrent callers may
receive smaller batches. Strict global FIFO across workers is not guaranteed.
Eligibility checks and acquisition must be atomic with respect to other claims
and lease mutations. Recovery must likewise conditionally check the current
lease so it cannot overwrite a concurrent heartbeat or completion. The entire
batch need not be one transaction; individual job claims must be atomic.

Recovery also applies to expired jobs with exhausted attempts, even when no job
can be returned. The limit bounds claims, not expiration recovery. State changes
happen on a subsequent claim for that queue, not automatically as time passes.

An attempt counts assignment, not handler invocation:

```text
enqueue         attemptsMade = 0
claim           attemptsMade = 1
lease expires   attemptsMade = 1
claim again     attemptsMade = 2
fail + retry    attemptsMade = 2
```

A crash before the handler starts still consumes an attempt. `attempts = 1`
permits no retry, including recovery after a worker crash.

## Lease mutations

`complete`, `fail`, and `heartbeat` atomically require all of:

- the job exists and is active;
- the supplied token matches its current lease;
- its current `expiresAt > now`.

If any condition fails, return `lease_lost` without changing anything. This
includes an expired lease that has not yet been recovered and a repeated
completion call. Otherwise apply the mutation and return `applied`.

### Complete

Set status to completed and invalidate the lease. Preserve attemptsMade and the last
handler error, if any. Results returned by handlers are not stored in this version.

### Fail

Record the supplied error without incrementing attemptsMade:

- If retryAt is non-null and attemptsMade is less than attempts, set status to
  pending and availableAt to retryAt.
- Otherwise set status to failed. The attempt limit overrides a retry request.

Invalidate the lease in either case. A retryAt at or before now is valid and
allows immediate retry. Computing backoff and deciding whether an error is
retryable belong above storage. `applied` means the failure was recorded, not
necessarily that a retry was scheduled.

### Heartbeat

Set expiry to `max(current expiresAt, now + leaseDuration)`. Keep the same token,
status, attemptsMade, and other job metadata. Heartbeat never shortens a lease and
cannot revive an expired one.

## State transitions

| Operation                                     | From                             | To        |
| --------------------------------------------- | -------------------------------- | --------- |
| enqueue                                       | absent                           | pending   |
| claim                                         | pending, due, attempts remaining | active    |
| complete                                      | active, live matching lease      | completed |
| fail with retry and attempts remaining        | active, live matching lease      | pending   |
| fail without retry or with attempts exhausted | active, live matching lease      | failed    |
| expiration recovery with attempts remaining   | active, expired                  | pending   |
| expiration recovery with attempts exhausted   | active, expired                  | failed    |
| heartbeat                                     | active, live matching lease      | active    |

Completed and failed jobs are terminal. Retention and deletion are outside this
contract; these operations do not delete jobs.
