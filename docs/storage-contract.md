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
cancellation, events, or a public Queue API. `claimQueues` is an optional adapter
capability; see [Grouped claim](#grouped-claim) and [Cleanup](#cleanup).

## Values and inputs

- All timestamps are finite, nonnegative safe-integer Unix milliseconds. Durations
  are positive safe-integer milliseconds; computed expiry must also be safe.
- Callers provide `now`. Each operation uses that value consistently. Distributed
  callers must use sufficiently synchronized clocks; storage does not establish
  a shared clock or substitute its own time.
- `limit` and `attempts` are positive safe integers. Retention counts are `null`
  or nonnegative safe integers.
- Queue and job names are nonempty strings. Queue names are matched exactly.
- Data is a serialized JSON value. Serialization belongs above storage.
- IDs and lease tokens are opaque strings. Storage generates unique job IDs and
  a fresh token for every successful claim; a job's token must never be reused.
- Adapters must reject invalid inputs without mutation. Error classes and
  messages are not standardized yet.

`StoredJob` is a snapshot of job metadata. `ClaimedJob` additionally carries the
lease credentials. No method is provided for querying arbitrary jobs yet.

## Enqueue

`enqueue` creates and returns a job with:

- a generated ID;
- the supplied queue, name, data, availableAt, and attempts;
- `createdAt = now`, `status = pending`, `attemptsMade = 0`, and `error = null`.

An availableAt in the past is valid. Every call creates an independent job;
matching names or data do not cause deduplication.

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

## Grouped claim

`claimQueues` is an optional adapter capability for callers that need to claim
from several queues at once. It accepts `{ requests: ClaimInput[] }` and returns
`ClaimedJob[][]`, where `results[k]` belongs to `requests[k]`.

- Each request has exactly the semantics of a standalone `claim`, including
  recovery, ordering, limit, and fresh lease tokens.
- Requests are applied in array order. Two requests for the same queue observe
  each other, so the second sees only work the first left behind and no job is
  claimed twice.
- An empty `requests` array is valid and returns an empty result without
  touching storage.
- An adapter may run the whole batch in one transaction. When it does, an error
  in any request rolls back the whole batch; when it does not, requests may
  commit independently. Callers must not depend on cross-request atomicity.
- Invalid requests are rejected without mutation. Adapters should validate the
  whole batch before opening a transaction.
- Absence of this method is not an error: a caller that needs grouped claims
  must fall back to one `claim` call per request.

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

## Cleanup

`cleanup` provides bounded terminal-job retention. Every adapter must implement
it. The method removes only terminal jobs of one queue and never touches pending
or active rows. The input carries:

- `queue` — the exact queue name to clean;
- `retention.completed` and `retention.failed` — how many jobs of each status to
  keep, where `0` makes every terminal job of that status eligible and `null`
  keeps all of them;
- `limit` — a positive bound on rows deleted by this call.

Within one call the adapter must:

1. Consider only rows of the requested queue with one of the two terminal
   statuses.
2. Order each status by the time the job finished, newest first, with a stable
   tie-breaker.
3. Delete only rows beyond the first `retention[status]` rows of that order.
   A bounded call deletes from the oldest eligible rows first, so repeated calls
   converge on the newest retained rows.
4. Delete at most `limit` rows across both statuses as one atomic mutation.

The result is `{ removed, more }`. `removed` is the number of rows deleted by
this call. `more` reports that another call may still find eligible rows; it may
be `true` even when nothing remains, so a caller repeats until it sees `false`.

One call must stay proportional to `limit` and the retention counts rather than
to the size of the terminal history, so draining a large backlog remains linear
in the number of deleted rows.

Cleanup is idempotent and restart-safe: leftover terminal rows are discovered by
a later call, including after a process crash. It is separate from `complete`
and `fail`, so successful handler acknowledgement never waits for maintenance.
Deletion frees pages for reuse but does not necessarily shrink the database
file; adapters must not run `VACUUM` as part of cleanup.

Callers choose when to run cleanup and how to bound and space batches. The
contract does not schedule cleanup or delete any rows outside this method.

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

Completed and failed jobs are terminal. `enqueue`, `claim`, `complete`, `fail`,
and `heartbeat` never delete jobs; deletion happens only through
[cleanup](#cleanup). Adapters record when a job
became terminal so retention can keep the most recently finished jobs.
