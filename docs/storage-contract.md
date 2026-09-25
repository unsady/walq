# Storage contract

`packages/core/src/storage.ts` defines the async adapter boundary, exported from
`@walq/core/storage` (not the main entry point). It lets local SQLite and remote
adapters implement the same contract.

## Scope and shared rules

- Delivery is at-least-once, subject to the attempt limit. Execution and external
  side effects are not exactly-once. A worker may continue after lease expiry;
  tokens protect queue state, not external effects, so handlers should tolerate
  repeated execution.
- A job has at most one live lease. Claims and lease mutations must coordinate
  atomically to prevent conflicting state changes.
- Successful mutations are persisted before resolving. Database/transport errors
  reject; they are not `lease_lost`. A transport error can leave commit status
  uncertain. Retrying `enqueue` may create duplicates; enqueue is not idempotent.
- All timestamps are finite, nonnegative safe-integer Unix milliseconds;
  durations are positive safe-integer milliseconds, and computed expiries must
  remain safe. Callers supply `now`, used consistently per operation. Storage
  does not establish a shared clock; distributed callers need synchronized clocks.
- `limit` and `attempts` are positive safe integers. Retention bounds are `null`
  or nonnegative safe integers (`maxAge` in milliseconds). Queue and job names
  are nonempty strings; queue matching is exact. Data is serialized JSON, with
  serialization handled above storage.
- IDs and lease tokens are opaque strings. Storage generates unique job IDs and
  a fresh, never-reused token for every successful claim. Invalid inputs must be
  rejected without mutation; error classes and messages are unspecified.
- `StoredJob` is a metadata snapshot; `ClaimedJob` also contains lease
  credentials. Arbitrary-job queries are not provided.

The contract excludes connections, migrations, polling, retry policies,
cancellation, events, and a public Queue API. `claimQueues` is optional; see
[Grouped claim](#grouped-claim).

## Enqueue

`enqueue` creates an independent job and returns its `StoredJob`: generated ID;
provided queue, name, data, `availableAt`, and `attempts`; plus `createdAt = now`,
`status = pending`, `attemptsMade = 0`, and `error = null`. Past `availableAt` is
valid. Matching names or data do not deduplicate.

`enqueueMany(inputs)` applies the same rules in input order. Empty input returns
`[]`. Validate the entire batch before mutation and commit all inserts atomically
(all or none). It must not resolve before commit or run inside a caller-managed
transaction. Each input is independent. As with any mutation, a transport error
may leave the caller unsure whether the batch committed.

## Claim and expiration

`claim` only considers its specified queue. It recovers all expired active jobs
(`expiresAt <= now`), even if the claim limit is reached or no job is returned:
with attempts remaining, make them pending at the old `expiresAt` for immediate
retry; otherwise mark them failed. Invalidate the old lease either way and
preserve the last handler error. Recovery must conditionally verify the current
lease so it cannot overwrite a concurrent heartbeat or completion. State changes
occur on a subsequent claim, not automatically as time passes.

Then select pending jobs with `availableAt <= now` and `attemptsMade < attempts`,
ordered by `availableAt` ascending, then ID ascending in binary string order.
Atomically make each selected job active, increment `attemptsMade`, and assign a
fresh token with `expiresAt = now + leaseDuration`. Return at most `limit`
snapshots in selection order, with the incremented count and lease credentials.
An empty or smaller-than-requested batch is valid; strict global FIFO across
workers is not guaranteed. Eligibility and acquisition must be atomic against
other claims and lease mutations. Individual claims must be atomic, but the whole
batch need not be one transaction.

An attempt counts assignment, not handler invocation: enqueue starts at 0, each
claim increments it, and neither expiration nor `fail` increments it. A crash
before handler start consumes an attempt; `attempts = 1` allows no retry, including
after a crash.

## Grouped claim

`claimQueues({ requests })` is optional and returns `ClaimedJob[][]`, with
`results[k]` corresponding to `requests[k]`. Each request has standalone `claim`
semantics. Requests run in array order, so repeated queues see earlier requests'
changes and cannot claim a job twice. Empty input returns `[]` without touching
storage. Validate all requests before mutation (and before opening a transaction).
An adapter may transact the whole batch, rolling all requests back on error, or
commit requests independently; callers must not rely on cross-request atomicity.
If unsupported, callers needing grouped claims must call `claim` per request.

## Lease mutations

`complete`, `fail`, and `heartbeat` atomically require that the job exists, is
active, has the supplied current token, and has `expiresAt > now`. Otherwise
return `lease_lost` without mutation—including for an expired but unrecovered
lease or repeated completion. On success return `applied`.

- **Complete:** mark completed and invalidate the lease; preserve `attemptsMade`
  and the last handler error. Handler results are not stored.
- **Fail:** record the supplied error without incrementing attempts. If
  `retryAt != null` and attempts remain, set pending with `availableAt = retryAt`;
  otherwise mark failed (the attempt limit overrides retry). Invalidate the lease.
  `retryAt <= now` is valid. Backoff and retryability decisions belong above
  storage; `applied` means recorded, not necessarily retried.
- **Heartbeat:** set expiry to `max(current expiresAt, now + leaseDuration)`;
  retain token and all other metadata. It never shortens or revives a lease.

## Cleanup

Every adapter implements `cleanup`, which deletes only terminal jobs (completed
or failed) from one exact queue; pending and active jobs are never touched. Input
specifies `queue`, `now`, positive `limit`, and independent `count`/`maxAge`
bounds for each terminal status. `count` keeps the newest N rows (`0` keeps
none); `maxAge` is a maximum age in milliseconds. Either bound may be `null` to
disable it. A row is eligible if it is beyond the newest `count` rows **or** has
`finishedAt < now - maxAge`; bounds combine as a union. The age comparison is
strict, the cutoff clamps to zero on underflow, and two null bounds keep all rows.

Per call, consider only the queue's terminal rows and order each status newest
first by finish time with a stable tie-breaker. Delete oldest eligible rows first,
up to `limit` across both statuses, as one atomic mutation. This makes repeated
bounded calls converge on the retained newest rows. Return `{ removed, more }`;
`more` indicates another call may find eligible rows and may be true even if none
remain, so callers repeat until false.

Work must be proportional to `limit` and retention bounds, not total history.
Cleanup is idempotent and restart-safe: later calls discover leftovers after a
crash. It is separate from `complete`/`fail`, so acknowledgement does not wait for
maintenance. Deletion frees pages for reuse but need not shrink the database file;
cleanup must not run `VACUUM`. Callers schedule cleanup and choose batch limits;
no other method deletes jobs.

## State transitions

| Operation                                   | From                             | To        |
| ------------------------------------------- | -------------------------------- | --------- |
| `enqueue`, `enqueueMany`                    | absent                           | pending   |
| `claim`                                     | pending, due, attempts remaining | active    |
| `complete`                                  | active, live matching lease      | completed |
| `fail` with retry and attempts remaining    | active, live matching lease      | pending   |
| `fail` otherwise                            | active, live matching lease      | failed    |
| Expiration recovery with attempts remaining | active, expired                  | pending   |
| Expiration recovery with attempts exhausted | active, expired                  | failed    |
| `heartbeat`                                 | active, live matching lease      | active    |

Completed and failed are terminal. All methods except `cleanup` retain jobs;
adapters record when a job became terminal for retention ordering.
