# Storage contract

`packages/core/src/storage.ts` defines the asynchronous adapter boundary, exported
from `@walq/core/storage` (not the main entry point). It lets SQLite and remote
adapters implement the same contract. The higher-level `Queue` API is documented
in [`packages/core/README.md`](../packages/core/README.md).

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
  or nonnegative safe integers (`maxAge` in milliseconds). Queue and job names are
  nonempty strings; queue matching is exact. Data is serialized JSON, with
  serialization handled above storage.
- IDs and lease tokens are opaque strings. Storage generates unique job IDs and
  a fresh, never-reused token for every successful claim. Invalid inputs must be
  rejected without mutation; error classes and messages are unspecified.
- `StoredJob` contains queue metadata and serialized data. `JobSnapshot` adds
  `finishedAt`, which is null unless the job is terminal. It has no `startedAt`
  field; active jobs do not record when a handler started. `ClaimedJob` also
  contains lease credentials. Inspection and listing do not expose those
  credentials.

The contract covers low-level job-state transitions, including manual retry and
cancellation; it does not prescribe automatic retry/backoff policy, polling,
events, connection ownership, or schema migrations. `claimQueues` is optional;
see [Grouped claim](#grouped-claim).

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

## Inspection and listing

`inspect({ queue, id })` returns the snapshot for that ID in that exact queue, or
`null` if it is absent or belongs to another queue. `list({ queue, status, limit })`
returns at most `limit` snapshots from that exact queue and required status. The
storage-level limit is a positive safe integer; the public `Queue.list` API
applies its own default and maximum. Supported statuses are exactly `pending`,
`active`, `completed`, `failed`, and `cancelled`—there is no `waiting` status.

Listing is deterministic, with a status-specific order and binary ID tie-break:

- `pending`: `availableAt` ascending, then ID ascending.
- `active`: `expiresAt` ascending, then ID ascending.
- `completed`, `failed`, and `cancelled`: `finishedAt` descending, then ID
  descending.

Inspection and listing are snapshots, not reservations. A concurrent mutation
can change or remove a row immediately afterward. In particular, automatic
retention may delete completed or failed rows between queries. Expired active
leases remain recorded as active until a later `claim` for that queue recovers
them; merely inspecting, listing, or waiting does not recover a lease.

## Manual job controls

These queue-scoped mutations return `true` when the matching job accepts the
operation (including rescheduling to its current time). They return `false` for
an absent ID, a different queue, or a job in a state that does not permit the
operation. Invalid inputs and storage errors reject rather than returning `false`.

- **Retry:** `retry({ queue, id, now })` changes a failed job to pending and makes
  it available immediately (`availableAt = now`). It clears `finishedAt` but
  preserves `attemptsMade` and the last `error`. If attempts remain, `attempts`
  is unchanged. If already exhausted, increase `attempts` to
  `attemptsMade + 1`, granting exactly one more claim without resetting the
  attempt history. If that increase cannot fit in a safe integer, reject without
  mutation.
- **Cancel:** `cancel({ queue, id, now })` accepts pending jobs only and changes
  one to `cancelled`, setting `finishedAt = now`. It does not reset attempt
  history or the error. Cancellation is terminal and is not undone by retry.
- **Reschedule:** `reschedule({ queue, id, availableAt })` accepts pending jobs
  only and replaces their absolute availability time. A past time is valid and
  makes the job immediately eligible for a claim.
- **Remove:** `remove({ queue, id })` physically deletes any matching job except
  an active one. This includes pending, completed, failed, and cancelled jobs.
  Active jobs cannot be removed even if their lease is expired but has not yet
  been recovered by a claim.

The public `Queue.reschedule` API takes exactly one of a nonnegative `delay` or
absolute `runAt`, both in milliseconds; storage receives the resulting absolute
`availableAt`.

## Claim and expiration

`claim` only considers its specified queue. It recovers all expired active jobs
(`expiresAt <= now`), even if the claim limit is reached or no job is returned:
with attempts remaining, make them pending at the old `expiresAt` for immediate
retry; otherwise mark them failed. Invalidate the old lease either way and
preserve the last handler error. Recovery must conditionally verify the current
lease so it cannot overwrite a concurrent heartbeat or completion. Expiration
alone does not change persisted state; recovery occurs on a subsequent claim.

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
before handler start consumes an attempt; `attempts = 1` allows no retry,
including after a crash.

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
  otherwise mark failed (the attempt limit overrides retry). Invalidate the
  lease. `retryAt <= now` is valid. Backoff and retryability decisions belong
  above storage; `applied` means recorded, not necessarily retried.
- **Heartbeat:** set expiry to `max(current expiresAt, now + leaseDuration)`;
  retain token and all other metadata. It never shortens or revives a lease.

Completed, failed, and cancelled jobs have `finishedAt` set to the operation's
supplied `now`; pending and active jobs have `finishedAt = null`. No state stores
a handler start time.

## Cleanup and retention

Every adapter implements `cleanup`, which deletes only completed and failed
jobs from one exact queue; pending, active, and cancelled jobs are never touched.
Input specifies `queue`, `now`, positive `limit`, and independent `count`/`maxAge`
bounds for each cleaned status. `count` keeps the newest N rows (`0` keeps
none); `maxAge` is a maximum age in milliseconds. Either bound may be `null` to
disable it. A row is eligible if it is beyond the newest `count` rows **or** has
`finishedAt < now - maxAge`; bounds combine as a union. The age comparison is
strict, the cutoff clamps to zero on underflow, and two null bounds keep all
rows.

Per call, consider only that queue's completed and failed rows. For each status,
order newest first by `finishedAt` descending then binary ID descending; delete
oldest eligible rows first by `finishedAt` ascending then binary ID ascending, up
to `limit` across both statuses, as one atomic mutation.
This makes repeated bounded calls converge on the retained newest rows. Return
`{ removed, more }`; `more` indicates another call may find eligible rows and
may be true even if none remain, so callers repeat until false.

Work must be proportional to `limit` and retention bounds, not total history.
Cleanup is idempotent and restart-safe: later calls discover leftovers after a
crash. It is separate from `complete`/`fail`, so acknowledgement does not wait
for maintenance. The public worker schedules cleanup asynchronously according
to its retention policy; terminal snapshots can therefore disappear after
`complete` or terminal `fail`, and may disappear between `get` or `list` calls.
Cancelled jobs are excluded from automatic retention and persist until explicitly
removed. Deletion frees pages for reuse but need not shrink the database file;
cleanup must not run `VACUUM`. Callers schedule cleanup and choose batch limits;
no other storage method deletes jobs except the explicit `remove` operation.

## State transitions

| Operation                                   | From                             | To        | Notes                                        |
| ------------------------------------------- | -------------------------------- | --------- | -------------------------------------------- |
| `enqueue`, `enqueueMany`                    | absent                           | pending   | `attemptsMade = 0`                           |
| `claim`                                     | pending, due, attempts remaining | active    | increments `attemptsMade`                    |
| `complete`                                  | active, live matching lease      | completed | invalidates lease                            |
| `fail` with retry and attempts remaining    | active, live matching lease      | pending   | sets retry time and error                    |
| `fail` otherwise                            | active, live matching lease      | failed    | sets finish time and error                   |
| Expiration recovery with attempts remaining | active, expired                  | pending   | available at old expiry; preserves error     |
| Expiration recovery with attempts exhausted | active, expired                  | failed    | preserves error                              |
| `retry`                                     | failed                           | pending   | immediate; retains attempt history and error |
| `cancel`                                    | pending                          | cancelled | records finish time                          |
| `reschedule`                                | pending                          | pending   | changes availability                         |
| `remove`                                    | any except active                | absent    | physical deletion                            |
| `heartbeat`                                 | active, live matching lease      | active    | extends but never shortens lease             |
| `cleanup`                                   | completed or failed              | absent    | only if beyond retention bounds              |
