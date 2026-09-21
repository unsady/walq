# walq

Typed queue API for walq storage adapters. Walq is ESM-only and requires Node.js 22 or newer.

```sh
pnpm add @walq/core @walq/better-sqlite3 better-sqlite3
```

```ts
import Database from 'better-sqlite3'
import { betterSqlite3 } from '@walq/better-sqlite3'
import { Queue } from '@walq/core'

const db = new Database('queue.sqlite')
const queue = new Queue<{ name: string }>('greetings', {
  storage: betterSqlite3(db),
})

const worker = queue.process(async ({ name }) => {
  console.log(`Hello, ${name}!`)
})

await queue.add({ name: 'Ada' })
await worker.close()
db.close()
```

Storage adapter authors can import the public contract from `@walq/core/storage`.

## API

- `new Queue(name, { storage, attempts?, onError?, retention? })` creates a queue. `attempts` defaults to 1.
- `retention` controls terminal-job cleanup: `{ completed?, failed? }`. Each status is a count, `null` to keep every job of that status, or a rule object `{ count?, maxAge? }` where `maxAge` is milliseconds. Omitted statuses default to `completed: 0` and `failed: 100`.
- `queue.add(data)` serializes the data and enqueues a job.
- `queue.process(handler, { concurrency? })` registers the queue with the shared poller. `concurrency` defaults to 1.
- Handlers receive `(data, context)`. Context contains `signal`, `jobId`, and the current `attempt`.
- `worker.close()` stops new claims and waits for active handlers without aborting them.

Queues created with the same `Storage` instance share one queue-aware poller. Ready queues are polled in rotating order, and adapters with `claimQueues` can claim for one sweep in a single call, split into a few transactions when the sweep covers many queues. A separate `Storage` instance has its own coordinator. One queue name can be processed by only one worker per `Storage`; registering a second worker for the same name is rejected.

The poller checks empty queues once per second and wakes on `add()` and on handler completion. Active jobs use a 30-second lease with a heartbeat every 10 seconds. Handler failures retry immediately while attempts remain.

Handlers run concurrently as asynchronous tasks in the current Node.js process. They are not worker threads. The context signal aborts when the job loses its lease, but handlers must stop cooperatively. Delivery is at-least-once, so handlers must tolerate repeated execution.

## Retention

Terminal jobs are removed asynchronously after `complete()` or a terminal
`fail()` commits. Retention is per queue and per status: by default every
completed job is removed and the newest 100 failures are kept for diagnostics.
Pass `retention` to keep more or fewer:

```ts
const queue = new Queue('email', {
  storage,
  // Count shorthand: keep the newest 10 completed jobs and 1,000 failures.
  retention: { completed: 10, failed: 1_000 },
})
```

A status can also be a rule object with independent `count` and `maxAge` bounds,
where `maxAge` is milliseconds:

```ts
const queue = new Queue('email', {
  storage,
  retention: {
    completed: 0,
    failed: { count: 1_000, maxAge: 7 * 24 * 60 * 60 * 1_000 },
  },
})
```

`count` keeps that many newest rows; an omitted `count` uses the status default
(completed `0`, failed `100`) and `count: null` disables the count bound. `maxAge`
removes rows finished before `now - maxAge`; an omitted or null `maxAge` disables
the age bound. A row is eligible when it exceeds either bound, so both are upper
limits and the stricter one wins. The cutoff is strict: a row finished exactly at
`now - maxAge` is retained. Cleanup passes the worker's current time as `now`, so
every age bound in a pass is evaluated against one clock reading.

A `null` status (or `count: null, maxAge: null`) keeps every job of that status,
for example `retention: { completed: null, failed: null }` to disable cleanup
entirely.

Cleanup runs in bounded batches, at most one pass per second per queue, and
yields to queue work between batches while eligible rows remain. A deferred task
schedules passes after claim passes (which can recover expired jobs) and after
terminal transitions, so `process()` and handler acknowledgements never run
database maintenance themselves. An idle queue pays at most one pass per
throttle interval. `worker.close()` stops scheduling new passes and waits for a
batch already in flight.

Every storage adapter implements bounded `cleanup`. Because cleanup is
asynchronous, terminal rows may remain
visible after `complete()` and can survive a process crash until a later worker
starts or finishes another job.

## Error reporting

`onError(err, ctx)` receives errors the queue would otherwise swallow. `ctx` is discriminated by `operation`:

- `claim` — a claim failed before any job was acquired. Grouped claim failures are reported once per affected queue.
- `cleanup` — a bounded cleanup pass failed for the queue. Cleanup is retried after the next terminal transition.
- `heartbeat`, `complete`, `fail` — a lease mutation for a claimed job failed.
- `handler` — the handler threw or rejected.

Every context carries `queue`; job-scoped operations also carry `jobId` and `attempt`. Handler contexts add `attemptsExhausted`, derived from the claimed job's attempt limits. It means no retry can be issued; it does not confirm that a terminal failure was committed to storage.

`lease_lost` is a normal protocol outcome, not an error, and is never reported. Without `onError`, errors are written to `console.error` with the same context. The callback may be sync or async; errors it throws or rejects are logged safely and never affect polling, acknowledgement, or shutdown.
