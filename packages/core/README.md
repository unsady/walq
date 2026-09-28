# @walq/core

Typed queue API for walq storage adapters. ESM-only; requires Node.js 22+.

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
queue.process(async ({ name }) => console.log(`Hello, ${name}!`))
await queue.add({ name: 'Ada' })
```

Storage adapter authors import `Storage` from `@walq/core/storage`.

## API

- `new Queue(name, { storage, attempts?, backoff?, onError?, retention? })`: `attempts` defaults to `1`; retries after handler errors are immediate unless backoff is configured. Retention defaults to `{ completed: 0, failed: 100 }`.
- `queue.add(data, options?)`: enqueue JSON-serializable data; omitted options make it immediately available. `options` accepts `delay` or `runAt`, not both, a signed safe-integer `priority` (default `0`), an optional nonempty `dedupe` key, and an optional `group` (a nonempty ID string or `{ id, concurrency? }`; concurrency must be a positive safe integer and defaults to `1`). A matching persisted key in the same queue returns the existing job ID unchanged, regardless of status; payload, options, and group membership are not replaced. Keys are released only when the job is physically removed, including by retention cleanup. Group concurrency is fixed per queue/group ID on first insertion and conflicting values reject; the configuration persists even after all its jobs are removed. Grouped claims enforce the active-job limit across workers and SQLite connections; saturated groups do not block other eligible jobs. Higher-priority due jobs are claimed first; a past `runAt` is immediately eligible.
- `queue.addMany(items)`: enqueue `{ data, options? }` items atomically, in input order. All items are validated before enqueue; repeated dedupe keys resolve to the same first or existing job ID in their result positions. An empty batch returns `[]`. Delayed items share one clock reading.
- `queue.schedule(data, { id, every })` or `queue.schedule(data, { id, cron })`: upsert a durable repeating schedule scoped by queue and ID. `every` is a positive safe-integer interval in milliseconds; `cron` is a cron-parser expression interpreted in UTC. Identical data and options preserve `nextRunAt`; changing either recalculates from the update time. `queue.getSchedule(id)` returns `{ id, data, nextRunAt, every }` or `{ id, data, nextRunAt, cron }`, or `null`; `queue.removeSchedule(id)` returns whether a definition was removed. Updating or removing a schedule does not alter jobs already created from it.
- `queue.process(handler, { concurrency? })`: starts processing; concurrency defaults to `1`. The handler receives `(data, { signal, jobId, attempt })`.
- `queue.processMany(handler, { batch?, concurrency? })`: starts batch processing. `batch` defaults to `10` and limits jobs per handler call; `concurrency` defaults to `1` and limits simultaneous batch calls. Each item is `{ data, context }`, with an independent `signal`, `jobId`, and `attempt`. A successful call completes each still-owned job independently. If the handler throws or rejects, every job in that batch is independently reported and failed/retried using the queue's normal attempt and backoff policy; use `process()` when jobs need independent handler outcomes.
- `worker.close()`: stop new claims and wait for active handlers; it does not abort them.
- `queue.get(id)` returns a snapshot or `null`. `queue.list({ status, limit? })` lists `pending`, `active`, `completed`, `failed`, or `cancelled` jobs (`limit` defaults to 100; range 1–1,000). Snapshots contain `id`, `data`, `status`, `attempt` (claims made), `attempts` (limit), `createdAt`, `availableAt`, `priority`, `finishedAt`, and `error`; no lease credentials. Expired active jobs remain active until a claim recovers them. Pending jobs list by availability, not priority; claims order due jobs by priority first.
- `queue.stats()` returns persisted counts for all five statuses, including zeroes. It does not recover expired leases, so those remain counted as active until a claim recovers them. Completed and failed counts reflect current retained rows, not lifetime totals.
- `queue.retry(id)` retries a failed job immediately. It preserves attempt history and error; if attempts are exhausted, it grants exactly one additional claim.
- `queue.cancel(id)` cancels pending jobs only. `queue.reschedule(id, { delay })` or `{ runAt }` changes availability of pending jobs only; exactly one value is required.
- `queue.remove(id)` removes any non-active job. These four lifecycle methods return `false` if the job is missing or in an incompatible state; invalid inputs and storage errors reject.

Queues using the same `Storage` instance share a queue-aware poller; only one worker per queue name may be registered on that instance. A separate storage instance has its own poller. While a worker polls, due schedules atomically create ordinary pending jobs and advance to the next occurrence. Missed occurrences are coalesced into one job on restart, with the next run after the current time. Existing storage adapters remain compatible, but must implement the optional schedule operations to use this API.

## Delivery and retries

A claim increments `attempt`; a crash after claiming consumes an attempt. Jobs are delivered at least once, so handlers must tolerate repetition. Active jobs have a 30-second lease, renewed every 10 seconds. The signal aborts if that job's lease is lost, but handlers must stop cooperatively. In `processMany()`, each job keeps an independent lease and signal while sharing the batch handler call. Expired leases are recovered on the next claim for that queue; expiry alone does not change stored state.

Configure optional handler-failure backoff:

```ts
const queue = new Queue('email', {
  storage,
  attempts: 5,
  backoff: { type: 'exponential', delay: 1_000, jitter: 0.2 },
})
```

Backoff can be `fixed` or `exponential`; `delay` is nonnegative safe-integer milliseconds and `jitter` is from 0 to 1 (default 0), reducing the base delay by a random fraction up to that value. It applies to handler failures only; lease-expiry retries are immediate. `attempts` includes the initial claim; exhausted attempts are recorded as failures without another retry.

```ts
const added = await queue.add({ name: 'Grace' })
const failed = await queue.list({ status: 'failed', limit: 20 })
if (failed[0]) await queue.retry(failed[0].id)

const job = await queue.get(added.id)
if (job?.status === 'pending') await queue.reschedule(job.id, { delay: 60_000 })
```

## Retention and errors

Completed and failed jobs are cleaned asynchronously; cancelled jobs remain until removed. Configure `retention` per status as a count, `null` to keep all, or `{ count?, maxAge? }` in milliseconds. Defaults are 0 completed and 100 failed. In a rule object, omitted `count` uses that status default and omitted `maxAge` disables the age bound. A job is removed when it exceeds either bound, so cleanup can make snapshots disappear between reads:

```ts
const emailQueue = new Queue('email', {
  storage,
  retention: { completed: 10, failed: { count: 1_000, maxAge: 7 * 24 * 60 * 60 * 1_000 } },
})
```

`onError(error, context)` receives `claim`, `cleanup`, `schedule`, `heartbeat`, `complete`, `fail`, or `handler` errors; without it errors go to `console.error`. `lease_lost` is a normal result, not an error. Errors thrown by `onError` are logged and do not affect queue processing.
