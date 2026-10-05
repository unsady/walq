# @walq/core

Typed queue API for walq storage adapters. ESM-only; requires Node.js 22+.

```sh
pnpm add @walq/core @walq/sqlite
```

```ts
import { Queue } from '@walq/core'
import { createStorage } from '@walq/sqlite'

const storage = await createStorage({
  filename: './queue.sqlite',
  worker: true,
})

const queue = new Queue<{ name: string }>('greetings', {
  storage,
})

const worker = queue.process(async ({ name }) => {
  console.log(`Hello, ${name}!`)
})
await queue.add({ name: 'Ada' })

// On shutdown:
await worker.close()
await storage.close()
```

`worker` defaults to `false`. Set it to `true` to run SQLite in a dedicated thread; job handlers remain in the main thread. Worker mode is currently verified on Node.js only. See the [`@walq/better-sqlite3`](../better-sqlite3/README.md#managed-storage) and [`@walq/sqlite`](../sqlite/README.md#managed-storage) READMEs for connection settings and lifecycle details.

## Storage adapters

| Adapter                                               | Connection you can pass manually          | Managed connection       |
| ----------------------------------------------------- | ----------------------------------------- | ------------------------ |
| [`@walq/sqlite`](../sqlite/README.md)                 | `sqlite(db)` with `node:sqlite`           | `createStorage(options)` |
| [`@walq/better-sqlite3`](../better-sqlite3/README.md) | `betterSqlite3(db)` with `better-sqlite3` | `createStorage(options)` |

For a caller-owned connection:

```ts
import { DatabaseSync } from 'node:sqlite'

import { sqlite } from '@walq/sqlite'

const db = new DatabaseSync('./queue.sqlite')

db.exec(`
  PRAGMA journal_mode = WAL;
  PRAGMA synchronous = FULL;
  PRAGMA busy_timeout = 5000;
`)

const storage = sqlite(db)
```

Pass `storage` to `new Queue()` as above. Stop workers and await outstanding storage calls before closing `db` yourself. See the [feature examples](../../README.md#queue-features) for delays, deduplication, groups, batches, and schedules.

Storage adapter authors import `Storage` from `@walq/core/storage`; see the [storage contract](../../docs/storage-contract.md).

## API

- `new Queue(name, { storage, attempts?, backoff?, onError?, retention? })`: `attempts` defaults to `1`; handler-error retries are immediate unless backoff is configured.
- `queue.add(data, options?)`: enqueue JSON-serializable data, immediately available by default. Options: `delay` or `runAt` (not both), signed safe-integer `priority` (default `0`), nonempty `dedupe`, and `group` (a nonempty ID string or `{ id, concurrency? }`). A past `runAt` is immediately eligible.
- `queue.addMany(items)`: enqueue `{ data, options? }` items atomically in input order, validating the whole batch first. Repeated dedupe keys return the same job ID in each result position. Empty input returns `[]`; delayed items share one clock reading.
- `queue.schedule(data, { id, every })` or `{ id, cron }`: upsert a durable queue-scoped schedule. `every` is a positive safe-integer millisecond interval; `cron` uses cron-parser expressions in UTC. Identical data/options preserve `nextRunAt`; changes recalculate it from the update time. `getSchedule(id)` returns the definition with `nextRunAt`, or `null`; `removeSchedule(id)` returns whether it existed. Neither updates nor removal affect already-created jobs.
- `queue.process(handler, { concurrency? })`: starts processing; concurrency defaults to `1`. The handler receives `(data, { signal, jobId, attempt })`.
- `queue.processMany(handler, { batch?, concurrency? })`: process up to `batch` jobs per call (default `10`), with up to `concurrency` simultaneous calls (default `1`). Items are `{ data, context }`, each with its own `signal`, `jobId`, and `attempt`. Success completes still-owned jobs; rejection reports and fails/retries each job under the queue policy. Use `process()` for independent handler outcomes.
- `worker.close()`: stop new claims and wait for active handlers; it does not abort them.
- `queue.get(id)` returns a snapshot or `null`. `queue.list({ status, limit? })` lists `pending`, `active`, `completed`, `failed`, or `cancelled` jobs (`limit` defaults to 100; range 1–1,000). Snapshots contain `id`, `data`, `status`, `attempt` (claims made), `attempts` (limit), `createdAt`, `availableAt`, `priority`, `finishedAt`, and `error`; no lease credentials. Expired active jobs remain active until a claim recovers them. Pending jobs list by availability, not priority; claims order due jobs by priority first.
- `queue.stats()` returns persisted counts for all five statuses, including zeroes. Expired leases still count as active until recovered; terminal counts reflect retained rows, not lifetime totals.
- `queue.pause()` / `queue.resume()` durably disable/enable new claims for this queue, even before `process()`. Both are idempotent. Adds, active handlers, and expired-lease recovery continue while paused; new claims and schedule materialization stop. Resume wakes local workers; remote workers notice on their normal poll.
- `queue.retry(id)` retries a failed job immediately. It preserves attempt history and error; if attempts are exhausted, it grants exactly one additional claim.
- `queue.cancel(id)` cancels pending jobs only. `queue.reschedule(id, { delay })` or `{ runAt }` changes availability of pending jobs only; exactly one value is required.
- `queue.remove(id)` removes any non-active job. These four lifecycle methods return `false` if the job is missing or in an incompatible state; invalid inputs and storage errors reject.

Queues sharing a `Storage` instance share one poller and allow only one worker per queue name. Separate storage instances have separate pollers. While a worker polls, due schedules create ordinary jobs; missed occurrences coalesce into one job rather than a catch-up burst. Adapters without the optional schedule capability reject schedule API calls.

## Deduplication and groups

A persisted dedupe key reuses the existing job within its queue, regardless of status, without replacing payload, options, or group membership. Physical deletion, including retention cleanup, releases the key.

Group concurrency is a positive safe integer (default `1`), fixed per queue/group ID on first insertion; conflicting values reject even after all jobs are removed. Limits apply across workers and SQLite connections. Eligible groups and ungrouped jobs take round-robin turns; within each stream, due jobs follow priority, availability, then insertion order. Saturated groups do not block other work. See the [storage contract](../../docs/storage-contract.md) for exact ordering.

## Delivery and retries

Delivery is at least once; handlers must tolerate repetition. Every claim consumes an attempt, even if the handler never starts. Leases last 30 seconds and renew every 10 seconds. Losing a lease aborts that job's signal; handlers must stop cooperatively, including within batches. Expired leases recover on the next claim, not at expiry.

Configure optional processing-failure backoff:

```ts
const queue = new Queue('email', {
  storage,
  attempts: 5,
  backoff: {
    type: 'exponential',
    delay: 1_000,
    jitter: 0.2,
  },
})
```

Backoff can be `fixed` or `exponential`; `delay` is nonnegative safe-integer milliseconds and `jitter` is from 0 to 1 (default 0), reducing the base delay by a random fraction up to that value. It applies to payload parsing and handler failures; lease-expiry retries are immediate. `attempts` includes the initial claim; exhausted attempts are recorded as failures without another retry.

```ts
const added = await queue.add({ name: 'Grace' })
const failed = await queue.list({ status: 'failed', limit: 20 })

if (failed[0]) {
  await queue.retry(failed[0].id)
}

const job = await queue.get(added.id)

if (job?.status === 'pending') {
  await queue.reschedule(job.id, { delay: 60_000 })
}
```

## Retention and errors

Completed and failed jobs are cleaned asynchronously; cancelled jobs remain until removed. Configure `retention` per status as a count, `null` to keep all, or `{ count?, maxAge? }` in milliseconds. Defaults are 0 completed and 100 failed. In a rule object, omitted `count` uses that status default and omitted `maxAge` disables the age bound. A job is removed when it exceeds either bound, so cleanup can make snapshots disappear between reads:

```ts
const emailQueue = new Queue('email', {
  storage,
  retention: {
    completed: 10,
    failed: {
      count: 1_000,
      maxAge: 7 * 24 * 60 * 60 * 1_000,
    },
  },
  onError: (error, context) => {
    console.error(context, error)
  },
})
```

`onError(error, context)` receives `claim`, `cleanup`, `schedule`, `heartbeat`, `complete`, `fail`, `parse`, or `handler` errors; without it errors go to `console.error`. `parse` means a job's JSON payload could not be decoded before calling its handler; it includes the job ID, attempt, and `attemptsExhausted`, just like `handler`. `lease_lost` is a normal result, not an error. Errors thrown by `onError` are logged and do not affect queue processing.

Log the error object, not just its message, to keep its stack, `cause`, and driver diagnostics available. `onError` handles background processing errors; direct calls such as `queue.add()` and `createStorage()` reject their promises, so handle them with `try/catch` around `await`.

`job.error` stores diagnostic text, including the stack and a depth-bounded cause chain when present.
