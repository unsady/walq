# walq

**A small, lease-based job queue for SQLite.** Typed jobs, retries, batches, and repeating schedules without a separate queue service.

Walq is ESM-only. Jobs are delivered at least once; make handlers idempotent when repeating side effects is unsafe.

## Quick start

```sh
npm install @walq/core @walq/sqlite
```

```ts
import { Queue } from '@walq/core'
import { createStorage } from '@walq/sqlite'

const storage = await createStorage({
  filename: './queue.sqlite',
})

const queue = new Queue<{ name: string }>('greetings', {
  storage,
})

const worker = queue.process(async ({ name }) => {
  console.log(`Hello, ${name}!`)
})

await queue.add({ name: 'Ada' })

process.once('SIGINT', async () => {
  await worker.close()
  await storage.close()
})
```

`createStorage` opens the database, configures WAL and durability settings, and initializes the schema. Stop workers before closing storage. All durations below are in milliseconds.

## Choose a storage adapter

| Adapter                                                     | Driver                                      | Runtimes                                |
| ----------------------------------------------------------- | ------------------------------------------- | --------------------------------------- |
| [`@walq/sqlite`](packages/sqlite/README.md)                 | Built-in `node:sqlite`; no native npm addon | Node.js 22.16+, Bun 1.4.2+, Deno 2.9.7+ |
| [`@walq/better-sqlite3`](packages/better-sqlite3/README.md) | `better-sqlite3`; native addon              | Node.js 22+                             |

Both adapters support the same queue features and database format. Install one alongside `@walq/core`. `@walq/sqlite-common` is an internal shared implementation, not an adapter to install directly.

### Managed connection

Use `createStorage` when walq should open and close the connection. For `better-sqlite3`, change the import:

```sh
npm install @walq/core @walq/better-sqlite3 better-sqlite3
```

```ts
import { createStorage } from '@walq/better-sqlite3'

const storage = await createStorage({
  filename: './queue.sqlite',
  worker: true,
})
```

Both adapters accept `worker: true` to move SQLite work to a dedicated thread; job handlers stay in the main thread. The default is `false`. Worker mode is currently verified on Node.js only.

### Bring your own connection

With `node:sqlite`:

```ts
import { DatabaseSync } from 'node:sqlite'

import { Queue } from '@walq/core'
import { sqlite } from '@walq/sqlite'

const db = new DatabaseSync('./queue.sqlite')

db.exec(`
  PRAGMA journal_mode = WAL;
  PRAGMA synchronous = FULL;
  PRAGMA busy_timeout = 5000;
`)

const storage = sqlite(db)
const queue = new Queue<{ name: string }>('greetings', {
  storage,
})

const worker = queue.process(async ({ name }) => {
  console.log(`Hello, ${name}!`)
})

await queue.add({ name: 'Ada' })

process.once('SIGINT', async () => {
  await worker.close()
  db.close()
})
```

With `better-sqlite3`, replace the connection setup and adapter call:

```ts
import Database from 'better-sqlite3'

import { betterSqlite3 } from '@walq/better-sqlite3'

const db = new Database('./queue.sqlite', {
  timeout: 5_000,
})

db.pragma('journal_mode = WAL')
db.pragma('synchronous = FULL')

const storage = betterSqlite3(db)
```

You own these connections: stop workers and await outstanding storage calls before calling `db.close()`. Initialize the adapter outside an external transaction. File-backed databases need a local filesystem with SQLite-compatible locking.

## Queue features

The examples below use the `queue` and `storage` from the quick start.

### Delays and priority

```ts
await queue.add(
  { name: 'Grace' },
  {
    delay: 60_000,
    priority: 10,
  },
)

await queue.add(
  { name: 'Linus' },
  {
    runAt: Date.now() + 3_600_000,
  },
)
```

Use either `delay` or `runAt`, not both. Higher priority runs first among due jobs within the same group or ungrouped stream.

### Deduplication and group limits

```ts
await queue.add(
  { name: 'Ada' },
  {
    dedupe: 'greeting:ada',
    group: {
      id: 'team:engineering',
      concurrency: 2,
    },
  },
)
```

A dedupe key returns the existing job until it is physically removed. Group limits apply across workers and connections; use the same concurrency for every job in a group.

### Retries and concurrent processing

```ts
const emails = new Queue<{ to: string }>('email', {
  storage,
  attempts: 5,
  backoff: {
    type: 'exponential',
    delay: 1_000,
    jitter: 0.2,
  },
  onError: (error, context) => {
    console.error(context, error)
  },
})

const emailWorker = emails.process(
  async ({ to }, { signal, jobId, attempt }) => {
    signal.throwIfAborted()
    console.log({ to, jobId, attempt })
  },
  {
    concurrency: 4,
  },
)
```

Throw from a handler to fail the job and apply the retry policy. `attempts` includes the first try. Cooperate with `signal` when a lease is lost. Close every worker before closing shared storage.

### Batch enqueue and processing

```ts
await queue.addMany([
  { data: { name: 'Ada' } },
  {
    data: { name: 'Grace' },
    options: { delay: 60_000 },
  },
])

// Use instead of queue.process(); one worker per queue name and storage instance.
const batchWorker = queue.processMany(
  async (jobs) => {
    for (const { data, context } of jobs) {
      context.signal.throwIfAborted()
      console.log(data.name)
    }
  },
  {
    batch: 20,
    concurrency: 2,
  },
)
```

`addMany` is atomic. A batch handler rejection fails or retries each still-owned job; use `process` for independent outcomes.

### Repeating schedules

```ts
await queue.schedule(
  { name: 'Ada' },
  {
    id: 'hourly-greeting',
    every: 3_600_000,
  },
)

await queue.schedule(
  { name: 'Grace' },
  {
    id: 'daily-greeting',
    cron: '0 9 * * *',
  },
)

const schedule = await queue.getSchedule('daily-greeting')
await queue.removeSchedule('daily-greeting')
```

Schedules are durable and queue-scoped; cron runs in UTC. A running worker materializes due jobs. Missed occurrences coalesce into one job.

### Inspect and control jobs

```ts
const { id } = await queue.add(
  { name: 'Ada' },
  {
    delay: 60_000,
  },
)

const job = await queue.get(id)
const failed = await queue.list({
  status: 'failed',
  limit: 20,
})
const counts = await queue.stats()

await queue.pause()
await queue.reschedule(id, { delay: 120_000 })
await queue.cancel(id)
await queue.remove(id)
await queue.resume()

if (failed[0]) {
  await queue.retry(failed[0].id)
}
```

Pause stops new claims, not active handlers. Cancel and reschedule affect pending jobs only; retry affects failed jobs. Lifecycle methods return `false` for missing jobs or incompatible states.

### Retention

```ts
const archive = new Queue<{ name: string }>('archive', {
  storage,
  retention: {
    completed: 100,
    failed: {
      count: 1_000,
      maxAge: 7 * 24 * 60 * 60 * 1_000,
    },
  },
})
```

Cleanup is asynchronous. Defaults keep 0 completed and 100 failed jobs; `null` keeps all. Cancelled jobs remain until removed.

## Docs

[Core API](packages/core/README.md) · [Built-in SQLite](packages/sqlite/README.md) · [`better-sqlite3`](packages/better-sqlite3/README.md) · [Storage adapter contract](docs/storage-contract.md) · [Changelog](CHANGELOG.md)

## Development

Requires pnpm 12.

```sh
pnpm install
pnpm check
```

## License

Apache-2.0
