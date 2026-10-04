# @walq/better-sqlite3

SQLite `Storage` adapter for walq. ESM-only; requires Node.js 22+.

```sh
pnpm add @walq/core @walq/better-sqlite3 better-sqlite3
```

`better-sqlite3` has a native addon and requires a supported prebuilt binary or native build tools. For the runtime's built-in driver, use [`@walq/sqlite`](../sqlite/README.md); both adapters share the storage implementation and database format through [`@walq/sqlite-common`](../sqlite-common/README.md), without depending on each other.

## Bring your own connection

```ts
import Database from 'better-sqlite3'
import { betterSqlite3 } from '@walq/better-sqlite3'
import { Queue } from '@walq/core'

const db = new Database('./queue.sqlite', {
  timeout: 5_000,
})

db.pragma('journal_mode = WAL')
db.pragma('synchronous = FULL')

const storage = betterSqlite3(db)
const queue = new Queue<{ to: string }>('email', {
  storage,
})

const worker = queue.process(async ({ to }) => {
  console.log(`Email ${to}`)
})
await queue.add({ to: 'user@example.com' })

// On shutdown:
await worker.close()
db.close()
```

## Managed storage

`createStorage` opens and owns a connection. Set `worker: true` to run SQLite in a dedicated thread instead of blocking the main event loop (worker mode is currently verified on Node.js only).

```ts
import { createStorage } from '@walq/better-sqlite3'

const storage = await createStorage({
  filename: './queue.sqlite',
  worker: true, // Defaults to false.
})

const queue = new Queue('email', {
  storage,
})

const processor = queue.process(async (data) => {
  console.log(data)
})

// On shutdown, stop queue processors before closing storage:
await processor.close()
await storage.close()
```

Both modes default to WAL, `synchronous = FULL`, and a 5000-millisecond busy timeout. Override these with `initialization`, a SQL string executed before schema initialization. For advanced driver-specific configuration, continue using an externally owned connection with `betterSqlite3(db)`.

`close()` is idempotent, rejects new calls, drains accepted calls (including failed calls), and closes the connection. Worker mode uses one thread per storage and processes calls sequentially. It defaults to at most 1024 outstanding calls; configure `maxPending` to change this limit. Calls beyond the limit reject rather than accumulating indefinitely. Worker failures reject outstanding calls; mutations are never automatically replayed because their commit outcome may be unknown. Threads isolate event-loop blocking, not SQLite write locks or delays to other storage operations.

## Connection and initialization

The caller owns the connection. Stop workers and await outstanding storage calls before closing it. For durable file-backed use, configure WAL, `synchronous = FULL`, and a suitable busy timeout. WAL requires a local filesystem with SQLite-compatible locking; durability depends on the filesystem and hardware honoring sync requests. SQLite allows one writer at a time.

Initialization requires a writable connection outside a caller-managed transaction and creates internal `walq_` tables. Schema v12 automatically migrates versions 2–10; v11 is unsupported. See the [1.0.0 migration notes](https://github.com/unsady/walq/releases/tag/v1.0.0) for backup guidance, historical ordering, and obsolete-index removal.

## Claims and transactions

Claims use persistent round-robin scheduling across eligible groups and the ungrouped stream; within each stream, priority, availability, and insertion order determine selection. Group concurrency, scheduling position, and pause state survive deletion of jobs and database reopen.

Storage methods return promises, but SQLite transactions block the event loop. `claimQueues` commits chunks with a 512-job budget and yields between them; a single oversized request cannot be split. Single-chunk calls do not yield, and multi-chunk calls are not atomic. `enqueueMany` is atomic, including deduplication.

See the [storage contract](../../docs/storage-contract.md) for exact ordering, leases, pause/schedule semantics, and error results.

Run `pnpm test` from the workspace root; the concurrent worker test requires Node.js 24+.
