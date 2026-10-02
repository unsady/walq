# @walq/better-sqlite3

SQLite `Storage` adapter for walq. ESM-only; requires Node.js 22+.

```sh
pnpm add @walq/core @walq/better-sqlite3 better-sqlite3
```

`better-sqlite3` has a native addon and requires a supported prebuilt binary or native build tools. For the runtime's built-in driver, use [`@walq/sqlite`](../sqlite/README.md); both adapters share the storage implementation and database format through [`@walq/sqlite-common`](../sqlite-common/README.md), without depending on each other.

```ts
import Database from 'better-sqlite3'
import { betterSqlite3 } from '@walq/better-sqlite3'
import { Queue } from '@walq/core'

const db = new Database('./queue.sqlite', { timeout: 5_000 })
db.pragma('journal_mode = WAL')
db.pragma('synchronous = FULL')

const queue = new Queue<{ to: string }>('email', {
  storage: betterSqlite3(db),
})
queue.process(async ({ to }) => console.log(`Email ${to}`))
await queue.add({ to: 'user@example.com' })
```

## Connection and initialization

The caller owns the connection. Stop workers and await outstanding storage calls before closing it. For durable file-backed use, configure WAL, `synchronous = FULL`, and a suitable busy timeout. WAL requires a local filesystem with SQLite-compatible locking; durability depends on the filesystem and hardware honoring sync requests. SQLite allows one writer at a time.

Initialization requires a writable connection outside a caller-managed transaction and creates internal `walq_` tables. Schema v12 automatically migrates versions 2–10; v11 is unsupported. See the [1.0.0 migration notes](../../release-notes/v1.0.0.md#sqlite-migration) for backup guidance, historical ordering, and obsolete-index removal.

## Claims and transactions

Claims use persistent round-robin scheduling across eligible groups and the ungrouped stream; within each stream, priority, availability, and insertion order determine selection. Group concurrency, scheduling position, and pause state survive deletion of jobs and database reopen.

Storage methods return promises, but SQLite transactions block the event loop. `claimQueues` commits chunks with a 512-job budget and yields between them; a single oversized request cannot be split. Single-chunk calls do not yield, and multi-chunk calls are not atomic. `enqueueMany` is atomic, including deduplication.

See the [storage contract](../../docs/storage-contract.md) for exact ordering, leases, pause/schedule semantics, and error results.

Run `pnpm test` from the workspace root; the concurrent worker test requires Node.js 24+.
