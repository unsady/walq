# @walq/better-sqlite3

SQLite `Storage` adapter for walq. ESM-only; requires Node.js 22+.

```sh
pnpm add @walq/core @walq/better-sqlite3 better-sqlite3
```

`better-sqlite3` has a native addon and requires a supported prebuilt binary or native build tools.

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

The caller owns the connection and must stop workers before closing it. Configure WAL, `synchronous = FULL`, and a suitable busy timeout for durable file-backed use; durability depends on the filesystem and hardware honoring SQLite sync requests. WAL needs a local filesystem with SQLite-compatible locking. SQLite has one writer at a time.

Storage methods return promises, but SQLite work is synchronous and blocks the event loop. The adapter initializes reserved `walq_schema` and `walq_jobs` tables; use a writable connection and do not call it inside a caller-managed transaction. `enqueueMany` commits the entire batch atomically.

See the [storage contract](../../docs/storage-contract.md) for adapter semantics, including leases, queue isolation, and error results.

Run `pnpm test` from the workspace root; the concurrent worker test requires Node.js 24+.
