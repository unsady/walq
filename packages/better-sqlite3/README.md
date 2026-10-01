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

Ungrouped jobs take turns as one virtual group alongside eligible groups in a persistent round-robin; named groups follow in binary group-ID order. Within each stream, jobs are selected by priority, availability, then insertion order. Claims tied on priority and availability within a stream, and pending-list rows tied on availability, follow successful insertion order. Deduplicated enqueues do not create a new position, and retries/reschedules retain the original position. This is stored internally and is not part of the public job API. Schema v12 migrates schemas 2–10 (version 11 is unsupported); existing pre-v6 rows receive a deterministic approximate historical order by `createdAt`, then binary ID. Queue-scoped group concurrency configurations, round-robin position, and pause state persist independently of jobs, including after deletion and database reopen. Databases created by the earlier v12 implementation can still have the obsolete `walq_active_group` index; after stopping workers, `DROP INDEX IF EXISTS walq_active_group` removes it. Fresh databases and migrations from v10 or earlier do not create or retain it.

Storage methods return promises, but each SQLite transaction is synchronous and blocks the event loop. `claimQueues` splits requests into transactions with a 512-job budget and yields to the event loop between committed chunks; one oversized request cannot be split. Single-chunk calls do not yield. Other operations may run between chunks, so the whole batch is not atomic. Stop workers and await outstanding storage calls before closing the connection. The adapter initializes reserved `walq_schema`, `walq_jobs`, `walq_groups`, `walq_schedules`, and `walq_paused_queues` tables and migrates schema versions 2–10 to v12; use a writable connection and do not call it inside a caller-managed transaction. `enqueueMany` commits the entire batch atomically, including deduplication. Pause state is read and written atomically with claim and schedule-materialization guards.

See the [storage contract](../../docs/storage-contract.md) for adapter semantics, including leases, queue isolation, and error results.

Run `pnpm test` from the workspace root; the concurrent worker test requires Node.js 24+.
