# @walq/sqlite

SQLite `Storage` adapter for walq using the built-in `node:sqlite` API. ESM-only; no native npm addon or external SQLite driver required. The same imports work in Node.js, Bun, and Deno.

## Install

```sh
pnpm add @walq/core @walq/sqlite
```

For Bun, use `bun add @walq/core @walq/sqlite`. For Deno, use `deno add npm:@walq/core npm:@walq/sqlite`.

## Bring your own connection

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

Deno needs filesystem permissions for a file-backed database. The runtime test uses `deno run --allow-read --allow-write --allow-env`; scope permissions appropriately for your application.

## Runtime support

| Runtime | Supported baseline |
| ------- | ------------------ |
| Node.js | 22.16.0+           |
| Bun     | 1.4.2+             |
| Deno    | 2.9.7+             |

These baselines are tested in CI, not claims about the first release implementing SQLite. The adapter requires `DatabaseSync.isTransaction`, `StatementSync.setAllowBareNamedParameters`, `setAllowUnknownNamedParameters`, and `setReadBigInts`. Node.js 22.16.0 introduced `isTransaction`; older Node.js releases are unsupported. Some Node.js versions emit an experimental SQLite warning.

## Managed storage

`createStorage` opens and owns a connection. By default, `worker` is `false` and SQLite runs in the main thread. Optionally set `worker: true` to move SQLite work to a dedicated thread; job handlers stay in the main thread and work in either mode. Worker mode is currently verified on Node.js only.

```ts
import { createStorage } from '@walq/sqlite'

const storage = await createStorage({
  filename: './queue.sqlite',
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

Both modes default to WAL, `synchronous = FULL`, and a 5000-millisecond busy timeout. Override these with `initialization`, a SQL string executed before schema initialization. For advanced driver-specific configuration, continue using an externally owned connection with `sqlite(db)`.

`close()` is idempotent, rejects new calls, drains accepted calls (including failed calls), and closes the connection. Worker mode uses one thread per storage and processes calls sequentially. It defaults to at most 1024 outstanding calls; configure `maxPending` to change this limit. Calls beyond the limit reject rather than accumulating indefinitely. Worker failures reject outstanding calls; mutations are never automatically replayed because their commit outcome may be unknown. Threads isolate event-loop blocking, not SQLite write locks or delays to other storage operations.

## Connection and initialization

The caller owns the connection. Stop workers and await outstanding storage calls before closing it. Initialize outside a caller-managed transaction; storage operations also reject while an external transaction is active.

For durable file-backed use, configure WAL, `synchronous = FULL`, and a suitable busy timeout before calling `sqlite(db)`. WAL requires a local filesystem with SQLite-compatible locking; durability depends on the filesystem and hardware honoring sync requests. SQLite allows one writer at a time.

Both `@walq/sqlite` and [`@walq/better-sqlite3`](../better-sqlite3/README.md) use the same schema, migrations, and storage implementation through [`@walq/sqlite-common`](../sqlite-common/README.md). Neither adapter depends on the other. Existing databases can be reopened with either adapter; no format conversion is needed. Initialization creates internal `walq_` tables. Schema v12 automatically migrates versions 2–10; v11 is unsupported. Back up existing databases before migration; see the [migration notes](https://github.com/unsady/walq/releases/tag/v1.0.0).

## Transactions

Storage methods return promises, but SQLite work blocks the event loop. Transactions use `BEGIN IMMEDIATE`; failed operations roll back. `enqueueMany` is atomic, including deduplication. `claimQueues` commits chunks with a 512-job budget and yields between them; a single oversized request cannot be split. Multi-chunk calls are not atomic.

See the [storage contract](../../docs/storage-contract.md) for ordering, leases, groups, retention, pause, and schedule semantics.

## Development

From the workspace root, run `pnpm check` for the complete workspace checks or `pnpm test:sqlite` for the built-package Node.js runtime checks. The same storage conformance suite runs under all three runtimes, alongside independent-process races for claims, deduplication, group limits, and schedules. Queue integration tests run against both SQLite adapters in Vitest.

After `pnpm build`, run the Bun and Deno checks with:

```sh
bun tests/sqlite/runtime.mjs
bun test tests/sqlite/*.test.mjs
deno run --allow-read --allow-write --allow-env tests/sqlite/runtime.mjs
deno test --no-check --allow-read --allow-write --allow-env --allow-run tests/sqlite/*.test.mjs
```

Deno's `--no-check` skips a second type check of the shared NodeNext test sources; `pnpm typecheck` checks those sources separately. This matrix verifies runtime behavior, not Deno-specific type resolution. Process-race tests require subprocess permissions.
