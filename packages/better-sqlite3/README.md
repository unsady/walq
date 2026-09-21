# @walq/better-sqlite3

A `Storage` implementation for walq using a caller-owned `better-sqlite3`
connection. Install `better-sqlite3` alongside this package; it is a peer dependency
and requires a native addon (a supported prebuilt binary or native build tools).

```ts
import Database from 'better-sqlite3'
import { betterSqlite3 } from '@walq/better-sqlite3'

const db = new Database('./queue.sqlite', { timeout: 5000 })
db.pragma('journal_mode = WAL')
db.pragma('synchronous = FULL')

const storage = betterSqlite3(db)

await storage.enqueue({
  queue: 'email',
  name: 'send',
  data: JSON.stringify({ to: 'user@example.com' }),
  now: Date.now(),
  availableAt: Date.now(),
  attempts: 3,
})

// Close only after all users of this connection have stopped.
db.close()
```

The `walq` package accepts this value as `storage: betterSqlite3(db)`. One
storage instance can serve multiple queues, isolated by their exact queue names.

## Connection and schema

- The factory synchronously initializes versioned `walq_schema` and `walq_jobs`
  tables in an immediate transaction, then prepares statements. Repeated
  initialization is supported; unknown or older schema versions are rejected.
  Because the project is pre-release, outdated development databases are
  recreated by deleting the file instead of migrated. These table names are
  reserved for the adapter.
- The caller owns and closes the connection. The adapter neither closes it nor
  changes connection pragmas. It requires a writable connection.
- Configure WAL, `synchronous = FULL`, and a suitable busy timeout as shown above
  for durable file-backed usage. Durability still depends on the filesystem and
  hardware honoring SQLite's synchronization requests.
- WAL requires a local filesystem with SQLite-compatible locking, not a network
  filesystem. SQLite allows only one writer at a time; busy timeouts and slow
  writes block the calling thread.
- Do not initialize or invoke storage inside a caller-managed transaction. Such
  calls are rejected so no operation resolves before its mutation commits.
- `:memory:` is supported for ephemeral usage but is not durable or shared across
  independent connections.

## Behavior

Methods return promises to implement `@walq/core/storage`, but database work is
**synchronous and blocks the event loop**. No background worker, polling loop,
retry policy, or cleanup scheduler is provided.

Inputs are validated before mutation. Data must already be serialized JSON.
The adapter uses the supplied `now`, never its own clock. Job IDs and fresh lease
tokens are generated with `crypto.randomUUID()`.

Claim recovery and acquisition run in one immediate transaction. Recovery covers
all expired active jobs in the requested queue, regardless of the batch limit.
Completion, failure, and heartbeat use atomic conditional updates. Terminal
transitions record the finish timestamp used for retention ordering. Database
errors (including lock timeouts) reject promises, rather than returning
`lease_lost`.

The optional `claimQueues` method applies every request in order inside a single
immediate transaction, so one coordinator sweep acquires one writer lock instead
of one per queue. Each request keeps the per-queue recovery, ordering, limit, and
lease-token semantics of `claim`. The whole batch is validated before the
transaction opens, and an error in any request rolls back every request. The
`StorageCoordinator` uses this method when present and otherwise falls back to
one `claim` call per request.

See [the storage contract](../../docs/storage-contract.md) for delivery semantics,
attempt accounting, expiry boundaries, and lease protection. Tokens protect queue
state, not handler side effects.

## Cleanup

Terminal transitions record when the job finished. `cleanup` keeps the newest
completed and failed jobs per queue and deletes the rest, oldest first:

```ts
const result = await storage.cleanup({
  queue: 'email',
  retention: { completed: 0, failed: 100 },
  limit: 500,
})
```

`retention` is per queue and per status: `0` deletes every terminal job of that
status, `null` keeps all of them. `limit` bounds rows deleted by one call; the
result reports how many rows were removed and whether more eligible rows remain,
so a caller can drain in batches. Rows are deleted inside one immediate
transaction, and only terminal rows of the requested queue are eligible, so
pending and active jobs are never removed. Rows beyond the retention counts are
kept in a partial index ordered by finish time.

Each call is bounded by `limit` and the retention counts rather than by the size
of the terminal history: the retained boundary is located with one index lookup
that walks at most `keep` index entries, deletion stops at `limit`, and `more`
is decided by index existence checks. Draining a large backlog therefore costs
one pass per deleted row, not a full recount per batch.

Cleanup never runs `VACUUM`: deleted pages stay available for reuse, and the
database file does not necessarily shrink. The adapter does not schedule cleanup;
callers decide when to run it and how to space batches.

## Tests

Run `pnpm test` from the workspace root. The concurrent worker test uses Node's
native TypeScript stripping and module hooks; use Node 24 or newer for the test
suite.
