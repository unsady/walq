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
  payload: JSON.stringify({ to: 'user@example.com' }),
  now: Date.now(),
  availableAt: Date.now(),
  attempts: 3,
})

// Close only after all users of this connection have stopped.
db.close()
```

The future Queue API can accept this value as `storage: betterSqlite3(db)`.
Queue itself is not implemented yet. One storage instance can serve multiple
queues, isolated by their exact queue names.

## Connection and schema

- The factory synchronously initializes versioned `walq_schema` and `walq_jobs`
  tables in an immediate transaction, then prepares statements. Repeated
  initialization is supported; unknown schema versions are rejected. These table
  names are reserved for the adapter.
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
retry policy, or automatic retention is provided.

Inputs are validated before mutation. Payload must already be serialized JSON.
The adapter uses the supplied `now`, never its own clock. Job IDs and fresh lease
tokens are generated with `crypto.randomUUID()`.

Claim recovery and acquisition run in one immediate transaction. Recovery covers
all expired active jobs in the requested queue, regardless of the batch limit.
Completion, failure, and heartbeat use atomic conditional updates. Database errors
(including lock timeouts) reject promises, rather than returning `lease_lost`.

See [the storage contract](../../docs/storage-contract.md) for delivery semantics,
attempt accounting, expiry boundaries, and lease protection. Tokens protect queue
state, not handler side effects.

## Tests

Run `pnpm test` from the workspace root. The concurrent worker test uses Node's
native TypeScript stripping and module hooks; use Node 24 or newer for the test
suite.
