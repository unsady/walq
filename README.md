# walq

**A small, lease-based job queue for SQLite.**

Walq provides reliable at-least-once processing without Redis or a separate queue service. It is written in TypeScript and designed to support different SQLite runtimes through storage adapters.

> **Early stage:** Walq is under active development. During the `0.x` series, the API and storage schema may change between minor versions; incompatible schema changes are called out in the changelog.

## Installation

Walq is ESM-only and requires Node.js 22 or newer.

```sh
pnpm add walq @walq/better-sqlite3 better-sqlite3
```

`better-sqlite3` uses a native addon and requires either a supported prebuilt binary or native build tools.

## Features

- atomic claims with expiring leases
- retries, attempt limits, and heartbeats
- stale-worker protection through unique lease tokens
- multiple logical queues on one database
- grouped claims across queues
- shared, queue-aware polling
- bounded count- and age-based terminal-job retention

## Example

```ts
import Database from 'better-sqlite3'
import { betterSqlite3 } from '@walq/better-sqlite3'
import { Queue } from 'walq'

const db = new Database('queue.sqlite')
db.pragma('journal_mode = WAL')

const queue = new Queue<{ name: string }>('greetings', {
  storage: betterSqlite3(db),
})

const worker = queue.process(async ({ name }) => {
  console.log(`Hello, ${name}!`)
})

await queue.add({ name: 'Ada' })
```

When shutting down, call `await worker.close()` to wait for active handlers, then `db.close()`.

Queue errors that would otherwise be swallowed — failed claims, lease mutations,
cleanup passes, and handler failures — are written to `console.error`. Pass
`onError` when creating the queue to handle them yourself; `lease_lost` is a
normal protocol outcome and is never reported.

```ts
const queue = new Queue<{ name: string }>('greetings', {
  storage: betterSqlite3(db),
  onError(err, ctx) {
    console.error(`[${ctx.queue}] ${ctx.operation} failed`, err, ctx)
  },
})
```

Jobs move from `pending` to `active` when claimed. Completing a live lease makes the job `completed`; failures retry while attempts remain, and expired leases are recovered automatically. Delivery is at least once, so handlers should be idempotent when side effects cannot safely be repeated.

Terminal jobs are cleaned up asynchronously. By default completed jobs are removed and the newest 100 failures per queue are kept. Pass `retention` to `new Queue` to change the count or set a `maxAge` in milliseconds; a job is removed when it exceeds either bound. For example, `retention: { completed: 0, failed: { count: 1_000, maxAge: 7 * 24 * 60 * 60 * 1_000 } }` keeps at most the newest 1,000 failures and none older than a week.

Queues using the same `Storage` instance share one coordinator. Supported adapters can group claims from several queues into one database transaction.

## Status

The Queue runtime, terminal-job retention, and the `better-sqlite3` adapter are
implemented. Additional SQLite adapters and adapter configuration helpers are
planned.

- [Storage semantics](docs/storage-contract.md)
- [`better-sqlite3` adapter](packages/better-sqlite3/README.md)
- [Benchmarks](benchmarks/README.md)
- [Roadmap](ROADMAP.md)
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
- [Release process](docs/releasing.md)
- [Changelog](CHANGELOG.md)

## License

Apache-2.0
