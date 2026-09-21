# walq

**A small, lease-based job queue for SQLite.**

Walq provides reliable at-least-once processing without Redis or a separate queue service. It is written in TypeScript and supports SQLite through storage adapters.

## Features

- atomic claims with expiring leases
- retries, attempt limits, and heartbeats
- multiple logical queues in one database
- concurrent processing with shared, queue-aware polling
- automatic count- and age-based cleanup

## Installation

Requires Node.js 22+. Walq is ESM-only.

```sh
npm install @walq/core @walq/better-sqlite3 better-sqlite3
```

`better-sqlite3` uses a native addon and requires either a supported prebuilt binary or native build tools.

## Quick start

```ts
import Database from 'better-sqlite3'
import { betterSqlite3 } from '@walq/better-sqlite3'
import { Queue } from '@walq/core'

const db = new Database('queue.sqlite', { timeout: 5_000 })
db.pragma('journal_mode = WAL')
db.pragma('synchronous = FULL')

const storage = betterSqlite3(db)
const queue = new Queue<{ name: string }>('greetings', { storage })

const worker = queue.process(async ({ name }) => {
  console.log(`Hello, ${name}!`)
})

await queue.add({ name: 'Ada' })
```

Jobs are delivered at least once. Handlers should be idempotent when repeating side effects would be unsafe.

## Shutdown

Stop claiming new jobs, wait for active handlers, and then close the caller-owned database connection:

```ts
async function shutdown() {
  await worker.close()
  db.close()
}

process.once('SIGINT', () => void shutdown())
process.once('SIGTERM', () => void shutdown())
```

## Retention and cleanup

Terminal jobs are cleaned up automatically in bounded batches. By default, completed jobs are removed and the newest 100 failed jobs per queue are retained.

```ts
const queue = new Queue('email', {
  storage,
  retention: {
    completed: 0,
    failed: {
      count: 1_000,
      maxAge: 7 * 24 * 60 * 60 * 1_000,
    },
  },
})
```

A job is removed when it exceeds either the count or age limit. Set a status to `null` to retain all jobs of that status.

## Documentation

- [Core API](packages/core/README.md)
- [`better-sqlite3` adapter](packages/better-sqlite3/README.md)
- [Storage adapter contract](docs/storage-contract.md)
- [Benchmarks](benchmarks/README.md)
- [Roadmap](ROADMAP.md)
- [Contributing](CONTRIBUTING.md)
- [Security policy](SECURITY.md)
- [Release process](docs/releasing.md)
- [Changelog](CHANGELOG.md)

## License

Apache-2.0
