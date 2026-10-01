# walq

**A small, lease-based job queue for SQLite.** At-least-once processing, written in TypeScript; no separate queue service required.

Requires Node.js 22+. Walq is ESM-only.

## Install

```sh
npm install @walq/core @walq/better-sqlite3 better-sqlite3
```

`better-sqlite3` needs a supported prebuilt binary or native build tools.

## Quick start

```ts
import Database from 'better-sqlite3'
import { betterSqlite3 } from '@walq/better-sqlite3'
import { Queue } from '@walq/core'

const db = new Database('queue.sqlite', { timeout: 5_000 })
db.pragma('journal_mode = WAL')
db.pragma('synchronous = FULL')

const queue = new Queue<{ name: string }>('greetings', {
  storage: betterSqlite3(db),
})
queue.process(async ({ name }) => console.log(`Hello, ${name}!`))
await queue.add({ name: 'Ada' })
```

Jobs are delivered at least once; make handlers idempotent when repeating side effects is unsafe. Stop workers before closing the caller-owned database. See the [Core API](packages/core/README.md) for batch processing, groups, durable pause/resume, and repeating schedules.

## Docs

[Core API](packages/core/README.md) · [`better-sqlite3`](packages/better-sqlite3/README.md) · [Storage adapter contract](docs/storage-contract.md) · [Changelog](CHANGELOG.md)

## Development

Requires pnpm 12.

```sh
pnpm install
pnpm check
```

## License

Apache-2.0
