# walq

**A small, lease-based job queue for SQLite.** At-least-once processing, written in TypeScript; no separate queue service required.

Walq is ESM-only. The built-in SQLite adapter supports Node.js 22.16+, Bun 1.4.2+, and Deno 2.9.7+.

## Install

```sh
npm install @walq/core @walq/sqlite
```

Uses the runtime's built-in `node:sqlite`; no native npm addon required. See the [SQLite adapter](packages/sqlite/README.md) for Bun and Deno installation. The existing [`better-sqlite3` adapter](packages/better-sqlite3/README.md) remains available.

## Quick start

```ts
import { DatabaseSync } from 'node:sqlite'
import { sqlite } from '@walq/sqlite'
import { Queue } from '@walq/core'

const db = new DatabaseSync('queue.sqlite')
db.exec('PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL; PRAGMA busy_timeout = 5000')

const queue = new Queue<{ name: string }>('greetings', {
  storage: sqlite(db),
})
queue.process(async ({ name }) => console.log(`Hello, ${name}!`))
await queue.add({ name: 'Ada' })
```

Jobs are delivered at least once; make handlers idempotent when repeating side effects is unsafe. Stop workers before closing the caller-owned database. See the [Core API](packages/core/README.md) for batch processing, groups, durable pause/resume, and repeating schedules.

## Docs

[Core API](packages/core/README.md) · [Built-in SQLite](packages/sqlite/README.md) · [`better-sqlite3`](packages/better-sqlite3/README.md) · [Storage adapter contract](docs/storage-contract.md) · [Changelog](CHANGELOG.md)

## Development

Requires pnpm 12.

```sh
pnpm install
pnpm check
```

## License

Apache-2.0
