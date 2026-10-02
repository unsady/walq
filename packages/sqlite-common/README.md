# @walq/sqlite-common

Shared implementation dependency of [`@walq/sqlite`](../sqlite/README.md) and [`@walq/better-sqlite3`](../better-sqlite3/README.md). Install an adapter, not this package directly.

Neither adapter depends on the other. This package contains the shared schema, migrations, SQL operations, validation, schedules, cleanup, and claim chunking. It depends on neither `node:sqlite` nor `better-sqlite3`.

`createStorage(Connection)` is a synchronous SQLite executor behind the promise-based `Storage` contract. Its connection and transaction contracts are implementation details coordinated with the adapters, not a supported extension API.

Pure validation and scheduling helpers are separate from connection-dependent execution. Future asynchronous libSQL/Turso adapters may reuse compatible schema, SQL, and helpers, but require their own transaction and batching implementation; the current executor does not support remote clients.

See the [storage contract](../../docs/storage-contract.md) for adapter semantics.
