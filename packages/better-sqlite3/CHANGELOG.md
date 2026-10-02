# @walq/better-sqlite3

## 1.1.0

### Patch Changes

- ac327e3: Add a built-in node:sqlite adapter for Node.js, Bun, and Deno. Share the SQLite storage implementation with better-sqlite3 while preserving the existing adapter API and database format.
- ac327e3: Move the shared SQLite storage implementation into @walq/sqlite-common so each adapter can be installed independently. Remove the @walq/sqlite/internal entry point; shared executor contracts remain implementation details.
- Updated dependencies [ac327e3]
  - @walq/sqlite-common@1.1.0
  - @walq/core@1.1.0

## 1.0.0

### Minor Changes

- 1443345: Add signed integer job priorities to enqueue options. Higher-priority due jobs are claimed first, and SQLite migrates existing jobs with priority 0.
- bee32f6: Add durable queue pause and resume operations that prevent new claims and schedule materialization.
- fed2ca5: Add durable repeating schedules with interval and UTC cron support, atomic SQLite materialization, and schedule schema migration.
- 9bb156c: Add queue-scoped job groups with persistent, globally enforced concurrency limits.
- 8702ba8: Add `queue.stats()` for persisted per-status queue counts and require the storage count operation.
- efb5221: Break claim and pending-list ties by original enqueue order instead of job ID. SQLite migrates schema versions 2–5 to v6 with a deterministic approximate historical order for existing jobs.
- 6cafa15: Add queue-scoped job deduplication keys to `queue.add()` and `queue.addMany()`. SQLite migrates schema v4 to v5 with persisted unique keys.

### Patch Changes

- Updated dependencies [1443345]
- Updated dependencies [bee32f6]
- Updated dependencies [fed2ca5]
- Updated dependencies [a85fba2]
- Updated dependencies [9bb156c]
- Updated dependencies [8702ba8]
- Updated dependencies [efb5221]
- Updated dependencies [6cafa15]
- Updated dependencies [0ede0d7]
  - @walq/core@1.0.0

## 0.3.0

### Minor Changes

- 3f74a4c: Add public job inspection, listing, retry, cancellation, rescheduling, and removal APIs. The SQLite adapter automatically migrates the schema from v2 to v3 to support the `cancelled` status.

### Patch Changes

- Updated dependencies [3f74a4c]
  - @walq/core@0.3.0

## 0.2.1

### Patch Changes

- Updated dependencies
  - @walq/core@0.2.1

## 0.2.0

### Minor Changes

- 911499e: Add atomic `queue.addMany()` support and require storage adapters to implement `enqueueMany()`.

### Patch Changes

- Updated dependencies [b619265]
- Updated dependencies [911499e]
- Updated dependencies [f24e1e7]
  - @walq/core@0.2.0
