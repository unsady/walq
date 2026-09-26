# @walq/better-sqlite3

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
