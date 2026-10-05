# @walq/sqlite

## 1.2.2

### Patch Changes

- b4fb895: Preserve both the original and cleanup errors when node:sqlite rollback fails or a managed adapter cannot close after initialization failure. These failures reject with a standard AggregateError containing both errors and the original as cause. Successful cleanup still rethrows the original error unchanged.
- 685303a: Use serialize-error for storage worker error transport so causes, SQLite diagnostic fields, and custom enumerable properties are preserved. Error properties are serialized with bounded depth and without invoking custom toJSON hooks. Public APIs and SQL execution policies are unchanged.
- e66742d: Document error logging, direct-call rejections, worker diagnostics, and aggregate cleanup errors.
- Updated dependencies [fd98379]
- Updated dependencies [e8af85e]
- Updated dependencies [685303a]
- Updated dependencies [946b1b6]
- Updated dependencies [e66742d]
- Updated dependencies [2fdd44c]
  - @walq/core@1.2.2
  - @walq/sqlite-common@1.2.2

## 1.2.1

### Patch Changes

- eab33ed: Improve README quick starts, adapter selection, caller-owned connection examples, and code examples for queue features.
- Updated dependencies [eab33ed]
  - @walq/core@1.2.1
  - @walq/sqlite-common@1.2.1

## 1.2.0

### Minor Changes

- 038a5cc: Add managed `createStorage` factories with optional dedicated worker execution, bounded outstanding calls, and graceful connection shutdown. Existing externally owned connection adapters remain unchanged.

### Patch Changes

- Updated dependencies [038a5cc]
  - @walq/sqlite-common@1.2.0
  - @walq/core@1.2.0

## 1.1.0

### Minor Changes

- ac327e3: Add a built-in node:sqlite adapter for Node.js, Bun, and Deno. Share the SQLite storage implementation with better-sqlite3 while preserving the existing adapter API and database format.

### Patch Changes

- ac327e3: Move the shared SQLite storage implementation into @walq/sqlite-common so each adapter can be installed independently. Remove the @walq/sqlite/internal entry point; shared executor contracts remain implementation details.
- Updated dependencies [ac327e3]
  - @walq/sqlite-common@1.1.0
  - @walq/core@1.1.0
