# @walq/sqlite-common

## 1.2.2

### Patch Changes

- 685303a: Use serialize-error for storage worker error transport so causes, SQLite diagnostic fields, and custom enumerable properties are preserved. Error properties are serialized with bounded depth and without invoking custom toJSON hooks. Public APIs and SQL execution policies are unchanged.
- Updated dependencies [fd98379]
- Updated dependencies [e8af85e]
- Updated dependencies [946b1b6]
- Updated dependencies [e66742d]
- Updated dependencies [2fdd44c]
  - @walq/core@1.2.2

## 1.2.1

### Patch Changes

- Updated dependencies [eab33ed]
  - @walq/core@1.2.1

## 1.2.0

### Minor Changes

- 038a5cc: Add managed `createStorage` factories with optional dedicated worker execution, bounded outstanding calls, and graceful connection shutdown. Existing externally owned connection adapters remain unchanged.

### Patch Changes

- @walq/core@1.2.0

## 1.1.0

### Minor Changes

- ac327e3: Move the shared SQLite storage implementation into @walq/sqlite-common so each adapter can be installed independently. Remove the @walq/sqlite/internal entry point; shared executor contracts remain implementation details.

### Patch Changes

- @walq/core@1.1.0
