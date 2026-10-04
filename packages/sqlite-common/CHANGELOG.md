# @walq/sqlite-common

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
