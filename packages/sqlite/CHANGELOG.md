# @walq/sqlite

## 1.1.0

### Minor Changes

- ac327e3: Add a built-in node:sqlite adapter for Node.js, Bun, and Deno. Share the SQLite storage implementation with better-sqlite3 while preserving the existing adapter API and database format.

### Patch Changes

- ac327e3: Move the shared SQLite storage implementation into @walq/sqlite-common so each adapter can be installed independently. Remove the @walq/sqlite/internal entry point; shared executor contracts remain implementation details.
- Updated dependencies [ac327e3]
  - @walq/sqlite-common@1.1.0
  - @walq/core@1.1.0
