# Roadmap

## Terminal-job retention follow-ups

Count- and age-based retention is implemented: queues clean terminal jobs
asynchronously in bounded batches through the storage `cleanup` operation.
Remaining work:

- Extend the retention benchmark from raw `DELETE`/`VACUUM` SQL to production
  `cleanup` runs with different batch sizes, event-loop stalls, warm or
  reopened databases, and confirm that per-call work does not grow with the
  retained history.
- Cover concurrent completion and cleanup explicitly, beyond per-call conformance.
- Consider exposing cleanup metrics such as rows removed, pass count, and backlog.

## Opt-in adapter configuration helpers

Goal: provide explicit, queue-oriented database defaults while preserving caller ownership and avoiding hidden expensive operations.

### Public API

- Add one helper per adapter rather than changing settings inside the storage factory.
- For `better-sqlite3`, expose an API such as `configureBetterSqlite3(db, options)` that must run before `betterSqlite3(db)` initializes the schema.
- Return the applied configuration or otherwise make the effective settings observable.
- Let callers override durability-sensitive choices instead of presenting one setting as universally correct.

### SQLite settings

- Support WAL journal mode, synchronous mode, busy timeout, and incremental auto-vacuum.
- Provide documented queue-oriented defaults, including the durability trade-off between `synchronous = NORMAL` and `FULL`.
- Treat `auto_vacuum = INCREMENTAL` as a schema-time choice and apply it before creating tables.
- Keep WAL checkpoint and incremental-vacuum execution as explicit maintenance operations with bounded work where possible.
- Do not modify unrelated caller pragmas.

### Initialization and errors

- Reject schema-time settings after tables have been initialized instead of attempting a migration or silently running `VACUUM`.
- Reject configuration from inside a caller-managed transaction when SQLite cannot apply it safely.
- Keep network-filesystem and single-writer limitations explicit in the adapter documentation.
- Ensure repeated configuration is idempotent.

### Validation

- Test fresh file-backed databases, repeated calls, option overrides, invalid transaction context, and rejection after schema initialization.
- Benchmark default versus configured connections for throughput, writer latency, WAL growth, checkpoint stalls, and incremental-vacuum batch sizes.
- Document when configuration must run, which operations may block, and which maintenance remains the caller's responsibility.
