# Roadmap

## Eventual terminal-job retention

Goal: keep terminal history bounded without adding latency to `complete()` or running automatic full-database maintenance.

### Semantics and configuration

- Add separate retention policies for completed and failed jobs.
- Default completed-job retention to zero: completed jobs become eligible for cleanup immediately, but deletion happens asynchronously after `complete()` commits.
- Keep failed-job retention explicit and independent so failures can remain available for diagnostics.
- Define count-based retention first; add age-based retention only with a real `finishedAt` timestamp rather than deriving age from `createdAt`.
- Preserve the current lease rule: only a live matching lease can move a job into a terminal state.

### Storage contract

- Add an optional bounded cleanup capability with a sequential fallback or a clear unsupported result for adapters that do not implement retention.
- Scope cleanup by queue and terminal status; accept a batch limit and return the number removed plus whether more eligible work may remain.
- Keep cleanup separate from `complete()` and `fail()` so successful handler acknowledgement does not wait for maintenance.
- Document eventual behavior: a completed row may remain visible until a later cleanup pass, including after a process crash.

### SQLite implementation

- Add `finishedAt` directly to the initial schema if age-based retention is included; no pre-release migrations are needed.
- Add only the indexes required by the selected retention queries; use partial indexes for terminal statuses where appropriate.
- Delete rows in bounded transactions, initially benchmarking batch sizes such as 100, 500, 1,000, and 5,000.
- Do not run `VACUUM` as part of automatic cleanup. Freed pages should remain available for reuse.
- Ensure cleanup is restart-safe and idempotent: leftover terminal rows are discovered by a later process.

### Scheduling

- Mark a queue as needing maintenance after terminal transitions instead of starting cleanup for every job.
- Throttle cleanup frequency and run at most one bounded batch per turn before yielding to queue work.
- Continue later while eligible rows remain, without waking unrelated queues.
- Stop maintenance timers during worker shutdown and avoid keeping an otherwise idle process alive.

### Validation

- Test zero retention, retained completed jobs, separate failed retention, process restart, concurrent completion and cleanup, and batch boundaries.
- Verify cleanup never deletes pending or active jobs and never changes lease outcomes.
- Extend the retention benchmark with production cleanup, event-loop stalls, batch-size comparisons, and warm/reopened databases.
- Document that `DELETE` reuses pages but does not necessarily shrink the database file.

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
- Before the first release, incompatible development databases are recreated rather than migrated.
- Reject configuration from inside a caller-managed transaction when SQLite cannot apply it safely.
- Keep network-filesystem and single-writer limitations explicit in the adapter documentation.
- Ensure repeated configuration is idempotent.

### Validation

- Test fresh file-backed databases, repeated calls, option overrides, invalid transaction context, and rejection after schema initialization.
- Benchmark default versus configured connections for throughput, writer latency, WAL growth, checkpoint stalls, and incremental-vacuum batch sizes.
- Document when configuration must run, which operations may block, and which maintenance remains the caller's responsibility.
