# Roadmap

## Terminal-job retention

Production cleanup batches, event-loop delays, and active processing after
cleanup are benchmarked. Remaining work:

- Check cleanup-call scaling against retained history with repeated full-grid runs.
- Cover concurrent completion and cleanup beyond per-call conformance.
- Consider cleanup metrics: rows removed, pass count, and backlog.

## Opt-in adapter configuration

Add an explicit helper such as `configureBetterSqlite3(db, options)`, called
before storage schema initialization. Keep connection ownership with the caller.

- Support WAL, synchronous mode, busy timeout, and schema-time incremental auto-vacuum.
- Make applied settings observable and durability-sensitive defaults overridable.
- Keep checkpoint/vacuum maintenance explicit; do not change unrelated pragmas.
- Reject late schema-time configuration and unsupported transaction contexts.
- Keep repeated configuration idempotent.

Before shipping, test fresh file-backed databases, overrides, repeated calls,
and invalid initialization contexts. Compare durability modes, writer latency,
WAL growth, and maintenance stalls. Document lifecycle ordering and SQLite's
single-writer/network-filesystem limitations.
