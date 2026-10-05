# @walq/core

## 1.2.2

### Patch Changes

- fd98379: Report malformed job JSON as `onError` operation `parse` instead of `handler`, with the same job ID, attempt, and attemptsExhausted fields. Handlers still receive only valid payloads, and retry/backoff behavior is unchanged in both process and processMany. Consumers switching on context.operation should handle the new parse operation.
- e8af85e: Report unexpected worker polling failures and reject malformed grouped claim results instead of treating missing results as empty claims.
- 946b1b6: Preserve the original error and context when onError fails, retain cron validation causes, and handle unexpected background rejections without letting logging failures disrupt polling. Public APIs and retry policies are unchanged.
- e66742d: Document error logging, direct-call rejections, worker diagnostics, and aggregate cleanup errors.
- 2fdd44c: Include error cause chains in persisted job.error diagnostic text. Formatting is depth-bounded, handles circular references, and does not invoke custom inspection hooks. Errors without causes keep their existing text; onError still receives the original error and retry behavior is unchanged.

## 1.2.1

### Patch Changes

- eab33ed: Improve README quick starts, adapter selection, caller-owned connection examples, and code examples for queue features.

## 1.2.0

Version alignment with the managed SQLite storage release; no core API changes.

## 1.1.0

Version alignment with the SQLite adapter release; no core API changes.

## 1.0.0

### Major Changes

- a85fba2: Move handler retry backoff configuration from the `retry` wrapper to the top-level `backoff` queue option.

### Minor Changes

- 1443345: Add signed integer job priorities to enqueue options. Higher-priority due jobs are claimed first, and SQLite migrates existing jobs with priority 0.
- bee32f6: Add durable queue pause and resume operations that prevent new claims and schedule materialization.
- fed2ca5: Add durable repeating schedules with interval and UTC cron support, atomic SQLite materialization, and schedule schema migration.
- 9bb156c: Add queue-scoped job groups with persistent, globally enforced concurrency limits.
- 8702ba8: Add `queue.stats()` for persisted per-status queue counts and require the storage count operation.
- efb5221: Break claim and pending-list ties by original enqueue order instead of job ID. SQLite migrates schema versions 2–5 to v6 with a deterministic approximate historical order for existing jobs.
- 6cafa15: Add queue-scoped job deduplication keys to `queue.add()` and `queue.addMany()`. SQLite migrates schema v4 to v5 with persisted unique keys.
- 0ede0d7: Add `queue.processMany()` for lease-based batch processing with configurable `batch` size and concurrency.

## 0.3.0

### Minor Changes

- 3f74a4c: Add public job inspection, listing, retry, cancellation, rescheduling, and removal APIs. The SQLite adapter automatically migrates the schema from v2 to v3 to support the `cancelled` status.

## 0.2.1

### Patch Changes

- Change retry backoff jitter to reduce delays, with jitter `1` providing full jitter from zero up to the base delay.

## 0.2.0

### Minor Changes

- 911499e: Add atomic `queue.addMany()` support and require storage adapters to implement `enqueueMany()`.

### Patch Changes

- b619265: Add fixed and exponential handler retry backoff with positive-only jitter.
- f24e1e7: Support delayed and scheduled jobs through `queue.add()` options.
