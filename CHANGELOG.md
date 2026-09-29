# Changelog

Notable changes to `@walq/core` and `@walq/better-sqlite3` are documented here.

## 1.0.0

- Move handler retry backoff to the top-level `backoff` queue option; update existing callers and custom storage adapters for the new API.
- Add batch processing, job priorities, deduplication, queue-scoped groups, durable pause/resume and repeating schedules, and persisted job counts.
- The SQLite adapter migrates supported existing schemas (v2–v10) to v12; v11 is unsupported. See [release notes](release-notes/v1.0.0.md) for migration details.

## 0.1.0

- Initial release of the typed queue runtime.
- Initial `better-sqlite3` storage adapter.
- Lease-based at-least-once processing, retries, heartbeats, grouped claims, and terminal-job retention.
