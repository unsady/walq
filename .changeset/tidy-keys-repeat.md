---
'@walq/core': minor
'@walq/better-sqlite3': minor
---

Add queue-scoped job deduplication keys to `queue.add()` and `queue.addMany()`. SQLite migrates schema v4 to v5 with persisted unique keys.
