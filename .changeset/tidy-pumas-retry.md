---
'@walq/core': minor
'@walq/better-sqlite3': minor
---

Add public job inspection, listing, retry, cancellation, rescheduling, and removal APIs. The SQLite adapter automatically migrates the schema from v2 to v3 to support the `cancelled` status.
