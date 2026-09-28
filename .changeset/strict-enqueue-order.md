---
'@walq/core': minor
'@walq/better-sqlite3': minor
---

Break claim and pending-list ties by original enqueue order instead of job ID. SQLite migrates schema versions 2–5 to v6 with a deterministic approximate historical order for existing jobs.
