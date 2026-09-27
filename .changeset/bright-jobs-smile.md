---
'@walq/core': minor
'@walq/better-sqlite3': minor
---

Add signed integer job priorities to enqueue options. Higher-priority due jobs are claimed first, and SQLite migrates existing jobs with priority 0.
