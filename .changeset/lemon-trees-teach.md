---
'@walq/sqlite': patch
'@walq/better-sqlite3': patch
---

Preserve both the original and cleanup errors when node:sqlite rollback fails or a managed adapter cannot close after initialization failure. These failures reject with a standard AggregateError containing both errors and the original as cause. Successful cleanup still rethrows the original error unchanged.
