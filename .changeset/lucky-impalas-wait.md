---
'@walq/sqlite-common': patch
'@walq/sqlite': patch
'@walq/better-sqlite3': patch
---

Use serialize-error for storage worker error transport so causes, SQLite diagnostic fields, and custom enumerable properties are preserved. Error properties are serialized with bounded depth and without invoking custom toJSON hooks. Public APIs and SQL execution policies are unchanged.
