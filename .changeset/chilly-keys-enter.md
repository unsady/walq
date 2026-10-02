---
'@walq/sqlite-common': minor
'@walq/better-sqlite3': patch
'@walq/sqlite': patch
---

Move the shared SQLite storage implementation into @walq/sqlite-common so each adapter can be installed independently. Remove the @walq/sqlite/internal entry point; shared executor contracts remain implementation details.
