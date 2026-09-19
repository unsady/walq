import type Database from 'better-sqlite3'

export function initialize(db: Database.Database): void {
  if (db.inTransaction) throw new Error('Storage cannot initialize inside a transaction')

  db.transaction(() => {
    db.exec(`
      CREATE TABLE IF NOT EXISTS walq_schema (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        version INTEGER NOT NULL
      );
    `)

    const row = db
      .prepare('SELECT version FROM walq_schema WHERE id = 1')
      .safeIntegers(false)
      .get() as { version: number } | undefined

    if (row) {
      if (row.version !== 1) throw new Error(`Unsupported walq schema version: ${row.version}`)
      return
    }

    db.exec(`
      CREATE TABLE walq_jobs (
        id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
        queue TEXT NOT NULL COLLATE BINARY,
        name TEXT NOT NULL,
        payload TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('pending', 'active', 'completed', 'failed')),
        createdAt INTEGER NOT NULL CHECK (createdAt >= 0),
        availableAt INTEGER NOT NULL CHECK (availableAt >= 0),
        attempts INTEGER NOT NULL CHECK (attempts >= 0 AND attempts <= maxAttempts),
        maxAttempts INTEGER NOT NULL CHECK (maxAttempts > 0),
        error TEXT,
        leaseToken TEXT,
        expiresAt INTEGER,
        CHECK (
          (status = 'active' AND leaseToken IS NOT NULL AND expiresAt IS NOT NULL AND expiresAt >= 0)
          OR (status != 'active' AND leaseToken IS NULL AND expiresAt IS NULL)
        )
      );
      CREATE INDEX walq_pending ON walq_jobs (queue, availableAt, id) WHERE status = 'pending';
      CREATE INDEX walq_active ON walq_jobs (queue, expiresAt) WHERE status = 'active';
      INSERT INTO walq_schema (id, version) VALUES (1, 1);
    `)
  }).immediate()
}
