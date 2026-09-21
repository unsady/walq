import type Database from 'better-sqlite3'

const schemaVersion = 2

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
      if (row.version !== schemaVersion) {
        throw new Error(`Unsupported walq schema version: ${row.version}`)
      }
      return
    }

    db.exec(`
      CREATE TABLE walq_jobs (
        id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
        queue TEXT NOT NULL COLLATE BINARY,
        name TEXT NOT NULL,
        data TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('pending', 'active', 'completed', 'failed')),
        createdAt INTEGER NOT NULL CHECK (createdAt >= 0),
        availableAt INTEGER NOT NULL CHECK (availableAt >= 0),
        finishedAt INTEGER CHECK (finishedAt IS NULL OR finishedAt >= 0),
        attemptsMade INTEGER NOT NULL CHECK (attemptsMade >= 0 AND attemptsMade <= attempts),
        attempts INTEGER NOT NULL CHECK (attempts > 0),
        error TEXT,
        leaseToken TEXT,
        expiresAt INTEGER,
        CHECK (
          (status = 'active' AND leaseToken IS NOT NULL AND expiresAt IS NOT NULL AND expiresAt >= 0)
          OR (status != 'active' AND leaseToken IS NULL AND expiresAt IS NULL)
        ),
        CHECK (
          (status IN ('completed', 'failed') AND finishedAt IS NOT NULL)
          OR (status NOT IN ('completed', 'failed') AND finishedAt IS NULL)
        )
      );
      CREATE INDEX walq_pending ON walq_jobs (queue, availableAt, id) WHERE status = 'pending';
      CREATE INDEX walq_active ON walq_jobs (queue, expiresAt) WHERE status = 'active';
      CREATE INDEX walq_terminal ON walq_jobs (queue, status, finishedAt DESC, id DESC)
        WHERE finishedAt IS NOT NULL;
      INSERT INTO walq_schema (id, version) VALUES (1, ${schemaVersion});
    `)
  }).immediate()
}
