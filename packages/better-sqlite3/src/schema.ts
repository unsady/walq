import type Database from 'better-sqlite3'

const schemaVersion = 6
const legacySchemaVersion = 5
const maxPriority = Number.MAX_SAFE_INTEGER

function jobsTable(name: string, withSequence = true): string {
  return `
    CREATE TABLE ${name} (
      ${withSequence ? 'seq INTEGER PRIMARY KEY AUTOINCREMENT,' : ''}
      id TEXT ${withSequence ? 'NOT NULL COLLATE BINARY UNIQUE' : 'PRIMARY KEY NOT NULL COLLATE BINARY'},
      queue TEXT NOT NULL COLLATE BINARY,
      name TEXT NOT NULL,
      data TEXT NOT NULL,
      status TEXT NOT NULL CHECK (status IN ('pending', 'active', 'completed', 'failed', 'cancelled')),
      createdAt INTEGER NOT NULL CHECK (createdAt >= 0),
      availableAt INTEGER NOT NULL CHECK (availableAt >= 0),
      priority INTEGER NOT NULL CHECK (priority >= ${-maxPriority} AND priority <= ${maxPriority}),
      dedupe TEXT,
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
        (status IN ('completed', 'failed', 'cancelled') AND finishedAt IS NOT NULL)
        OR (status NOT IN ('completed', 'failed', 'cancelled') AND finishedAt IS NULL)
      )
    );
  `
}

const legacyIndexes = `
  CREATE INDEX walq_pending ON walq_jobs (queue, priority DESC, availableAt, id) WHERE status = 'pending';
  CREATE INDEX walq_active ON walq_jobs (queue, expiresAt, id) WHERE status = 'active';
  CREATE INDEX walq_terminal ON walq_jobs (queue, status, finishedAt DESC, id DESC)
    WHERE finishedAt IS NOT NULL;
  CREATE UNIQUE INDEX walq_dedupe ON walq_jobs (queue, dedupe) WHERE dedupe IS NOT NULL;
`

const indexes = `
  CREATE INDEX walq_pending ON walq_jobs (queue, priority DESC, availableAt, seq) WHERE status = 'pending';
  CREATE INDEX walq_active ON walq_jobs (queue, expiresAt, id) WHERE status = 'active';
  CREATE INDEX walq_terminal ON walq_jobs (queue, status, finishedAt DESC, id DESC)
    WHERE finishedAt IS NOT NULL;
  CREATE UNIQUE INDEX walq_dedupe ON walq_jobs (queue, dedupe) WHERE dedupe IS NOT NULL;
`

function migrateV2(db: Database.Database): void {
  db.exec(`
    ${jobsTable('walq_jobs_v5', false)}
    INSERT INTO walq_jobs_v5 (
      id, queue, name, data, status, createdAt, availableAt, priority, finishedAt,
      attemptsMade, attempts, error, leaseToken, expiresAt
    )
    SELECT
      id, queue, name, data, status, createdAt, availableAt, 0, finishedAt,
      attemptsMade, attempts, error, leaseToken, expiresAt
    FROM walq_jobs;
    DROP TABLE walq_jobs;
    ALTER TABLE walq_jobs_v5 RENAME TO walq_jobs;
    ${legacyIndexes}
    UPDATE walq_schema SET version = ${legacySchemaVersion} WHERE id = 1 AND version = 2;
  `)
}

function migrateV3(db: Database.Database): void {
  db.exec(`
    ALTER TABLE walq_jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0
      CHECK (priority >= ${-maxPriority} AND priority <= ${maxPriority});
    DROP INDEX walq_pending;
    CREATE INDEX walq_pending ON walq_jobs (queue, priority DESC, availableAt, id)
      WHERE status = 'pending';
    ALTER TABLE walq_jobs ADD COLUMN dedupe TEXT;
    CREATE UNIQUE INDEX walq_dedupe ON walq_jobs (queue, dedupe) WHERE dedupe IS NOT NULL;
    UPDATE walq_schema SET version = ${legacySchemaVersion} WHERE id = 1 AND version = 3;
  `)
}

function migrateV4(db: Database.Database): void {
  db.exec(`
    ALTER TABLE walq_jobs ADD COLUMN dedupe TEXT;
    CREATE UNIQUE INDEX walq_dedupe ON walq_jobs (queue, dedupe) WHERE dedupe IS NOT NULL;
    UPDATE walq_schema SET version = ${legacySchemaVersion} WHERE id = 1 AND version = 4;
  `)
}

function migrateV5(db: Database.Database): void {
  db.exec(`
    ${jobsTable('walq_jobs_v6')}
    INSERT INTO walq_jobs_v6 (
      seq, id, queue, name, data, status, createdAt, availableAt, priority, dedupe, finishedAt,
      attemptsMade, attempts, error, leaseToken, expiresAt
    )
    SELECT
      ROW_NUMBER() OVER (ORDER BY createdAt, id COLLATE BINARY),
      id, queue, name, data, status, createdAt, availableAt, priority, dedupe, finishedAt,
      attemptsMade, attempts, error, leaseToken, expiresAt
    FROM walq_jobs;
    DROP TABLE walq_jobs;
    ALTER TABLE walq_jobs_v6 RENAME TO walq_jobs;
    ${indexes}
    UPDATE walq_schema SET version = ${schemaVersion} WHERE id = 1 AND version = ${legacySchemaVersion};
  `)
}

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
      if (row.version === 2) migrateV2(db)
      else if (row.version === 3) migrateV3(db)
      else if (row.version === 4) migrateV4(db)
      else if (row.version !== legacySchemaVersion && row.version !== schemaVersion) {
        throw new Error(`Unsupported walq schema version: ${row.version}`)
      }

      if (row.version !== schemaVersion) migrateV5(db)
      return
    }

    db.exec(`
      ${jobsTable('walq_jobs')}
      ${indexes}
      INSERT INTO walq_schema (id, version) VALUES (1, ${schemaVersion});
    `)
  }).immediate()
}
