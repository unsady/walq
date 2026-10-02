import type { Connection } from './driver.js'

// Version 11 was used by an unpublished ready-flag experiment; do not reuse it.
const schemaVersion = 12
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

function migrateV2(db: Connection): void {
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

function migrateV3(db: Connection): void {
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

function migrateV4(db: Connection): void {
  db.exec(`
    ALTER TABLE walq_jobs ADD COLUMN dedupe TEXT;
    CREATE UNIQUE INDEX walq_dedupe ON walq_jobs (queue, dedupe) WHERE dedupe IS NOT NULL;
    UPDATE walq_schema SET version = ${legacySchemaVersion} WHERE id = 1 AND version = 4;
  `)
}

function migrateV5(db: Connection): void {
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
    UPDATE walq_schema SET version = 6 WHERE id = 1 AND version = ${legacySchemaVersion};
  `)
}

function migrateV6(db: Connection): void {
  db.exec(`
    ALTER TABLE walq_jobs ADD COLUMN groupId TEXT;
    CREATE TABLE walq_groups (
      queue TEXT NOT NULL COLLATE BINARY,
      id TEXT NOT NULL COLLATE BINARY,
      concurrency INTEGER NOT NULL CHECK (concurrency > 0),
      PRIMARY KEY (queue, id)
    ) WITHOUT ROWID;
    UPDATE walq_schema SET version = 7 WHERE id = 1 AND version = 6;
  `)
}

function migrateV7(db: Connection): void {
  db.exec(`
    CREATE INDEX walq_active_group ON walq_jobs (queue, groupId)
      WHERE status = 'active' AND groupId IS NOT NULL;
    UPDATE walq_schema SET version = 8 WHERE id = 1 AND version = 7;
  `)
}

const pausedQueuesTable = `
  CREATE TABLE walq_paused_queues (
    queue TEXT NOT NULL COLLATE BINARY PRIMARY KEY
  ) WITHOUT ROWID;
`

const schedulesTable = `
  CREATE TABLE walq_schedules (
    queue TEXT NOT NULL COLLATE BINARY,
    id TEXT NOT NULL COLLATE BINARY,
    data TEXT NOT NULL,
    every INTEGER CHECK (every IS NULL OR every > 0),
    cron TEXT,
    nextRunAt INTEGER NOT NULL CHECK (nextRunAt >= 0),
    CHECK ((every IS NOT NULL AND cron IS NULL) OR (every IS NULL AND cron IS NOT NULL)),
    PRIMARY KEY (queue, id)
  ) WITHOUT ROWID;
  CREATE INDEX walq_schedules_due ON walq_schedules (queue, nextRunAt, id);
`

function migrateV8(db: Connection): void {
  db.exec(`
    ${schedulesTable}
    UPDATE walq_schema SET version = 9 WHERE id = 1 AND version = 8;
  `)
}

const groupScheduling = `
  DROP INDEX walq_pending;
  CREATE INDEX walq_pending ON walq_jobs (queue, priority DESC, availableAt, seq)
    WHERE status = 'pending' AND groupId IS NULL;
  CREATE INDEX IF NOT EXISTS walq_pending_grouped
    ON walq_jobs (queue, groupId, priority DESC, availableAt, seq)
    WHERE status = 'pending' AND groupId IS NOT NULL;
  CREATE INDEX walq_groups_eligible ON walq_groups (queue, id)
    WHERE pendingCount > 0 AND activeCount < concurrency;
  CREATE TABLE walq_group_cursor (
    queue TEXT NOT NULL COLLATE BINARY PRIMARY KEY,
    lastGroupId TEXT NOT NULL COLLATE BINARY
  ) WITHOUT ROWID;
  CREATE TRIGGER walq_group_counts_insert AFTER INSERT ON walq_jobs
    WHEN NEW.groupId IS NOT NULL AND NEW.status IN ('pending', 'active')
    BEGIN
      UPDATE walq_groups SET
        pendingCount = pendingCount + (NEW.status = 'pending'),
        activeCount = activeCount + (NEW.status = 'active')
      WHERE queue = NEW.queue AND id = NEW.groupId;
    END;
  CREATE TRIGGER walq_group_counts_delete AFTER DELETE ON walq_jobs
    WHEN OLD.groupId IS NOT NULL AND OLD.status IN ('pending', 'active')
    BEGIN
      UPDATE walq_groups SET
        pendingCount = pendingCount - (OLD.status = 'pending'),
        activeCount = activeCount - (OLD.status = 'active')
      WHERE queue = OLD.queue AND id = OLD.groupId;
    END;
  CREATE TRIGGER walq_group_counts_update AFTER UPDATE OF status ON walq_jobs
    WHEN OLD.groupId IS NOT NULL AND OLD.status != NEW.status
    BEGIN
      UPDATE walq_groups SET
        pendingCount = pendingCount + (NEW.status = 'pending') - (OLD.status = 'pending'),
        activeCount = activeCount + (NEW.status = 'active') - (OLD.status = 'active')
      WHERE queue = NEW.queue AND id = NEW.groupId;
    END;
`

function migrateV10(db: Connection): void {
  db.exec(`
    ALTER TABLE walq_groups ADD COLUMN pendingCount INTEGER NOT NULL DEFAULT 0
      CHECK (pendingCount >= 0);
    ALTER TABLE walq_groups ADD COLUMN activeCount INTEGER NOT NULL DEFAULT 0
      CHECK (activeCount >= 0);
    CREATE INDEX walq_pending_grouped
      ON walq_jobs (queue, groupId, priority DESC, availableAt, seq)
      WHERE status = 'pending' AND groupId IS NOT NULL;
    UPDATE walq_groups SET
      pendingCount = (SELECT count(*) FROM walq_jobs WHERE queue = walq_groups.queue
        AND groupId = walq_groups.id AND status = 'pending'),
      activeCount = (SELECT count(*) FROM walq_jobs WHERE queue = walq_groups.queue
        AND groupId = walq_groups.id AND status = 'active');
    ${groupScheduling}
    DROP INDEX walq_active_group;
    UPDATE walq_schema SET version = ${schemaVersion} WHERE id = 1 AND version = 10;
  `)
}

function migrateV9(db: Connection): void {
  db.exec(`
    ${pausedQueuesTable}
    UPDATE walq_schema SET version = 10 WHERE id = 1 AND version = 9;
  `)
}

export function initialize(db: Connection): void {
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

      .get() as { version: number } | undefined

    if (row) {
      if (row.version === 2) migrateV2(db)
      else if (row.version === 3) migrateV3(db)
      else if (row.version === 4) migrateV4(db)
      else if (
        row.version !== legacySchemaVersion &&
        row.version !== 6 &&
        row.version !== 7 &&
        row.version !== 8 &&
        row.version !== 9 &&
        row.version !== 10 &&
        row.version !== schemaVersion
      ) {
        throw new Error(`Unsupported walq schema version: ${row.version}`)
      }

      if (row.version === 2 || row.version === 3 || row.version === 4 || row.version === 5) {
        migrateV5(db)
      }
      if (row.version <= 6) migrateV6(db)
      if (row.version <= 7) migrateV7(db)
      if (row.version <= 8) migrateV8(db)
      if (row.version <= 9) migrateV9(db)
      if (row.version <= 10) migrateV10(db)
      return
    }

    db.exec(`
      ${jobsTable('walq_jobs')}
      ${indexes}
      ALTER TABLE walq_jobs ADD COLUMN groupId TEXT;
      CREATE TABLE walq_groups (
        queue TEXT NOT NULL COLLATE BINARY,
        id TEXT NOT NULL COLLATE BINARY,
        concurrency INTEGER NOT NULL CHECK (concurrency > 0),
        pendingCount INTEGER NOT NULL DEFAULT 0 CHECK (pendingCount >= 0),
        activeCount INTEGER NOT NULL DEFAULT 0 CHECK (activeCount >= 0),
        PRIMARY KEY (queue, id)
      ) WITHOUT ROWID;
      ${schedulesTable}
      ${pausedQueuesTable}
      ${groupScheduling}
      INSERT INTO walq_schema (id, version) VALUES (1, ${schemaVersion});
    `)
  }).immediate()
}
