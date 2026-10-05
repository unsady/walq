import type { Storage } from '@walq/core/storage'
import {
  createStorage as createSqliteStorage,
  createWorkerStorage,
  type Connection,
  defaultInitialization,
  manageStorage,
  type ManagedStorage,
  type StorageOptions,
} from '@walq/sqlite-common'
import Database from 'better-sqlite3'

/** The caller owns the connection and configures its durability and busy timeout. */
export function betterSqlite3(db: Database.Database): Storage {
  const connection: Connection = {
    get inTransaction() {
      return db.inTransaction
    },
    exec: (sql) => db.exec(sql),
    prepare: (sql) => db.prepare(sql).safeIntegers(false),
    transaction: (callback) => db.transaction(callback),
  }

  return createSqliteStorage(connection)
}

export type { ManagedStorage, StorageOptions } from '@walq/sqlite-common'

/** Open and own a connection, optionally in a dedicated worker thread. */
export async function createStorage(options: StorageOptions): Promise<ManagedStorage> {
  if (options.worker) return createWorkerStorage(new URL('./worker.js', import.meta.url), options)

  const db = new Database(options.filename)

  try {
    db.exec(options.initialization ?? defaultInitialization)

    return manageStorage(betterSqlite3(db), () => db.close())
  } catch (error) {
    try {
      db.close()
    } catch (closeError) {
      throw new AggregateError([error, closeError], 'Storage initialization and close failed', {
        cause: error,
      })
    }

    throw error
  }
}
