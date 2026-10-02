import type { Storage } from '@walq/core/storage'
import { createStorage, type Connection } from '@walq/sqlite-common'
import type Database from 'better-sqlite3'

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

  return createStorage(connection)
}
