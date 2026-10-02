import type { DatabaseSync, SQLInputValue } from 'node:sqlite'

import type { Storage } from '@walq/core/storage'
import { createStorage, type Connection, type Transaction } from '@walq/sqlite-common'

/** The caller owns the connection and configures its durability and busy timeout. */
export function sqlite(db: DatabaseSync): Storage {
  const connection: Connection = {
    get inTransaction() {
      return db.isTransaction
    },
    exec: (sql) => db.exec(sql),
    prepare: (sql) => {
      const statement = db.prepare(sql)
      statement.setAllowBareNamedParameters(true)
      statement.setAllowUnknownNamedParameters(true)
      statement.setReadBigInts(false)

      return {
        get: (parameters) =>
          parameters === undefined
            ? statement.get()
            : statement.get(parameters as Record<string, SQLInputValue>),
        all: (parameters) =>
          parameters === undefined
            ? statement.all()
            : statement.all(parameters as Record<string, SQLInputValue>),
        run: (parameters) => {
          const result =
            parameters === undefined
              ? statement.run()
              : statement.run(parameters as Record<string, SQLInputValue>)

          return { changes: Number(result.changes) }
        },
      }
    },
    transaction: (callback) => immediateTransaction(db, callback),
  }

  return createStorage(connection)
}

function immediateTransaction<Args extends unknown[], Result>(
  db: DatabaseSync,
  callback: (...args: Args) => Result,
): Transaction<Args, Result> {
  return {
    immediate: (...args) => {
      db.exec('BEGIN IMMEDIATE')

      try {
        const result = callback(...args)
        db.exec('COMMIT')

        return result
      } catch (error) {
        // SQLite can roll back automatically after an IO or resource error.
        if (db.isTransaction) db.exec('ROLLBACK')

        throw error
      }
    },
  }
}
