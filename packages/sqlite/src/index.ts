import { DatabaseSync, type SQLInputValue } from 'node:sqlite'

import type { Storage } from '@walq/core/storage'
import {
  createStorage as createSqliteStorage,
  createWorkerStorage,
  type Connection,
  type Transaction,
  defaultInitialization,
  manageStorage,
  type ManagedStorage,
  type StorageOptions,
} from '@walq/sqlite-common'

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

  return createSqliteStorage(connection)
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
        try {
          if (db.isTransaction) db.exec('ROLLBACK')
        } catch (rollbackError) {
          throw new AggregateError([error, rollbackError], 'Transaction and rollback failed', {
            cause: error,
          })
        }

        throw error
      }
    },
  }
}

export type { ManagedStorage, StorageOptions } from '@walq/sqlite-common'

/** Open and own a connection, optionally in a dedicated worker thread. */
export async function createStorage(options: StorageOptions): Promise<ManagedStorage> {
  if (options.worker) return createWorkerStorage(new URL('./worker.js', import.meta.url), options)

  const db = new DatabaseSync(options.filename)

  try {
    db.exec(options.initialization ?? defaultInitialization)

    return manageStorage(sqlite(db), () => db.close())
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
