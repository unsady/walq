/** The synchronous SQLite operations used by the shared storage implementation. */
export interface Statement {
  get(parameters?: object): unknown
  all(parameters?: object): unknown[]
  run(parameters?: object): { changes: number }
}

export interface Transaction<Args extends unknown[], Result> {
  immediate(...args: Args): Result
}

export interface Connection {
  readonly inTransaction: boolean
  exec(sql: string): unknown
  prepare(sql: string): Statement
  transaction<Args extends unknown[], Result>(
    callback: (...args: Args) => Result,
  ): Transaction<Args, Result>
}
