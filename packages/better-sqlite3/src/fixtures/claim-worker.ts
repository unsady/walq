import { registerHooks } from 'node:module'
import { DatabaseSync } from 'node:sqlite'
import { parentPort, workerData } from 'node:worker_threads'

import Database from 'better-sqlite3'

// Native Node type stripping does not remap the source's NodeNext .js imports.
registerHooks({
  resolve(specifier, context, nextResolve) {
    if (specifier === '@walq/sqlite-common') {
      return nextResolve(
        new URL('../../../sqlite-common/src/index.ts', import.meta.url).href,
        context,
      )
    }
    if (specifier.startsWith('./') && specifier.endsWith('.js')) {
      return nextResolve(`${specifier.slice(0, -3)}.ts`, context)
    }
    return nextResolve(specifier, context)
  },
})

const { betterSqlite3 } = await import(new URL('../index.ts', import.meta.url).href)
const { sqlite } = await import(new URL('../../../sqlite/src/index.ts', import.meta.url).href)
const db =
  workerData.driver === 'node:sqlite'
    ? new DatabaseSync(workerData.path)
    : new Database(workerData.path)
try {
  db.exec('PRAGMA busy_timeout = 5000')
  const storage = db instanceof DatabaseSync ? sqlite(db) : betterSqlite3(db)
  const gate = new Int32Array(workerData.gate)
  parentPort!.postMessage({ ready: true })
  for (;;) {
    const count = Atomics.load(gate, 0)
    if (count === workerData.count) break
    Atomics.wait(gate, 0, count)
  }
  const result = await storage[workerData.method](workerData.input)
  parentPort!.postMessage({ result })
} finally {
  db.close()
}
