import { registerHooks } from 'node:module'
import { parentPort, workerData } from 'node:worker_threads'

import Database from 'better-sqlite3'

// Native Node type stripping does not remap the source's NodeNext .js imports.
registerHooks({
  resolve(specifier, context, nextResolve) {
    if (specifier.startsWith('./') && specifier.endsWith('.js')) {
      return nextResolve(`${specifier.slice(0, -3)}.ts`, context)
    }
    return nextResolve(specifier, context)
  },
})

const { betterSqlite3 } = await import(new URL('../index.ts', import.meta.url).href)
const db = new Database(workerData.path)
try {
  const storage = betterSqlite3(db)
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
