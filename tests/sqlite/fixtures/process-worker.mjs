import { once } from 'node:events'
import { DatabaseSync } from 'node:sqlite'

import { sqlite } from '../../../packages/sqlite/dist/index.js'

const [path, method, serialized] = process.argv.slice(2)
const db = new DatabaseSync(path)

try {
  db.exec('PRAGMA busy_timeout = 5000')
  const storage = sqlite(db)
  const start = once(process.stdin, 'data')
  console.log('ready')
  await start

  const result = await storage[method](JSON.parse(serialized))
  console.log(JSON.stringify(result))
} finally {
  db.close()
}
