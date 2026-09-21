import Database from 'better-sqlite3'
import type { Storage } from 'walq/storage'

import {
  runCleanupConformance,
  runGroupedClaimConformance,
  runStorageConformance,
} from '../../../tests/storage/conformance.js'
import { betterSqlite3 } from './index.js'

const databases: Database.Database[] = []

function createStorage(): Storage {
  const db = new Database(':memory:')
  databases.push(db)
  return betterSqlite3(db)
}

async function cleanup(): Promise<void> {
  for (const db of databases.splice(0)) {
    if (db.open) db.close()
  }
}

runStorageConformance(createStorage, cleanup)
runGroupedClaimConformance(createStorage, cleanup)
runCleanupConformance(createStorage, cleanup)
