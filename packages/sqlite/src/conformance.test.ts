import { DatabaseSync } from 'node:sqlite'

import type { Storage } from '@walq/core/storage'
import { describe, it, beforeEach, afterEach, expect } from 'vitest'

import {
  runCleanupConformance,
  runGroupedClaimConformance,
  runStorageConformance,
} from '../../../tests/storage/conformance.js'
import { sqlite } from './index.js'

const databases: DatabaseSync[] = []

function createStorage(): Storage {
  const db = new DatabaseSync(':memory:')
  databases.push(db)
  return sqlite(db)
}

async function cleanup(): Promise<void> {
  for (const db of databases.splice(0)) {
    if (db.isOpen) db.close()
  }
}

runStorageConformance(createStorage, cleanup, { describe, it, beforeEach, afterEach, expect })
runGroupedClaimConformance(createStorage, cleanup, { describe, it, beforeEach, afterEach, expect })
runCleanupConformance(createStorage, cleanup, { describe, it, beforeEach, afterEach, expect })
