import { DatabaseSync } from 'node:sqlite'
import { afterEach, beforeEach, describe, it } from 'node:test'

import { sqlite } from '../../packages/sqlite/dist/index.js'
import {
  runStorageConformance,
  runGroupedClaimConformance,
  runCleanupConformance,
} from '../storage/conformance.ts'

const databases = []

function createStorage() {
  const db = new DatabaseSync(':memory:')
  databases.push(db)

  return sqlite(db)
}

async function cleanup() {
  for (const db of databases.splice(0)) if (db.isOpen) db.close()
}

const { expect } = process.versions.bun ? await import('bun:test') : await import('expect')
const hooks = { describe, it, beforeEach, afterEach, expect }
runStorageConformance(createStorage, cleanup, hooks)
runGroupedClaimConformance(createStorage, cleanup, hooks)
runCleanupConformance(createStorage, cleanup, hooks)
