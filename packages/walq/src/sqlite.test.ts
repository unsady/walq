import { betterSqlite3 } from '@walq/better-sqlite3'
import Database from 'better-sqlite3'
import { afterEach, expect, it } from 'vitest'

import { Queue } from './index.js'

const databases: Database.Database[] = []

function openStorage() {
  const db = new Database(':memory:')
  databases.push(db)
  return betterSqlite3(db)
}

afterEach(() => {
  for (const db of databases.splice(0)) if (db.open) db.close()
})

it('processes a job through the SQLite storage adapter', async () => {
  const storage = openStorage()
  const queue = new Queue<{ userId: string }>('email', { storage })
  let resolveHandled: (userId: string) => void
  const handled = new Promise<string>((resolve) => {
    resolveHandled = resolve
  })
  const worker = queue.process(async ({ userId }) => {
    resolveHandled(userId)
  })

  const added = await queue.add({ userId: '123' })
  await expect(handled).resolves.toBe('123')
  await worker.close()

  expect(added.id).toEqual(expect.any(String))
  expect(
    await storage.claim({ queue: 'email', limit: 1, now: Date.now(), leaseDuration: 30_000 }),
  ).toEqual([])
})

it('immediately retries failed handlers while attempts remain', async () => {
  const storage = openStorage()
  const queue = new Queue('email', { storage, attempts: 2 })
  let calls = 0
  let resolveHandled: () => void
  const handled = new Promise<void>((resolve) => {
    resolveHandled = resolve
  })
  const worker = queue.process(async () => {
    calls += 1
    if (calls === 1) throw new Error('temporary')
    resolveHandled()
  })

  await queue.add({ userId: '123' })
  await handled
  await worker.close()

  expect(calls).toBe(2)
})
