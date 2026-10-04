import { describe, expect, it } from 'vitest'

import { createWorkerStorage } from './worker.js'

function script(source: string): URL {
  return new URL(`data:text/javascript,${encodeURIComponent(source)}`)
}

const prelude = `
import { parentPort } from 'node:worker_threads'
parentPort.postMessage({ id: 0 })
`

const input = { queue: 'test' }

describe('storage worker lifecycle', () => {
  it('rejects startup failures', async () => {
    await expect(
      createWorkerStorage(script("throw new Error('startup failed')"), { filename: ':memory:' }),
    ).rejects.toThrow('startup failed')
  })

  it('rejects outstanding and subsequent calls after unexpected exit', async () => {
    const storage = await createWorkerStorage(
      script(`${prelude}
parentPort.on('message', () => process.exit(7))
`),
      { filename: ':memory:' },
    )
    const first = storage.count(input)
    const second = storage.count(input)

    await expect(first).rejects.toThrow('Storage worker exited (7)')
    await expect(second).rejects.toThrow('Storage worker exited (7)')
    await expect(storage.count(input)).rejects.toThrow('Storage worker exited (7)')
    await expect(storage.close()).rejects.toThrow('Storage worker exited (7)')
  })

  it('bounds outstanding calls and still permits draining close', async () => {
    const storage = await createWorkerStorage(
      script(`${prelude}
parentPort.on('message', (message) => {
  setTimeout(() => {
    parentPort.postMessage({ id: message.id, result: null })
    if (message.method === 'close') parentPort.close()
  }, 50)
})
`),
      { filename: ':memory:', maxPending: 1 },
    )

    try {
      const accepted = storage.count(input)
      await expect(storage.count(input)).rejects.toThrow('at capacity')
      const closed = storage.close()
      await accepted
      await closed
    } finally {
      await storage.close()
    }
  })

  it('recovers from uncloneable input without leaking pending capacity', async () => {
    const storage = await createWorkerStorage(
      script(`${prelude}
parentPort.on('message', (message) => {
  parentPort.postMessage({ id: message.id, result: null })
  if (message.method === 'close') parentPort.close()
})
`),
      { filename: ':memory:', maxPending: 1 },
    )

    try {
      await expect(storage.count({ queue: (() => {}) as unknown as string })).rejects.toThrow(
        /could not be cloned/,
      )
      await expect(storage.count(input)).resolves.toBeNull()
    } finally {
      await storage.close()
    }
  })

  it('validates pending capacity before starting a worker', async () => {
    await expect(
      createWorkerStorage(script(prelude), { filename: ':memory:', maxPending: 0 }),
    ).rejects.toThrow('maxPending')
  })
})
