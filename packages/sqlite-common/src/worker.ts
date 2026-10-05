import { parentPort, Worker } from 'node:worker_threads'

import type { Storage } from '@walq/core/storage'
import { deserializeError, serializeError, type ErrorObject } from 'serialize-error'

import {
  storageFacade,
  storageMethods,
  type ManagedStorage,
  type StorageOptions,
} from './managed.js'

interface Request {
  id: number
  method: keyof Storage | 'close'
  input?: unknown
}

interface Response {
  id: number
  result?: unknown
  error?: ErrorObject
}

interface Pending {
  resolve: (value: unknown) => void
  reject: (error: Error) => void
}

export async function createWorkerStorage(
  url: URL,
  options: StorageOptions,
): Promise<ManagedStorage> {
  const maxPending = options.maxPending ?? 1024
  if (!Number.isSafeInteger(maxPending) || maxPending < 1) {
    throw new RangeError('maxPending must be a positive safe integer')
  }

  const worker = new Worker(url, { workerData: { ...options, worker: false } })
  const pending = new Map<number, Pending>()
  let sequence = 0
  let failure: Error | undefined
  let closing: Promise<void> | undefined
  let resolveExit: () => void
  const exited = new Promise<void>((resolve) => {
    resolveExit = resolve
  })

  function fail(error: Error): void {
    failure ??= error
    for (const request of pending.values()) request.reject(failure)
    pending.clear()
  }

  function request(method: Request['method'], input?: unknown): Promise<unknown> {
    if (failure) return Promise.reject(failure)

    return new Promise((resolve, reject) => {
      const id = ++sequence
      pending.set(id, { resolve, reject })

      try {
        worker.postMessage({ id, method, input } satisfies Request)
      } catch (error) {
        pending.delete(id)
        reject(error)
      }
    })
  }

  const ready = new Promise<unknown>((resolve, reject) => {
    pending.set(0, { resolve, reject })
  })
  worker.on('message', (message: Response) => {
    const request = pending.get(message.id)
    if (!request) return

    pending.delete(message.id)
    if (message.error) request.reject(deserializeError(message.error))
    else request.resolve(message.result)
  })
  worker.on('error', fail)
  worker.on('exit', (code) => {
    fail(new Error(`Storage worker exited (${code})`))
    resolveExit()
  })

  try {
    await ready
  } catch (error) {
    await worker.terminate()
    throw error
  }

  return storageFacade(
    (method, input) => {
      if (closing) return Promise.reject(new Error('Storage is closed'))
      if (pending.size >= maxPending)
        return Promise.reject(new Error('Storage worker is at capacity'))

      return request(method, input)
    },
    () => {
      closing ??= request('close').then(async () => {
        await exited
      })

      return closing
    },
  )
}

/** Serve operations serially so close drains every accepted request. */
export function serveStorage(storage: ManagedStorage): void {
  const port = parentPort
  if (!port) throw new Error('Storage server requires a worker thread')

  let tail = Promise.resolve()
  port.on('message', (message: Request) => {
    tail = tail.then(async () => {
      try {
        if (message.method !== 'close' && !storageMethods.includes(message.method)) {
          throw new Error('Unknown storage method')
        }

        const operation = storage[message.method] as (input: unknown) => Promise<unknown>
        const result = await operation.call(storage, message.input)
        port.postMessage({ id: message.id, result } satisfies Response)
      } catch (error) {
        port.postMessage({
          id: message.id,
          error: serializeError(error, { maxDepth: 8, useToJSON: false }),
        } satisfies Response)
      } finally {
        if (message.method === 'close') port.close()
      }
    })
  })
  port.postMessage({ id: 0 } satisfies Response)
}
