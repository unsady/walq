import type { Storage } from '@walq/core/storage'

export interface StorageOptions {
  filename: string
  /** Run the connection in a dedicated thread. Defaults to false. */
  worker?: boolean
  /** SQL executed before schema initialization; overrides the default pragmas. */
  initialization?: string
  /** Maximum outstanding calls in worker mode. Defaults to 1024. */
  maxPending?: number
}

export interface ManagedStorage extends Storage {
  /** Reject new calls, drain accepted calls, and close the owned connection. */
  close(): Promise<void>
}

export const defaultInitialization =
  'PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL; PRAGMA busy_timeout = 5000'

const methods: Record<keyof Storage, true> = {
  enqueue: true,
  enqueueMany: true,
  pause: true,
  resume: true,
  claim: true,
  claimQueues: true,
  inspect: true,
  count: true,
  list: true,
  retry: true,
  cancel: true,
  reschedule: true,
  remove: true,
  complete: true,
  fail: true,
  heartbeat: true,
  cleanup: true,
  upsertSchedule: true,
  getSchedule: true,
  removeSchedule: true,
  materializeSchedules: true,
}

export const storageMethods = Object.keys(methods) as (keyof Storage)[]

export interface Invoke {
  (method: keyof Storage, input: unknown): Promise<unknown>
}

export function storageFacade(invoke: Invoke, close: () => Promise<void>): ManagedStorage {
  return Object.fromEntries([
    ...storageMethods.map((method) => [method, (input: unknown) => invoke(method, input)]),
    ['close', close],
  ]) as unknown as ManagedStorage
}

export function manageStorage(storage: Storage, dispose: () => void): ManagedStorage {
  const pending = new Set<Promise<unknown>>()
  let closing: Promise<void> | undefined

  return storageFacade(
    (method, input) => {
      if (closing) return Promise.reject(new Error('Storage is closed'))

      const operation = storage[method] as (input: unknown) => Promise<unknown>
      const task = Promise.resolve().then(() => operation.call(storage, input))
      pending.add(task)
      void task.then(
        () => pending.delete(task),
        () => pending.delete(task),
      )

      return task
    },
    () => {
      closing ??= Promise.allSettled(pending).then(() => dispose())

      return closing
    },
  )
}
