import type { CompleteInput } from '@walq/core/storage'

export function integer(value: number, name: string, minimum = 0): void {
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new TypeError(`${name} must be a safe integer >= ${minimum}`)
  }
}

export function text(value: string, name: string, nonempty = true): void {
  if (typeof value !== 'string' || (nonempty && value.length === 0)) {
    throw new TypeError(`${name} must be ${nonempty ? 'a nonempty' : 'a'} string`)
  }
}

export function expiry(now: number, duration: number): number {
  integer(now, 'now')
  integer(duration, 'leaseDuration', 1)
  const result = now + duration
  integer(result, 'expiresAt')
  return result
}

export function lease(input: CompleteInput): void {
  text(input.id, 'id')
  text(input.leaseToken, 'leaseToken')
  integer(input.now, 'now')
}
