export type Delay = {
  promise: Promise<void>
  finish(): void
}

export function delay(duration: number): Delay {
  let settled = false
  let resolvePromise: () => void
  const promise = new Promise<void>((resolve) => {
    resolvePromise = resolve
  })
  const timer = setTimeout(finish, duration)

  function finish(): void {
    if (settled) return
    settled = true
    clearTimeout(timer)
    resolvePromise()
  }

  return { promise, finish }
}

export type Deferred = {
  promise: Promise<void>
  resolve(): void
}

export function deferred(): Deferred {
  let resolvePromise: () => void
  const promise = new Promise<void>((resolve) => {
    resolvePromise = resolve
  })
  return { promise, resolve: () => resolvePromise() }
}
