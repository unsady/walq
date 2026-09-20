export type GridName = 'quick' | 'full'

/** Settings shared by the benchmark files and the custom provider. */
export type BenchEnvironment = {
  grid: GridName
  repeats: number
  warmup: number
  jobs: number | undefined
  only: string | undefined
  json: string | undefined
}

function count(value: string, label: string): number {
  const parsed = Number(value)
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`${label} must be a non-negative integer, received "${value}"`)
  }

  return parsed
}

function positive(value: string, label: string): number {
  const parsed = count(value, label)
  if (parsed === 0) throw new Error(`${label} must be positive, received "${value}"`)

  return parsed
}

function grid(value: string | undefined): GridName {
  if (value === undefined || value === '' || value === 'quick') return 'quick'
  if (value === 'full') return 'full'

  throw new Error(`BENCH_GRID must be "quick" or "full", received "${value}"`)
}

function optional(value: string | undefined): string | undefined {
  return value === undefined || value === '' ? undefined : value
}

/** Read the `BENCH_*` settings. Unknown values fail fast instead of silently defaulting. */
export function readBenchEnvironment(env: NodeJS.ProcessEnv): BenchEnvironment {
  return {
    grid: grid(env.BENCH_GRID),
    repeats: count(env.BENCH_REPEATS ?? '3', 'BENCH_REPEATS'),
    warmup: count(env.BENCH_WARMUP ?? '1', 'BENCH_WARMUP'),
    jobs: env.BENCH_JOBS === undefined ? undefined : positive(env.BENCH_JOBS, 'BENCH_JOBS'),
    only: optional(env.BENCH_ONLY),
    json: optional(env.BENCH_JSON),
  }
}

/**
 * Derive a per-suite artifact path from the `BENCH_JSON` base path, so benchmark
 * files never overwrite each other's domain metrics:
 * `bench.json` becomes `bench.coordinator.json` and `bench.contention.json`.
 */
export function artifactPath(base: string, suite: string): string {
  const separator = base.lastIndexOf('.')
  if (separator <= 0) return `${base}.${suite}`

  return `${base.slice(0, separator)}.${suite}${base.slice(separator)}`
}
