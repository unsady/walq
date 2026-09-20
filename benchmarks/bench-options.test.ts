import { describe, expect, it } from 'vitest'

import { artifactPath, readBenchEnvironment } from './bench-options.js'

const empty: NodeJS.ProcessEnv = {}

describe('readBenchEnvironment', () => {
  it('defaults to the quick grid, three repeats, one warmup and no overrides', () => {
    expect(readBenchEnvironment(empty)).toEqual({
      grid: 'quick',
      repeats: 3,
      warmup: 1,
      jobs: undefined,
      only: undefined,
      json: undefined,
    })
  })

  it('reads every setting from the environment', () => {
    expect(
      readBenchEnvironment({
        BENCH_GRID: 'full',
        BENCH_REPEATS: '5',
        BENCH_WARMUP: '0',
        BENCH_JOBS: '50',
        BENCH_ONLY: 'shared',
        BENCH_JSON: 'reports/bench.json',
      }),
    ).toEqual({
      grid: 'full',
      repeats: 5,
      warmup: 0,
      jobs: 50,
      only: 'shared',
      json: 'reports/bench.json',
    })
  })

  it('treats empty optional values as unset', () => {
    const environment = readBenchEnvironment({ BENCH_ONLY: '', BENCH_JSON: '' })

    expect(environment.only).toBeUndefined()
    expect(environment.json).toBeUndefined()
  })

  it('rejects invalid values instead of defaulting silently', () => {
    expect(() => readBenchEnvironment({ BENCH_GRID: 'wide' })).toThrow('BENCH_GRID')
    expect(() => readBenchEnvironment({ BENCH_REPEATS: '-1' })).toThrow('BENCH_REPEATS')
    expect(() => readBenchEnvironment({ BENCH_REPEATS: 'many' })).toThrow('BENCH_REPEATS')
    expect(() => readBenchEnvironment({ BENCH_WARMUP: '1.5' })).toThrow('BENCH_WARMUP')
    expect(() => readBenchEnvironment({ BENCH_JOBS: '0' })).toThrow('BENCH_JOBS')
    expect(() => readBenchEnvironment({ BENCH_JOBS: '-10' })).toThrow('BENCH_JOBS')
  })
})

describe('artifactPath', () => {
  it('inserts the suite before the extension', () => {
    expect(artifactPath('reports/bench.json', 'coordinator')).toBe('reports/bench.coordinator.json')
    expect(artifactPath('bench.json', 'contention')).toBe('bench.contention.json')
  })

  it('appends the suite when there is no extension', () => {
    expect(artifactPath('reports/bench', 'coordinator')).toBe('reports/bench.coordinator')
    expect(artifactPath('.hidden', 'coordinator')).toBe('.hidden.coordinator')
  })
})
