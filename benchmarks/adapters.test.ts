import { describe, expect, it } from 'vitest'

import {
  adapterNames,
  adapterScenarios,
  defineAdapterScenario,
  runAdapterScenario,
  summarizeAdapterRuns,
  type AdapterOutcome,
  type AdapterScenario,
} from './adapters.js'
import { readBenchEnvironment } from './bench-options.js'
import { renderDomainSummary, renderJson } from './harness.js'

const base: AdapterScenario = {
  adapter: 'node:sqlite',
  database: 'memory',
  operation: 'enqueueMany',
  jobs: 17,
  batch: 8,
}

function outcome(overrides: Partial<AdapterOutcome> = {}): AdapterOutcome {
  return { jobs: 17, duration: 10, latency: [4, 6], sqlite: 'test-version', ...overrides }
}

describe('adapter benchmark selection', () => {
  it('compares both drivers only on Node.js and validates overrides', () => {
    expect(adapterNames(undefined)).toEqual(['better-sqlite3', 'node:sqlite'])
    expect(adapterNames(undefined, 'bun')).toEqual(['node:sqlite'])
    expect(adapterNames(undefined, 'deno')).toEqual(['node:sqlite'])
    expect(adapterNames('node:sqlite,node:sqlite')).toEqual(['node:sqlite'])
    expect(() => adapterNames('unknown')).toThrow('BENCH_ADAPTERS')
    expect(() => adapterNames('better-sqlite3', 'bun')).toThrow('only on Node.js')
  })

  it('uses identical workloads for both adapters and filters scenario names', () => {
    const scenarios = adapterScenarios(readBenchEnvironment({}), adapterNames(undefined))
    expect(scenarios).toHaveLength(20)
    const better = scenarios.filter((scenario) => scenario.adapter === 'better-sqlite3')
    const builtin = scenarios.filter((scenario) => scenario.adapter === 'node:sqlite')
    expect(better.map(({ adapter: _adapter, ...scenario }) => scenario)).toEqual(
      builtin.map(({ adapter: _adapter, ...scenario }) => scenario),
    )
    expect(scenarios.slice(0, 2).map((scenario) => scenario.adapter)).toEqual([
      'better-sqlite3',
      'node:sqlite',
    ])
    expect(
      adapterScenarios(
        readBenchEnvironment({ BENCH_ONLY: 'wal/cleanup', BENCH_JOBS: '17' }),
        adapterNames(undefined),
      ),
    ).toMatchObject([
      { adapter: 'better-sqlite3', jobs: 17 },
      { adapter: 'node:sqlite', jobs: 17 },
    ])
  })
})

const scenarios = adapterScenarios(
  readBenchEnvironment({ BENCH_JOBS: '17' }),
  adapterNames(undefined),
).map((scenario) => ({ ...scenario, batch: 8 }))

it.each(scenarios)(
  '$adapter/$database/$operation validates actual measured work',
  async (scenario) => {
    const run = await runAdapterScenario(scenario, 'full')
    const calls =
      scenario.operation === 'lifecycle' ? 23 : scenario.operation === 'claimQueues-grouped' ? 1 : 3

    expect(run.jobs).toBe(17)
    expect(run.latency).toHaveLength(calls)
    expect(run.duration).toBe(run.latency.reduce((total, value) => total + value, 0))
    expect(run.duration).toBeGreaterThan(0)
    expect(run.sqlite).toMatch(/^\d+\.\d+\.\d+/)
  },
)

it.each(adapterNames(undefined))(
  '%s grouped claims include multiple chunks and a partial final sweep',
  async (adapter) => {
    const run = await runAdapterScenario(
      { ...base, adapter, operation: 'claimQueues-grouped', jobs: 1025, batch: 64 },
      'normal',
    )

    expect(run.jobs).toBe(1025)
    expect(run.latency).toHaveLength(2)
  },
)

it('rejects invalid workload sizes before opening a connection', async () => {
  await expect(runAdapterScenario({ ...base, jobs: 0 }, 'normal')).rejects.toThrow(
    'positive safe-integer',
  )
  await expect(runAdapterScenario({ ...base, batch: 0 }, 'normal')).rejects.toThrow(
    'positive safe-integer',
  )
})

describe('adapter benchmark reporting', () => {
  it('counts jobs over actual elapsed totals, not calls or percentile-derived time', () => {
    const runs = [outcome(), outcome({ duration: 100, latency: [40, 60] })]
    const result = summarizeAdapterRuns(base, { outcomes: runs, failures: [] })

    expect(result.metrics['jobs/sec']).toBeCloseTo((34 / 110) * 1000)
    expect(result.metrics['call samples']).toBe(4)
    expect(result.metrics['measured runs']).toBe(2)
    expect(result.runs).toEqual(runs)
    expect(result.ok).toBe(true)
    expect(
      JSON.parse(
        renderJson(
          {
            node: 'compat',
            runtime: 'deno',
            runtimeVersion: '2.9.7',
            platform: 'linux',
            arch: 'x64',
            cpu: 'test',
            cores: 1,
          },
          {},
          [result],
        ),
      ).results[0].runs,
    ).toEqual(runs)
  })

  it('rejects failed, empty, and incomplete measured work', () => {
    expect(summarizeAdapterRuns(base, { outcomes: [], failures: [] }).ok).toBe(false)
    expect(summarizeAdapterRuns(base, { outcomes: [outcome()], failures: ['failed run'] }).ok).toBe(
      false,
    )
    for (const run of [
      outcome({ jobs: 16 }),
      outcome({ duration: 0 }),
      outcome({ latency: [] }),
      outcome({ latency: [NaN] }),
    ]) {
      expect(summarizeAdapterRuns(base, { outcomes: [run], failures: [] }).ok).toBe(false)
    }
  })

  it('uses the shared descriptor and renders the actual runtime and SQLite version', () => {
    const definition = defineAdapterScenario(base, 'normal')
    const result = definition.descriptor.summarize({ outcomes: [outcome()], failures: [] })
    const table = renderDomainSummary(
      'adapters',
      {
        node: 'compat',
        runtime: 'bun',
        runtimeVersion: '1.4.2',
        platform: 'linux',
        arch: 'x64',
        cpu: 'test',
        cores: 1,
      },
      {},
      [result],
    )

    expect(definition.descriptor.throughput(result)).toBe(1700)
    expect(table).toContain('bun 1.4.2')
    expect(table).toContain('test-version')
    expect(table).toContain('call samples')
  })
})
