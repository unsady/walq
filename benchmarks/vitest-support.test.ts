import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import type { Bench, BenchRegistration, BenchResult } from 'vitest'
import { afterEach, describe, expect, it, vi } from 'vitest'

import type { BenchmarkResult } from './harness.js'
import { defineScenario, type ScenarioDefinition } from './scenario.js'
import { executeDefinitions, writeArtifact } from './vitest-support.js'

const emptyStats = {
  aad: 0,
  critical: 1.96,
  df: 0,
  mad: 0,
  max: 0,
  mean: 0,
  min: 0,
  moe: 0,
  p50: 0,
  p75: 0,
  p99: 0,
  p995: 0,
  p999: 0,
  rme: 0,
  samples: undefined,
  samplesCount: 0,
  sd: 0,
  sem: 0,
  variance: 0,
}

function domain(scenario: string): BenchmarkResult {
  return { suite: 'test', scenario, params: {}, metrics: {}, samples: [], notes: [], ok: true }
}

function benchResult(
  name: string,
  result: BenchmarkResult,
): BenchResult & { domain: BenchmarkResult } {
  return {
    name,
    state: 'completed',
    latency: emptyStats,
    throughput: emptyStats,
    period: 0,
    totalTime: 0,
    runtime: 'node',
    runtimeVersion: process.versions.node,
    timestampProviderName: 'performance.now',
    domain: result,
  }
}

function definition(name: string): ScenarioDefinition {
  return defineScenario({
    suite: 'test',
    scenario: name,
    jobs: 1,
    run: async () => 'outcome',
    summarize: () => domain(name),
    throughput: () => 0,
    latency: () => [],
  })
}

/** A fake `bench` fixture whose results always carry `results.get(name)`. */
function fakeBench(results: Map<string, BenchmarkResult>): Bench {
  const factory = ((name: string, fn: () => Promise<unknown>): BenchRegistration<string> => ({
    name,
    fn,
    run: async () => benchResult(name, results.get(name) ?? domain(name)),
  })) as unknown as Bench

  factory.compare = (async () => ({
    get: (name: string) => benchResult(name, results.get(name) ?? domain(name)),
  })) as unknown as Bench['compare']

  return factory
}

function benchWithoutDomain(name: string, fn: () => Promise<unknown>): BenchRegistration<string> {
  return {
    name,
    fn,
    run: async () => ({
      name,
      state: 'completed',
      latency: emptyStats,
      throughput: emptyStats,
      period: 0,
      totalTime: 0,
      runtime: 'node',
      runtimeVersion: process.versions.node,
      timestampProviderName: 'performance.now',
    }),
  }
}

afterEach(() => {
  vi.unstubAllEnvs()
})

describe('executeDefinitions', () => {
  it('runs a single scenario through registration.run', async () => {
    const results = await executeDefinitions(
      fakeBench(new Map()),
      [definition('alpha')],
      'test group',
    )

    expect(results).toHaveLength(1)
    expect(results[0]?.scenario).toBe('alpha')
  })

  it('runs a group through bench.compare', async () => {
    const definitions = [definition('alpha'), definition('beta')]
    const results = await executeDefinitions(fakeBench(new Map()), definitions, 'test group')

    expect(results.map((result) => result.scenario)).toEqual(['alpha', 'beta'])
  })

  it('fails when the provider omits the domain metrics', async () => {
    const factory = ((name: string, fn: () => Promise<unknown>) =>
      benchWithoutDomain(name, fn)) as unknown as Bench

    await expect(executeDefinitions(factory, [definition('alpha')], 'test group')).rejects.toThrow(
      'no domain metrics',
    )
  })
})

describe('writeArtifact', () => {
  it('writes one JSON file per suite under the BENCH_JSON base path', () => {
    const directory = mkdtempSync(join(tmpdir(), 'walq-bench-artifact-'))
    const base = join(directory, 'bench.json')
    vi.stubEnv('BENCH_JSON', base)
    try {
      const path = writeArtifact([domain('alpha')], 'coordinator')

      expect(path).toBe(join(directory, 'bench.coordinator.json'))
      const parsed = JSON.parse(readFileSync(path ?? '', 'utf8')) as { results: BenchmarkResult[] }
      expect(parsed.results.map((result) => result.scenario)).toEqual(['alpha'])
    } finally {
      rmSync(directory, { recursive: true, force: true })
    }
  })

  it('writes nothing when BENCH_JSON is unset', () => {
    expect(writeArtifact([domain('alpha')], 'coordinator')).toBeUndefined()
  })
})
