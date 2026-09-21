import type { BenchmarkGroup } from 'vitest'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { SuiteAbortError } from './harness.js'
import { defineScenario, type ScenarioDefinition } from './scenario.js'
import provider, { type DomainBenchResult } from './vitest-provider.js'

function definition(name: string, run: () => Promise<unknown>): ScenarioDefinition {
  return defineScenario({
    suite: 'test',
    scenario: name,
    jobs: 1,
    run,
    summarize: (collected) => ({
      suite: 'test',
      scenario: name,
      params: {},
      metrics: { 'jobs/sec': collected.outcomes.length * 10 },
      samples: collected.outcomes.map(() => ({ 'elapsed (ms)': 5 })),
      notes: collected.failures,
      ok: collected.failures.length === 0 && collected.outcomes.length > 0,
    }),
    throughput: (result) =>
      typeof result.metrics['jobs/sec'] === 'number' ? result.metrics['jobs/sec'] : 0,
    latency: (result) => result.samples.map((sample) => sample['elapsed (ms)'] ?? 0),
  })
}

function group(definitions: ScenarioDefinition[]): BenchmarkGroup {
  return {
    test: {} as BenchmarkGroup['test'],
    config: {} as BenchmarkGroup['config'],
    registrations: definitions.map((entry) => ({ name: entry.name, fn: entry.fn })),
  }
}

function domains(results: Awaited<ReturnType<typeof provider.run>>): DomainBenchResult[] {
  return results as DomainBenchResult[]
}

afterEach(() => {
  vi.unstubAllEnvs()
})

describe('vitest provider', () => {
  it('runs exactly warmup + repeats times and fills the Tinybench columns', async () => {
    vi.stubEnv('BENCH_WARMUP', '1')
    vi.stubEnv('BENCH_REPEATS', '2')
    const run = vi.fn<() => Promise<string>>(async () => 'outcome')
    const results = domains(await provider.run(group([definition('alpha', run)])))

    expect(run).toHaveBeenCalledTimes(3)
    expect(results).toHaveLength(1)
    expect(results[0]?.name).toBe('alpha')
    expect(results[0]?.throughput.mean).toBe(20)
    expect(results[0]?.latency.mean).toBe(5)
    expect(results[0]?.latency.samplesCount).toBe(2)
    expect(results[0]?.domain.ok).toBe(true)
  })

  it('keeps a failing run visible instead of turning it into a success', async () => {
    vi.stubEnv('BENCH_WARMUP', '0')
    vi.stubEnv('BENCH_REPEATS', '1')
    const failure = new Error('run timed out')
    const results = domains(
      await provider.run(
        group([
          definition('broken', async () => {
            throw failure
          }),
        ]),
      ),
    )

    expect(results[0]?.domain.ok).toBe(false)
    expect(results[0]?.domain.notes).toEqual(['run timed out'])
  })

  it('stops the group when a scenario leaves the process unsafe to measure in', async () => {
    vi.stubEnv('BENCH_WARMUP', '0')
    vi.stubEnv('BENCH_REPEATS', '1')
    const abort = vi.fn<() => Promise<never>>(async () => {
      throw new SuiteAbortError('workers did not stop')
    })
    const later = vi.fn<() => Promise<string>>(async () => 'outcome')
    const results = domains(
      await provider.run(group([definition('aborting', abort), definition('later', later)])),
    )

    expect(abort).toHaveBeenCalledTimes(1)
    expect(later).not.toHaveBeenCalled()
    expect(results.map((result) => result.domain.ok)).toEqual([false, false])
  })

  it('rejects registrations that were not created by defineScenario', async () => {
    async function untagged(): Promise<unknown> {
      return 'outcome'
    }
    const input: BenchmarkGroup = {
      test: {} as BenchmarkGroup['test'],
      config: {} as BenchmarkGroup['config'],
      registrations: [{ name: 'raw', fn: untagged }],
    }

    await expect(provider.run(input)).rejects.toThrow('defineScenario')
  })

  it('summarizes the primary rate and the latency spread of a scenario', async () => {
    vi.stubEnv('BENCH_WARMUP', '0')
    vi.stubEnv('BENCH_REPEATS', '1')
    const results = domains(
      await provider.run(
        group([
          defineScenario({
            suite: 'test',
            scenario: 'spread',
            jobs: 1,
            run: async () => 'outcome',
            summarize: () => ({
              suite: 'test',
              scenario: 'spread',
              params: {},
              metrics: {},
              samples: [],
              notes: [],
              ok: true,
            }),
            throughput: () => 100,
            latency: () => [1, 2, 3],
          }),
        ]),
      ),
    )

    expect(results[0]?.throughput.mean).toBe(100)
    expect(results[0]?.latency.max).toBe(3)
    expect(results[0]?.latency.p50).toBe(2)
    expect(results[0]?.totalTime).toBe(6)
  })
})
