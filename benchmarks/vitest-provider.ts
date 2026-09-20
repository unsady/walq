import type { BenchResult, BenchmarkProvider } from 'vitest'

import { readBenchEnvironment } from './bench-options.js'
import { collectRuns, percentile, type BenchmarkResult, type RunOptions } from './harness.js'
import { scenarioOf, type ScenarioDescriptor } from './scenario.js'

/**
 * Vitest shows `throughput.mean` in the `hz` column and the `latency.*` fields
 * in the remaining columns, so both are filled from the scenario's own samples.
 * Every other statistics field is computed as well to keep the result a valid
 * Tinybench-shaped object, even though the table only reads a subset.
 */
function statistics(values: number[]) {
  const sorted = [...values].sort((left, right) => left - right)
  const size = sorted.length
  const mean = size === 0 ? 0 : sorted.reduce((total, value) => total + value, 0) / size
  const variance =
    size === 0 ? 0 : sorted.reduce((total, value) => total + (value - mean) ** 2, 0) / size
  const sd = Math.sqrt(variance)
  const sem = size === 0 ? 0 : sd / Math.sqrt(size)
  const deviations = sorted.map((value) => Math.abs(value - mean))
  const mad = percentile(
    [...deviations].sort((left, right) => left - right),
    0.5,
  )

  return {
    aad: size === 0 ? 0 : deviations.reduce((total, value) => total + value, 0) / size,
    critical: 1.96,
    df: Math.max(0, size - 1),
    mad,
    max: size === 0 ? 0 : (sorted[size - 1] ?? 0),
    mean,
    min: size === 0 ? 0 : (sorted[0] ?? 0),
    moe: 1.96 * sem,
    p50: percentile(sorted, 0.5),
    p75: percentile(sorted, 0.75),
    p99: percentile(sorted, 0.99),
    p995: percentile(sorted, 0.995),
    p999: percentile(sorted, 0.999),
    rme: mean === 0 ? 0 : (sem / mean) * 100,
    samples: undefined,
    samplesCount: size,
    sd,
    sem,
    variance,
  }
}

export type DomainBenchResult = BenchResult & { domain: BenchmarkResult }

export function toBenchResult(
  descriptor: ScenarioDescriptor,
  domain: BenchmarkResult,
): DomainBenchResult {
  const latencies = descriptor.latency(domain)
  const latency = statistics(latencies)

  return {
    name: descriptor.scenario,
    state: 'completed',
    latency,
    throughput: statistics([descriptor.throughput(domain)]),
    period: latency.mean,
    totalTime: latencies.reduce((total, value) => total + value, 0),
    runtime: 'node',
    runtimeVersion: process.versions.node,
    timestampProviderName: 'performance.now',
    domain,
  }
}

/**
 * Custom Vitest benchmark provider. It runs every scenario of one `bench()`
 * group through the shared `collectRuns()` harness, so expensive workloads run
 * exactly `warmup + repeats` times, alternate their order, and keep the abort
 * and domain-validation semantics of the integration harness. The returned
 * result carries the full `BenchmarkResult` on `domain` for assertions and the
 * JSON artifact; Vitest only serializes the Tinybench-shaped statistics.
 */
const provider: BenchmarkProvider = {
  async run(group): Promise<BenchResult[]> {
    const descriptors = group.registrations.map((registration) => {
      const descriptor = scenarioOf(registration.fn)
      if (descriptor === undefined) {
        throw new Error(
          `benchmark "${registration.name}" was not registered through defineScenario()`,
        )
      }

      return descriptor
    })
    const environment = readBenchEnvironment(process.env)
    const options: RunOptions = {
      repeats: environment.repeats,
      warmup: environment.warmup,
      jobs: environment.jobs ?? 0,
      only: environment.only,
      report: (message) => console.error(message),
    }
    const { scenarios } = await collectRuns(descriptors, options, (descriptor) => descriptor.run())

    return descriptors.map((descriptor) => {
      const collected = scenarios.get(descriptor) ?? { outcomes: [], failures: [] }

      return toBenchResult(descriptor, descriptor.summarize(collected))
    })
  },
}

export default provider
