import { writeFileSync } from 'node:fs'

import type { Bench, BenchResult } from 'vitest'

import { artifactPath, readBenchEnvironment } from './bench-options.js'
import { describeEnvironment, renderJson, type BenchmarkResult } from './harness.js'
import type { ScenarioDefinition } from './scenario.js'
import type { DomainBenchResult } from './vitest-provider.js'

/**
 * Run one suite through Vitest's `bench()`/`bench.compare()` so the provider
 * executes the expensive scenarios once and Vitest records a single benchmark
 * table with one row per scenario. The full domain result travels back on
 * `domain`; Vitest's own serialization only keeps the Tinybench statistics.
 */
export async function executeDefinitions(
  bench: Bench,
  definitions: ScenarioDefinition[],
  groupName: string,
): Promise<BenchmarkResult[]> {
  if (definitions.length === 0) return []

  const registrations = definitions.map((definition) => bench(definition.name, definition.fn))
  const serialized = new Map<string, BenchResult>()
  if (registrations.length === 1) {
    const [registration] = registrations
    const [definition] = definitions
    if (registration === undefined || definition === undefined) return []
    serialized.set(definition.name, await registration.run({ name: groupName }))
  } else {
    const storage = await bench.compare(...registrations, { name: groupName })
    for (const definition of definitions)
      serialized.set(definition.name, storage.get(definition.name))
  }

  return definitions.map((definition) => {
    const result = serialized.get(definition.name) as DomainBenchResult | undefined
    if (result?.domain === undefined) {
      throw new Error(`benchmark provider returned no domain metrics for "${definition.name}"`)
    }

    return result.domain
  })
}

/** Persist the domain results of one suite when `BENCH_JSON` is set. */
export function writeArtifact(results: BenchmarkResult[], suite: string): string | undefined {
  const environment = readBenchEnvironment(process.env)
  if (environment.json === undefined) return undefined

  const path = artifactPath(environment.json, suite)
  writeFileSync(
    path,
    renderJson(
      describeEnvironment(),
      {
        suite,
        grid: environment.grid,
        repeats: environment.repeats,
        warmup: environment.warmup,
        jobs: environment.jobs ?? 0,
        only: environment.only ?? '',
      },
      results,
    ),
  )

  return path
}
