import { mkdirSync, writeFileSync } from 'node:fs'
import { dirname } from 'node:path'

import { artifactPath, type BenchEnvironment } from './bench-options.js'
import {
  collectRuns,
  describeEnvironment,
  renderDomainSummary,
  renderJson,
  type BenchmarkResult,
  type MetricValue,
} from './harness.js'
import type { ScenarioDefinition } from './scenario.js'

export interface RunOutput {
  write: (message: string) => void
  report: (message: string) => void
}

/** Execute controlled integration workloads without a benchmark framework. */
export async function runSuite(
  suite: string,
  definitions: ScenarioDefinition[],
  environment: BenchEnvironment,
  output: RunOutput = {
    write: (message) => console.log(message),
    report: (message) => console.error(message),
  },
): Promise<boolean> {
  if (environment.repeats < 1) throw new Error('BENCH_REPEATS must be positive')
  if (definitions.length === 0) throw new Error(`No ${suite} scenario matches BENCH_ONLY`)

  const attempts = new Map<ScenarioDefinition, number>()
  const collected = await collectRuns(
    definitions,
    { ...environment, jobs: environment.jobs ?? 0, report: output.report },
    async (definition) => {
      const attempt = (attempts.get(definition) ?? 0) + 1
      attempts.set(definition, attempt)
      const phase =
        attempt <= environment.warmup
          ? `warmup ${attempt}/${environment.warmup}`
          : `repeat ${attempt - environment.warmup}/${environment.repeats}`
      const label = `${definition.scenario} — ${phase}`
      output.report(`start: ${label}`)
      const started = performance.now()

      try {
        const outcome = await definition.run()
        output.report(`done: ${label} (${((performance.now() - started) / 1000).toFixed(2)} s)`)

        return outcome
      } catch (error) {
        output.report(`failed: ${label}: ${error instanceof Error ? error.message : String(error)}`)

        throw error
      }
    },
  )
  const results: BenchmarkResult[] = definitions.map((definition) =>
    definition.summarize(collected.scenarios.get(definition) ?? { outcomes: [], failures: [] }),
  )
  if (suite === 'journal') {
    const { applyDurabilityComparison } = await import('./journal.suite.js')
    applyDurabilityComparison(results)
  }

  const settings: Record<string, MetricValue> = {
    suite,
    grid: environment.grid,
    repeats: environment.repeats,
    warmup: environment.warmup,
    jobs: environment.jobs ?? definitions[0]?.jobs ?? 0,
    only: environment.only ?? '',
    synchronous: suite === 'journal' ? 'per scenario' : environment.synchronous,
  }
  if (suite === 'adapters') settings.batch = 64

  const host = describeEnvironment()
  output.write(renderDomainSummary(`${suite} (${environment.grid})`, host, settings, results))
  if (environment.json !== undefined) {
    const suffix = suite === 'adapters' ? `${suite}.${host.runtime}` : suite
    const path = artifactPath(environment.json, suffix)
    mkdirSync(dirname(path), { recursive: true })
    writeFileSync(path, renderJson(host, settings, results))
    output.write(`wrote ${path}`)
  }

  for (const result of results) {
    if (!result.ok) {
      output.report(`${result.scenario}: ${result.notes.join('; ') || 'invalid run'}`)
    }
  }

  return collected.abortReason === undefined && results.every((result) => result.ok)
}
