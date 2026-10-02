import { mkdirSync, writeFileSync } from 'node:fs'
import { dirname } from 'node:path'

import { adapterNames, adapterScenarios, defineAdapterScenario } from './adapters.js'
import { artifactPath, readBenchEnvironment } from './bench-options.js'
import {
  collectRuns,
  describeEnvironment,
  renderDomainSummary,
  renderJson,
  type MetricValue,
} from './harness.js'

const environment = readBenchEnvironment(process.env)
if (environment.repeats < 1) throw new Error('Adapter comparisons require BENCH_REPEATS >= 1')
const runtime = process.versions.bun ? 'bun' : process.versions.deno ? 'deno' : 'node'
const adapters = adapterNames(process.env.BENCH_ADAPTERS, runtime)
const entries = {
  sqlite: new URL('../../packages/sqlite/dist/index.js', import.meta.url).href,
  betterSqlite3: new URL('../../packages/better-sqlite3/dist/index.js', import.meta.url).href,
}
const definitions = adapterScenarios(environment, adapters).map((scenario) =>
  defineAdapterScenario(scenario, environment.synchronous, entries),
)
if (definitions.length === 0) throw new Error('No adapter scenario matches BENCH_ONLY')
const options = {
  repeats: environment.repeats,
  warmup: environment.warmup,
  jobs: environment.jobs ?? 1024,
  only: environment.only,
  report: (message: string) => console.error(message),
}
const attempts = new Map<string, number>()
const collected = await collectRuns(definitions, options, async (definition) => {
  const attempt = (attempts.get(definition.name) ?? 0) + 1
  attempts.set(definition.name, attempt)
  const phase =
    attempt <= environment.warmup
      ? `warmup ${attempt}/${environment.warmup}`
      : `repeat ${attempt - environment.warmup}/${environment.repeats}`
  console.error(`${phase}: ${definition.name}`)

  return await definition.descriptor.run()
})
const results = definitions.map((definition) =>
  definition.descriptor.summarize(
    collected.scenarios.get(definition) ?? { outcomes: [], failures: [] },
  ),
)
const settings: Record<string, MetricValue> = {
  repeats: environment.repeats,
  warmup: environment.warmup,
  jobs: definitions[0]?.descriptor.jobs ?? 0,
  batch: 64,
  synchronous: environment.synchronous,
}
console.log(
  renderDomainSummary('SQLite adapter comparison', describeEnvironment(), settings, results),
)
if (environment.json !== undefined) {
  // Runtime suffixes keep sequential Node/Bun/Deno runs from overwriting each other.
  const path = artifactPath(environment.json, `adapters.${runtime}`)
  mkdirSync(dirname(path), { recursive: true })
  writeFileSync(path, renderJson(describeEnvironment(), settings, results))
  console.log(`wrote ${path}`)
}
if (results.some((result) => !result.ok)) {
  for (const result of results)
    for (const note of result.notes) console.error(`${result.scenario}: ${note}`)
  process.exitCode = 1
}
