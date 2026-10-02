import { adapterNames, adapterScenarios, defineAdapterScenario } from './adapters.js'
import type { BenchEnvironment } from './bench-options.js'
import type { ScenarioDefinition } from './scenario.js'

export function definitions(environment: BenchEnvironment): ScenarioDefinition[] {
  const runtime = process.versions.bun ? 'bun' : process.versions.deno ? 'deno' : 'node'

  const entries = {
    sqlite: new URL('../../packages/sqlite/dist/index.js', import.meta.url).href,
    betterSqlite3: new URL('../../packages/better-sqlite3/dist/index.js', import.meta.url).href,
  }

  return adapterScenarios(environment, adapterNames(process.env.BENCH_ADAPTERS, runtime)).map(
    (scenario) => defineAdapterScenario(scenario, environment.synchronous, entries),
  )
}
