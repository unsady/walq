import type { BenchEnvironment } from './bench-options.js'
import { matches } from './harness.js'
import {
  defineRetentionScenario,
  fullRetentionGrid,
  quickRetentionGrid,
  retentionBatchOverride,
  retentionScenarioName,
  retentionScenarios,
  withRetentionBatch,
} from './retention.js'
import type { ScenarioDefinition } from './scenario.js'

export function definitions(environment: BenchEnvironment): ScenarioDefinition[] {
  const grid = withRetentionBatch(
    environment.grid === 'full' ? fullRetentionGrid : quickRetentionGrid,
    retentionBatchOverride(process.env.BENCH_RETENTION_BATCH),
  )
  const jobs = environment.jobs ?? 1000

  return retentionScenarios(grid)
    .filter((scenario) => matches(retentionScenarioName(scenario), environment.only))
    .map((scenario) => defineRetentionScenario(scenario, jobs, environment.synchronous))
}
