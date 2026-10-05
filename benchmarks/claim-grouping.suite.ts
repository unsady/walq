import type { BenchEnvironment } from './bench-options.js'
import {
  claimGroupingScenarioName,
  claimGroupingScenarios,
  claimLimitOverride,
  claimQueueOverride,
  defineClaimGroupingScenario,
  fullClaimGroupingGrid,
  quickClaimGroupingGrid,
  withClaimGroupingTiers,
} from './claim-grouping.js'
import { matches } from './harness.js'
import type { ScenarioDefinition } from './scenario.js'

export function definitions(environment: BenchEnvironment): ScenarioDefinition[] {
  const grid = withClaimGroupingTiers(
    environment.grid === 'full' ? fullClaimGroupingGrid : quickClaimGroupingGrid,
    {
      queues: claimQueueOverride(process.env.BENCH_CLAIM_QUEUES),
      limits: claimLimitOverride(process.env.BENCH_CLAIM_LIMITS),
    },
  )
  const jobs = environment.jobs ?? 4096

  return claimGroupingScenarios(grid)
    .filter((scenario) => matches(claimGroupingScenarioName(scenario), environment.only))
    .map((scenario) => defineClaimGroupingScenario(scenario, jobs, environment.synchronous))
}
