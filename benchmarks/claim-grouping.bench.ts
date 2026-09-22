import { readBenchEnvironment } from './bench-options.js'
import {
  claimChunkOverride,
  claimGroupingScenarioName,
  claimGroupingScenarios,
  claimLimitOverride,
  claimModeOverride,
  claimQueueOverride,
  defineClaimGroupingScenario,
  fullClaimGroupingGrid,
  quickClaimGroupingGrid,
  withClaimGroupingTiers,
} from './claim-grouping.js'
import { matches } from './harness.js'
import type { ScenarioDefinition } from './scenario.js'
import { registerSuite } from './vitest-support.js'

const environment = readBenchEnvironment(process.env)
const grid = withClaimGroupingTiers(
  environment.grid === 'full' ? fullClaimGroupingGrid : quickClaimGroupingGrid,
  {
    queues: claimQueueOverride(process.env.BENCH_CLAIM_QUEUES),
    limits: claimLimitOverride(process.env.BENCH_CLAIM_LIMITS),
    modes: claimModeOverride(process.env.BENCH_CLAIM_MODES),
    chunks: claimChunkOverride(process.env.BENCH_CLAIM_CHUNKS),
  },
)
const jobs = environment.jobs ?? 4096

function selectedScenarios(): ScenarioDefinition[] {
  return claimGroupingScenarios(grid)
    .filter((scenario) => matches(claimGroupingScenarioName(scenario), environment.only))
    .map((scenario) => defineClaimGroupingScenario(scenario, jobs, environment.synchronous))
}

registerSuite('claim-grouping', 'claim grouping', selectedScenarios)
