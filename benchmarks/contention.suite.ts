import type { BenchEnvironment } from './bench-options.js'
import {
  contentionScenarios,
  defineContentionScenario,
  fullContentionGrid,
  quickContentionGrid,
  scenarioName,
} from './contention.js'
import { matches } from './harness.js'
import type { ScenarioDefinition } from './scenario.js'

export function definitions(environment: BenchEnvironment): ScenarioDefinition[] {
  const grid = environment.grid === 'full' ? fullContentionGrid : quickContentionGrid
  const jobs = environment.jobs ?? 2000

  return contentionScenarios(grid)
    .filter((scenario) => matches(scenarioName(scenario), environment.only))
    .map((scenario) => defineContentionScenario(scenario, jobs, environment.synchronous))
}
