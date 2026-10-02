import type { BenchEnvironment } from './bench-options.js'
import {
  coordinatorScenarios,
  defineCoordinatorScenario,
  fullCoordinatorGrid,
  quickCoordinatorGrid,
  scenarioName,
} from './coordinator.js'
import { matches } from './harness.js'
import type { ScenarioDefinition } from './scenario.js'

export function definitions(environment: BenchEnvironment): ScenarioDefinition[] {
  const grid = environment.grid === 'full' ? fullCoordinatorGrid : quickCoordinatorGrid
  const jobs = environment.jobs ?? 1000

  return coordinatorScenarios(grid)
    .filter((scenario) => matches(scenarioName(scenario), environment.only))
    .map((scenario) => defineCoordinatorScenario(scenario, jobs))
}
