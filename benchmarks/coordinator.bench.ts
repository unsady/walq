import { readBenchEnvironment } from './bench-options.js'
import {
  coordinatorScenarios,
  defineCoordinatorScenario,
  fullCoordinatorGrid,
  quickCoordinatorGrid,
  scenarioName,
} from './coordinator.js'
import { matches } from './harness.js'
import type { ScenarioDefinition } from './scenario.js'
import { registerSuite } from './vitest-support.js'

const environment = readBenchEnvironment(process.env)
const grid = environment.grid === 'full' ? fullCoordinatorGrid : quickCoordinatorGrid
const jobs = environment.jobs ?? 1000

function selectedScenarios(): ScenarioDefinition[] {
  return coordinatorScenarios(grid)
    .filter((scenario) => matches(scenarioName(scenario), environment.only))
    .map((scenario) => defineCoordinatorScenario(scenario, jobs))
}

registerSuite('coordinator', 'coordinator', selectedScenarios)
