import { readBenchEnvironment } from './bench-options.js'
import {
  contentionScenarios,
  defineContentionScenario,
  fullContentionGrid,
  quickContentionGrid,
  scenarioName,
} from './contention.js'
import { matches } from './harness.js'
import type { ScenarioDefinition } from './scenario.js'
import { registerSuite } from './vitest-support.js'

const environment = readBenchEnvironment(process.env)
const grid = environment.grid === 'full' ? fullContentionGrid : quickContentionGrid
const jobs = environment.jobs ?? 2000

function selectedScenarios(): ScenarioDefinition[] {
  return contentionScenarios(grid)
    .filter((scenario) => matches(scenarioName(scenario), environment.only))
    .map((scenario) => defineContentionScenario(scenario, jobs, environment.synchronous))
}

registerSuite('contention', 'contention', selectedScenarios)
