import { readBenchEnvironment } from './bench-options.js'
import {
  completeBatchScenarioName,
  completeBatchScenarios,
  defineCompleteBatchScenario,
  fullCompleteBatchGrid,
  quickCompleteBatchGrid,
} from './complete-batch.js'
import { matches } from './harness.js'
import type { ScenarioDefinition } from './scenario.js'
import { registerSuite } from './vitest-support.js'

const environment = readBenchEnvironment(process.env)
const grid = environment.grid === 'full' ? fullCompleteBatchGrid : quickCompleteBatchGrid
const jobs = environment.jobs ?? 4096

function selectedScenarios(): ScenarioDefinition[] {
  return completeBatchScenarios(grid)
    .filter((scenario) => matches(completeBatchScenarioName(scenario), environment.only))
    .map((scenario) => defineCompleteBatchScenario(scenario, jobs, environment.synchronous))
}

registerSuite('complete-batch', 'complete batch', selectedScenarios)
