import { expect, test } from 'vitest'

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
import { executeDefinitions, writeArtifact } from './vitest-support.js'

const environment = readBenchEnvironment(process.env)
const grid = environment.grid === 'full' ? fullCoordinatorGrid : quickCoordinatorGrid
const jobs = environment.jobs ?? 1000

function selectedScenarios(): ScenarioDefinition[] {
  return coordinatorScenarios(grid)
    .filter((scenario) => matches(scenarioName(scenario), environment.only))
    .map((scenario) => defineCoordinatorScenario(scenario, jobs))
}

// The provider owns every measured run and guards each one; this only bounds a
// broken suite, so it is deliberately generous.
test('coordinator scenarios', { timeout: 60 * 60_000 }, async ({ bench, skip }) => {
  const definitions = selectedScenarios()
  if (definitions.length === 0)
    skip(`no coordinator scenario matches BENCH_ONLY=${environment.only ?? ''}`)

  const results = await executeDefinitions(bench, definitions, `coordinator (${environment.grid})`)
  const path = writeArtifact(results, 'coordinator')
  if (path !== undefined) process.stdout.write(`wrote ${path}\n`)

  for (const result of results) {
    expect
      .soft(result.ok, `${result.scenario}: ${result.notes.join('; ') || 'invalid run'}`)
      .toBe(true)
  }
})
