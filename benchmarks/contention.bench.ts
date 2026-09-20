import { expect, test } from 'vitest'

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
import { executeDefinitions, writeArtifact } from './vitest-support.js'

const environment = readBenchEnvironment(process.env)
const grid = environment.grid === 'full' ? fullContentionGrid : quickContentionGrid
const jobs = environment.jobs ?? 2000

function selectedScenarios(): ScenarioDefinition[] {
  return contentionScenarios(grid)
    .filter((scenario) => matches(scenarioName(scenario), environment.only))
    .map((scenario) => defineContentionScenario(scenario, jobs))
}

// The provider owns every measured run and guards each one; this only bounds a
// broken suite, so it is deliberately generous.
test('contention scenarios', { timeout: 60 * 60_000 }, async ({ bench, skip }) => {
  const definitions = selectedScenarios()
  if (definitions.length === 0)
    skip(`no contention scenario matches BENCH_ONLY=${environment.only ?? ''}`)

  const results = await executeDefinitions(bench, definitions, `contention (${environment.grid})`)
  const path = writeArtifact(results, 'contention')
  if (path !== undefined) process.stdout.write(`wrote ${path}\n`)

  for (const result of results) {
    expect
      .soft(result.ok, `${result.scenario}: ${result.notes.join('; ') || 'invalid run'}`)
      .toBe(true)
  }
})
