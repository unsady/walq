import { expect, test } from 'vitest'

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
import { executeDefinitions, writeArtifact } from './vitest-support.js'

const environment = readBenchEnvironment(process.env)
const grid = environment.grid === 'full' ? fullCompleteBatchGrid : quickCompleteBatchGrid
const jobs = environment.jobs ?? 4096

function selectedScenarios(): ScenarioDefinition[] {
  return completeBatchScenarios(grid)
    .filter((scenario) => matches(completeBatchScenarioName(scenario), environment.only))
    .map((scenario) => defineCompleteBatchScenario(scenario, jobs, environment.synchronous))
}

test('complete batch scenarios', { timeout: 60 * 60_000 }, async ({ bench, skip }) => {
  const definitions = selectedScenarios()
  if (definitions.length === 0)
    skip(`no complete batch scenario matches BENCH_ONLY=${environment.only ?? ''}`)

  const results = await executeDefinitions(
    bench,
    definitions,
    `complete batch (${environment.grid})`,
  )
  const path = writeArtifact(results, 'complete-batch')
  if (path !== undefined) process.stdout.write(`wrote ${path}\n`)

  for (const result of results) {
    expect
      .soft(result.ok, `${result.scenario}: ${result.notes.join('; ') || 'invalid run'}`)
      .toBe(true)
  }
})
