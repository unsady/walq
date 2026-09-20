import { expect, test } from 'vitest'

import { readBenchEnvironment } from './bench-options.js'
import { matches } from './harness.js'
import {
  defineRetentionScenario,
  fullRetentionGrid,
  quickRetentionGrid,
  retentionBatchOverride,
  retentionScenarioName,
  retentionScenarios,
  withRetentionBatch,
} from './retention.js'
import type { ScenarioDefinition } from './scenario.js'
import { executeDefinitions, writeArtifact } from './vitest-support.js'

const environment = readBenchEnvironment(process.env)
const grid = withRetentionBatch(
  environment.grid === 'full' ? fullRetentionGrid : quickRetentionGrid,
  retentionBatchOverride(process.env.BENCH_RETENTION_BATCH),
)
const jobs = environment.jobs ?? 1000

function selectedScenarios(): ScenarioDefinition[] {
  return retentionScenarios(grid)
    .filter((scenario) => matches(retentionScenarioName(scenario), environment.only))
    .map((scenario) => defineRetentionScenario(scenario, jobs))
}

test('retention scenarios', { timeout: 60 * 60_000 }, async ({ bench, skip }) => {
  const definitions = selectedScenarios()
  if (definitions.length === 0)
    skip(`no retention scenario matches BENCH_ONLY=${environment.only ?? ''}`)

  const results = await executeDefinitions(bench, definitions, `retention (${environment.grid})`)
  const path = writeArtifact(results, 'retention')
  if (path !== undefined) process.stdout.write(`wrote ${path}\n`)

  for (const result of results) {
    expect
      .soft(result.ok, `${result.scenario}: ${result.notes.join('; ') || 'invalid run'}`)
      .toBe(true)
  }
})
