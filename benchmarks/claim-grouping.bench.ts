import { expect, test } from 'vitest'

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
import { executeDefinitions, writeArtifact } from './vitest-support.js'

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

test('claim grouping scenarios', { timeout: 60 * 60_000 }, async ({ bench, skip }) => {
  const definitions = selectedScenarios()
  if (definitions.length === 0)
    skip(`no claim grouping scenario matches BENCH_ONLY=${environment.only ?? ''}`)

  const results = await executeDefinitions(
    bench,
    definitions,
    `claim grouping (${environment.grid})`,
  )
  const path = writeArtifact(results, 'claim-grouping')
  if (path !== undefined) process.stdout.write(`wrote ${path}\n`)

  for (const result of results) {
    expect
      .soft(result.ok, `${result.scenario}: ${result.notes.join('; ') || 'invalid run'}`)
      .toBe(true)
  }
})
