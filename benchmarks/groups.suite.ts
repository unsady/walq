import type { BenchEnvironment } from './bench-options.js'
import { defineGroupScenario, groupScenarios, positiveSetting } from './groups.js'
import { matches } from './harness.js'
import type { ScenarioDefinition } from './scenario.js'

export function definitions(environment: BenchEnvironment): ScenarioDefinition[] {
  const jobs = environment.jobs ?? (environment.grid === 'full' ? 20_000 : 512)
  if (jobs < 257) throw new Error('BENCH_JOBS must exceed 256')
  const defaultGroups = environment.grid === 'full' ? 2_000 : 64
  const groups = positiveSetting(process.env.BENCH_GROUPS, defaultGroups, 'BENCH_GROUPS')
  const futureGroups = positiveSetting(
    process.env.BENCH_FUTURE_GROUPS,
    defaultGroups,
    'BENCH_FUTURE_GROUPS',
  )

  return groupScenarios(jobs, groups, futureGroups)
    .filter((scenario) => matches(scenario.name, environment.only))
    .map((scenario) => defineGroupScenario(scenario, environment.synchronous))
}
