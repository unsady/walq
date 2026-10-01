import { readBenchEnvironment } from './bench-options.js'
import { defineGroupScenario, groupScenarios, positiveSetting } from './groups.js'
import { matches } from './harness.js'
import type { ScenarioDefinition } from './scenario.js'
import { registerSuite } from './vitest-support.js'

const environment = readBenchEnvironment(process.env)
const jobs = environment.jobs ?? (environment.grid === 'full' ? 20_000 : 512)
if (jobs < 257) throw new Error('BENCH_JOBS must exceed 256')
const defaultGroups = environment.grid === 'full' ? 2_000 : 64
const groups = positiveSetting(process.env.BENCH_GROUPS, defaultGroups, 'BENCH_GROUPS')
const futureGroups = positiveSetting(
  process.env.BENCH_FUTURE_GROUPS,
  defaultGroups,
  'BENCH_FUTURE_GROUPS',
)

function selectedScenarios(): ScenarioDefinition[] {
  return groupScenarios(jobs, groups, futureGroups)
    .filter((scenario) => matches(scenario.name, environment.only))
    .map((scenario) => defineGroupScenario(scenario, environment.synchronous))
}

registerSuite('groups', 'group scheduling', selectedScenarios)
