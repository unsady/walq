import type { BenchmarkResult, Collected } from './harness.js'

export interface ScenarioDefinition<Outcome = unknown> {
  suite: string
  scenario: string
  jobs: number
  run: () => Promise<Outcome>
  summarize: (collected: Collected<Outcome>) => BenchmarkResult
}

/** Keep each workload's outcome type local while exposing a uniform runner contract. */
export function defineScenario<Outcome>(options: ScenarioDefinition<Outcome>): ScenarioDefinition {
  return {
    suite: options.suite,
    scenario: options.scenario,
    jobs: options.jobs,
    run: options.run,
    summarize: (collected) => options.summarize(collected as Collected<Outcome>),
  }
}
