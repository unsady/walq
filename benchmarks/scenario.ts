import type { BenchmarkResult, Collected } from './harness.js'

/**
 * Tag attached to a benchmark function so the custom Vitest provider can recover
 * the scenario it belongs to. `Symbol.for` is process-global, so the tag survives
 * even if the module is evaluated more than once.
 */
const descriptorKey = Symbol.for('walq.benchmark.scenario')

/**
 * One integration scenario as seen by the Vitest provider. `run` executes the
 * measured phase once; `summarize` reduces the controlled number of outcomes
 * into the report row that also carries the domain metrics and the validation
 * verdict.
 */
export type ScenarioDescriptor = {
  suite: string
  scenario: string
  jobs: number
  run: () => Promise<unknown>
  summarize: (collected: Collected<unknown>) => BenchmarkResult
  /** Primary rate (operations per second) shown in the Vitest `hz` column. */
  throughput: (result: BenchmarkResult) => number
  /** Per-run latency samples in milliseconds shown in the latency columns. */
  latency: (result: BenchmarkResult) => number[]
}

export type ScenarioFunction = (() => Promise<unknown>) & {
  [descriptorKey]?: ScenarioDescriptor
}

export type ScenarioDefinition = {
  name: string
  fn: ScenarioFunction
  descriptor: ScenarioDescriptor
}

export type ScenarioInput<Outcome> = {
  suite: string
  scenario: string
  jobs: number
  run: () => Promise<Outcome>
  summarize: (collected: Collected<Outcome>) => BenchmarkResult
  throughput: (result: BenchmarkResult) => number
  latency: (result: BenchmarkResult) => number[]
}

/**
 * Build the benchmark function and its descriptor. The function carries the
 * descriptor as a property, so the provider does not need a shared registry.
 */
export function defineScenario<Outcome>(input: ScenarioInput<Outcome>): ScenarioDefinition {
  const descriptor: ScenarioDescriptor = {
    suite: input.suite,
    scenario: input.scenario,
    jobs: input.jobs,
    run: input.run,
    summarize: (collected) => input.summarize(collected as Collected<Outcome>),
    throughput: input.throughput,
    latency: input.latency,
  }
  const fn = (() => descriptor.run()) as ScenarioFunction
  fn[descriptorKey] = descriptor

  return { name: input.scenario, fn, descriptor }
}

export function scenarioOf(fn: unknown): ScenarioDescriptor | undefined {
  if (typeof fn !== 'function') return undefined

  return (fn as ScenarioFunction)[descriptorKey]
}
