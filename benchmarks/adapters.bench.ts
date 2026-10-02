import { adapterNames, adapterScenarios, defineAdapterScenario } from './adapters.js'
import { readBenchEnvironment } from './bench-options.js'
import { registerSuite } from './vitest-support.js'

registerSuite('adapters', 'SQLite adapters', () => {
  const environment = readBenchEnvironment(process.env)

  return adapterScenarios(environment, adapterNames(process.env.BENCH_ADAPTERS)).map((scenario) =>
    defineAdapterScenario(scenario, environment.synchronous),
  )
})
