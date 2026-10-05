import { readBenchEnvironment, type BenchEnvironment } from './bench-options.js'
import { runSuite } from './runner.js'
import type { ScenarioDefinition } from './scenario.js'

interface SuiteModule {
  definitions: (environment: BenchEnvironment) => ScenarioDefinition[]
}

const suites: Record<string, () => Promise<SuiteModule>> = {
  adapters: () => import('./adapters.suite.js'),
  contention: () => import('./contention.suite.js'),
  journal: () => import('./journal.suite.js'),
  'claim-grouping': () => import('./claim-grouping.suite.js'),
}

async function main(): Promise<void> {
  const args = process.argv.slice(2)
  if (args.length === 1 && (args[0] === '--help' || args[0] === '-h')) {
    console.log(
      `Usage: pnpm bench [suite ...]\n\nSuites: ${Object.keys(suites).join(', ')}\n\nNo suites selects all, sequentially. Configure workloads with BENCH_* variables.\nSee benchmarks/README.md for settings and cross-runtime adapter comparisons.`,
    )

    return
  }

  const selected = args.length === 0 ? Object.keys(suites) : args
  for (const suite of selected) {
    if (!Object.hasOwn(suites, suite)) throw new Error(`Unknown benchmark suite "${suite}"`)
  }

  const environment = readBenchEnvironment(process.env)
  if (environment.repeats < 1) throw new Error('BENCH_REPEATS must be positive')

  for (const suite of selected) {
    const module = await suites[suite]!()
    if (!(await runSuite(suite, module.definitions(environment), environment))) {
      process.exitCode = 1
      // A failed suite may have left the process unsafe for further measurements.
      break
    }
  }
}

try {
  await main()
} catch (error) {
  console.error(error instanceof Error ? error.message : String(error))
  process.exitCode = 1
}
