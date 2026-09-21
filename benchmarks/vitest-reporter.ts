import { VerboseReporter } from 'vitest/node'

/**
 * Benchmark-only reporter for the `bench:*` scripts.
 *
 * It keeps everything from Vitest's verbose reporter — per-file and per-test
 * status lines, failures, and the final run summary — and suppresses only the
 * built-in Tinybench table. Every suite renders its own compact domain summary
 * from the harness results, so the standard table would only repeat the same
 * scenarios with statistics the domain summary already covers.
 *
 * `pnpm test` keeps the default reporter; this class is referenced only by the
 * benchmark scripts in `package.json`.
 */
export default class WalqBenchmarkReporter extends VerboseReporter {
  protected override printBenchmarkTable(): void {}
}
