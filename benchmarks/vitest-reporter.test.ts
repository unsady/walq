import { describe, expect, it, vi } from 'vitest'
import { VerboseReporter } from 'vitest/node'

import WalqBenchmarkReporter from './vitest-reporter.js'

describe('WalqBenchmarkReporter', () => {
  it('extends the built-in verbose reporter so status, errors and summary stay', () => {
    expect(Object.getPrototypeOf(WalqBenchmarkReporter)).toBe(VerboseReporter)
  })

  it('replaces the inherited benchmark table renderer with a no-op', () => {
    const override = Object.getOwnPropertyDescriptor(
      WalqBenchmarkReporter.prototype,
      'printBenchmarkTable',
    )
    const inherited = (VerboseReporter.prototype as unknown as { printBenchmarkTable: unknown })
      .printBenchmarkTable
    expect(override?.value).toBeTypeOf('function')
    expect(override?.value).not.toBe(inherited)

    const log = vi.fn<() => void>()
    const reporter = Object.create(WalqBenchmarkReporter.prototype) as {
      printBenchmarkTable: (benchmarks: unknown, padding: string) => void
    }
    Object.defineProperty(reporter, 'log', { value: log })

    reporter.printBenchmarkTable([{ name: 'group', tasks: [{ name: 'alpha' }] }], '')

    expect(log).not.toHaveBeenCalled()
  })
})
