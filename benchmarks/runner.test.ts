import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { describe, expect, it, vi } from 'vitest'

import type { BenchEnvironment } from './bench-options.js'
import { SuiteAbortError, type BenchmarkResult } from './harness.js'
import { runSuite } from './runner.js'
import { defineScenario } from './scenario.js'

const environment: BenchEnvironment = {
  grid: 'quick',
  repeats: 2,
  warmup: 1,
  jobs: undefined,
  only: undefined,
  json: undefined,
  synchronous: 'normal',
}

function definition(name: string, run: () => Promise<number>, suite = 'claim-grouping') {
  return defineScenario({
    suite,
    scenario: name,
    jobs: 12,
    run,
    summarize: ({ outcomes, failures }) => ({
      suite,
      scenario: name,
      params: {},
      metrics: { 'jobs/sec': outcomes.length * 10 },
      samples: outcomes.map((duration) => ({ duration })),
      notes: failures,
      ok: failures.length === 0 && outcomes.length > 0,
    }),
  })
}

function output() {
  return { write: vi.fn<(message: string) => void>(), report: vi.fn<(message: string) => void>() }
}

describe('runSuite', () => {
  it('prints one summary and reports warmup and measured progress', async () => {
    const sink = output()
    const scenarios = ['alpha', 'beta'].map((name) => definition(name, async () => 1))

    expect(await runSuite('claim-grouping', scenarios, environment, sink)).toBe(true)
    expect(sink.write).toHaveBeenCalledTimes(1)
    expect(sink.write.mock.calls[0]?.[0]).toContain('domain summary — claim-grouping (quick)')
    expect(sink.write.mock.calls[0]?.[0]).toContain('jobs/sec')
    expect(sink.report).toHaveBeenCalledWith(expect.stringContaining('warmup 1/1'))
    expect(sink.report).toHaveBeenCalledWith(expect.stringContaining('repeat 2/2'))
  })

  it('labels ablation durability as per-scenario rather than the harness default', async () => {
    const sink = output()

    expect(
      await runSuite(
        'ablation',
        [definition('paired modes', async () => 1, 'ablation')],
        environment,
        sink,
      ),
    ).toBe(true)
    expect(sink.write.mock.calls[0]?.[0]).toContain('synchronous=per scenario')
  })

  it('rejects empty selections and zero measured runs before executing', async () => {
    const run = vi.fn<() => Promise<number>>(async () => 1)

    await expect(runSuite('claim-grouping', [], environment, output())).rejects.toThrow(
      'No claim-grouping scenario',
    )
    await expect(
      runSuite(
        'claim-grouping',
        [definition('alpha', run)],
        { ...environment, repeats: 0 },
        output(),
      ),
    ).rejects.toThrow('BENCH_REPEATS must be positive')
    expect(run).not.toHaveBeenCalled()
  })

  it('reports failures and returns an unsuccessful verdict', async () => {
    const sink = output()
    const broken = definition('broken', async () => {
      throw new Error('run timed out')
    })

    expect(await runSuite('claim-grouping', [broken], { ...environment, warmup: 0 }, sink)).toBe(
      false,
    )
    expect(sink.report).toHaveBeenCalledWith('broken: run timed out; run timed out')
    expect(sink.write).toHaveBeenCalledTimes(1)
  })

  it('stops after an unsafe failure even during warmup', async () => {
    const later = vi.fn<() => Promise<number>>(async () => 1)
    const abort = definition('abort', async () => {
      throw new SuiteAbortError('workers did not stop')
    })

    expect(
      await runSuite('claim-grouping', [abort, definition('later', later)], environment, output()),
    ).toBe(false)
    expect(later).not.toHaveBeenCalled()
  })

  it('writes raw measured samples, actual jobs and environment into nested suite artifacts', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'walq-bench-runner-'))
    const base = join(directory, 'nested', 'run.json')
    let attempts = 0
    try {
      await runSuite(
        'claim-grouping',
        [definition('alpha', async () => ++attempts)],
        { ...environment, json: base },
        output(),
      )
      const parsed = JSON.parse(
        readFileSync(join(directory, 'nested', 'run.claim-grouping.json'), 'utf8'),
      ) as {
        environment: { runtime: string }
        options: { jobs: number }
        results: BenchmarkResult[]
      }

      expect(parsed.environment.runtime).toBe('node')
      expect(parsed.options.jobs).toBe(12)
      expect(parsed.results[0]?.samples).toEqual([{ duration: 2 }, { duration: 3 }])
    } finally {
      rmSync(directory, { recursive: true, force: true })
    }
  })

  it('includes paired durability metrics in the same table and JSON artifact', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'walq-bench-durability-'))
    const sink = output()
    const definitions = (['NORMAL', 'FULL'] as const).map((synchronous) =>
      defineScenario({
        suite: 'journal',
        scenario: `WAL / ${synchronous} / batch 16`,
        jobs: 12,
        run: async () => (synchronous === 'NORMAL' ? 100 : 25),
        summarize: ({ outcomes }) => ({
          suite: 'journal',
          scenario: `WAL / ${synchronous} / batch 16`,
          params: { journal: 'WAL', synchronous, batch: 16, jobs: 12 },
          metrics: { 'jobs/sec': outcomes[0]! },
          samples: [],
          notes: [],
          ok: true,
        }),
      }),
    )

    try {
      expect(
        await runSuite(
          'journal',
          definitions,
          { ...environment, json: join(directory, 'run.json') },
          sink,
        ),
      ).toBe(true)
      const parsed = JSON.parse(readFileSync(join(directory, 'run.journal.json'), 'utf8')) as {
        options: { synchronous: string }
        results: BenchmarkResult[]
      }

      expect(sink.write.mock.calls[0]?.[0]).toContain('FULL drop (%)')
      expect(parsed.options.synchronous).toBe('per scenario')
      expect(parsed.results[1]?.metrics['FULL drop (%)']).toBe(75)
    } finally {
      rmSync(directory, { recursive: true, force: true })
    }
  })

  it('preserves failed adapter results with runtime-suffixed artifacts', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'walq-bench-adapters-'))
    try {
      const result = await runSuite(
        'adapters',
        [
          definition(
            'broken',
            async () => {
              throw new Error('incomplete work')
            },
            'adapters',
          ),
        ],
        { ...environment, warmup: 0, repeats: 1, json: join(directory, 'run.json') },
        output(),
      )
      const parsed = JSON.parse(
        readFileSync(join(directory, 'run.adapters.node.json'), 'utf8'),
      ) as {
        results: BenchmarkResult[]
      }

      expect(result).toBe(false)
      expect(parsed.results[0]?.ok).toBe(false)
      expect(parsed.results[0]?.notes).toEqual(['incomplete work'])
    } finally {
      rmSync(directory, { recursive: true, force: true })
    }
  })
})
