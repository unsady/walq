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

function definition(name: string, run: () => Promise<number>, suite = 'coordinator') {
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

    expect(await runSuite('coordinator', scenarios, environment, sink)).toBe(true)
    expect(sink.write).toHaveBeenCalledTimes(1)
    expect(sink.write.mock.calls[0]?.[0]).toContain('domain summary — coordinator (quick)')
    expect(sink.write.mock.calls[0]?.[0]).toContain('jobs/sec')
    expect(sink.report).toHaveBeenCalledWith(expect.stringContaining('warmup 1/1'))
    expect(sink.report).toHaveBeenCalledWith(expect.stringContaining('repeat 2/2'))
  })

  it('rejects empty selections and zero measured runs before executing', async () => {
    const run = vi.fn<() => Promise<number>>(async () => 1)

    await expect(runSuite('coordinator', [], environment, output())).rejects.toThrow(
      'No coordinator scenario',
    )
    await expect(
      runSuite('coordinator', [definition('alpha', run)], { ...environment, repeats: 0 }, output()),
    ).rejects.toThrow('BENCH_REPEATS must be positive')
    expect(run).not.toHaveBeenCalled()
  })

  it('reports failures and returns an unsuccessful verdict', async () => {
    const sink = output()
    const broken = definition('broken', async () => {
      throw new Error('run timed out')
    })

    expect(await runSuite('coordinator', [broken], { ...environment, warmup: 0 }, sink)).toBe(false)
    expect(sink.report).toHaveBeenCalledWith('broken: run timed out; run timed out')
    expect(sink.write).toHaveBeenCalledTimes(1)
  })

  it('stops after an unsafe failure even during warmup', async () => {
    const later = vi.fn<() => Promise<number>>(async () => 1)
    const abort = definition('abort', async () => {
      throw new SuiteAbortError('workers did not stop')
    })

    expect(
      await runSuite('coordinator', [abort, definition('later', later)], environment, output()),
    ).toBe(false)
    expect(later).not.toHaveBeenCalled()
  })

  it('writes raw measured samples, actual jobs and environment into nested suite artifacts', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'walq-bench-runner-'))
    const base = join(directory, 'nested', 'run.json')
    let attempts = 0
    try {
      await runSuite(
        'coordinator',
        [definition('alpha', async () => ++attempts)],
        { ...environment, json: base },
        output(),
      )
      const parsed = JSON.parse(
        readFileSync(join(directory, 'nested', 'run.coordinator.json'), 'utf8'),
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
