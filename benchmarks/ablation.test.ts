import { describe, expect, it, vi } from 'vitest'

import { definitions, execute, variants, variantName } from './ablation.suite.js'
import { readBenchEnvironment } from './bench-options.js'
import * as grouping from './claim-grouping.js'
import { renderJson, describeEnvironment } from './harness.js'

const environment = readBenchEnvironment({ BENCH_JOBS: '1024', BENCH_REPEATS: '7' })

describe('SQLite ablation', () => {
  it('pairs journal, durability and concurrency without changing job count', () => {
    expect(variants(false)).toHaveLength(20)
    expect(variants(true)).toHaveLength(4)
    expect(new Set(variants(false).map(variantName)).size).toBe(20)
    expect(definitions(environment).every((definition) => definition.jobs === 1024)).toBe(true)
  })

  it.each([1, 2, 3, 4, 5])(
    'stage %s processes identical jobs using production workers',
    async (stage) => {
      const variant = variants(false).find(
        (v) => v.stage === stage && v.synchronous === 'normal' && v.concurrency === 16,
      )!
      const result = await execute(variant, 1024)
      expect(result.jobs).toBe(1024)
      expect(result.transactions.reduce((total, tx) => total + tx.jobs, 0)).toBe(1024)
      expect(result.calls.reduce((total, call) => total + call.jobs, 0)).toBe(1024)
      const grouped = stage >= 4
      expect(result.calls.every((call) => call.grouped === grouped)).toBe(true)
      expect(Math.max(...result.calls.map((call) => call.jobs))).toBe(
        stage <= 2 ? 1 : stage === 3 ? 16 : 512,
      )
      expect(result.calls.some((call) => call.requests > 1)).toBe(grouped)
      expect(result.transactions.every((tx) => tx.jobs <= 512)).toBe(true)
      expect(result.eventLoop.length).toBeGreaterThan(1)
      expect(result.pragmas).toContainEqual({
        name: 'journal_mode',
        value: stage === 1 ? 'delete' : 'wal',
      })
    },
    30_000,
  )

  it.each([4, 5])(
    'stress stage %s uses production claim transactions and requests a production competing writer',
    async (stage) => {
      const variant = variants(true).find((v) => v.stage === stage && v.synchronous === 'normal')!
      const competitor = vi.spyOn(grouping, 'startCompetitor').mockReturnValue({
        start: async () => {},
        waitForSamples: async () => {},
        stop: async () => ({
          enqueueSamples: Array.from({ length: 20 }, () => 0.1),
          completeSamples: Array.from({ length: 20 }, () => 0.1),
          operations: 20,
          errors: 0,
          firstError: null,
        }),
        terminate: async () => 0,
      })
      try {
        const result = await execute(variant, 131072)
        expect(competitor).toHaveBeenCalledWith(expect.any(String), 'normal', 16384, true)
        expect(result.calls).toHaveLength(32)
        expect(result.calls.every((call) => call.requests === 256 && call.jobs === 4096)).toBe(true)
        expect(result.transactions).toHaveLength(stage === 4 ? 32 : 256)
        expect(result.transactions.every((tx) => tx.jobs === (stage === 4 ? 4096 : 512))).toBe(true)
        expect(result.transactions.reduce((total, tx) => total + tx.jobs, 0)).toBe(131072)
        expect(result.competitor.errors).toBe(0)
        expect(result.competitor.operations).toBeGreaterThanOrEqual(20)
      } finally {
        competitor.mockRestore()
      }
    },
    60_000,
  )

  it('keeps raw operation timings and separates explicit transactions from completion autocommits', async () => {
    const definition = definitions({
      ...environment,
      only: 'process / 5 / WAL / normal / concurrency 16',
    })[0]!
    const outcome = await definition.run()
    const result = definition.summarize({ outcomes: [outcome], failures: [] })
    const json = JSON.parse(renderJson(describeEnvironment(), {}, [result]))
    expect(json.results[0].raw[0].calls.length).toBeGreaterThan(0)
    expect(json.results[0].raw[0].transactions.length).toBe(result.metrics['explicit transactions'])
    expect(result.metrics['completion autocommits']).toBe(1024)
    expect(result.metrics['writer enqueue p95 (µs)']).toBe('N/A')
    expect(result.metrics['measured runs']).toBe(1)
  })
})
