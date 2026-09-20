import { describe, expect, it, vi } from 'vitest'

import {
  collectRuns,
  distribute,
  formatValue,
  guard,
  matches,
  median,
  percentile,
  renderJson,
  renderMarkdown,
  settledWithin,
  spread,
  SuiteAbortError,
  summarize,
  summarizeMicros,
  type BenchmarkResult,
  type RunOptions,
} from './harness.js'

function options(overrides: Partial<RunOptions> = {}): RunOptions {
  return {
    repeats: 1,
    warmup: 1,
    jobs: 10,
    only: undefined,
    report: () => {},
    ...overrides,
  }
}

function result(overrides: Partial<BenchmarkResult> = {}): BenchmarkResult {
  return {
    suite: 'suite',
    scenario: 'scenario',
    params: {},
    metrics: {},
    samples: [],
    notes: [],
    ok: true,
    ...overrides,
  }
}

describe('statistics', () => {
  it('uses the nearest rank for percentiles', () => {
    const sorted = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    expect(percentile(sorted, 0)).toBe(1)
    expect(percentile(sorted, 0.5)).toBe(5)
    expect(percentile(sorted, 0.95)).toBe(10)
    expect(percentile([], 0.5)).toBe(0)
  })

  it('summarizes millisecond samples in microseconds', () => {
    const summary = summarizeMicros([0.001, 0.002, 0.003])

    expect(summary.count).toBe(3)
    expect(summary.p50).toBeCloseTo(2, 6)
    expect(summary.max).toBeCloseTo(3, 6)
    expect(summarize([0.001, 0.002]).p95).toBeCloseTo(0.002, 6)
  })

  it('reports the spread between extremes relative to the median', () => {
    expect(median([3, 1, 2])).toBe(2)
    expect(spread([9, 10, 11])).toBe(20)
    expect(spread([5])).toBe(0)
    expect(spread([0, 0])).toBe(0)
  })
})

describe('distribute', () => {
  it('splits a total into even parts and keeps the remainder first', () => {
    expect(distribute(10, 3)).toEqual([4, 3, 3])
    expect(distribute(2, 5)).toEqual([1, 1, 0, 0, 0])
    expect(distribute(97, 7).reduce((sum, count) => sum + count, 0)).toBe(97)
  })
})

describe('formatValue', () => {
  it('keeps precision proportional to the magnitude', () => {
    expect(formatValue('shared')).toBe('shared')
    expect(formatValue(12)).toBe('12')
    expect(formatValue(1234.56)).toBe('1235')
    expect(formatValue(12.3456)).toBe('12.3')
    expect(formatValue(1.23456)).toBe('1.23')
    expect(formatValue(0.012345)).toBe('0.012')
    expect(formatValue(Number.NaN)).toBe('NaN')
  })
})

describe('matches', () => {
  it('treats a missing filter as a match', () => {
    expect(matches('shared / 8 threads', undefined)).toBe(true)
    expect(matches('shared / 8 threads', 'isolated')).toBe(false)
    expect(matches('shared / 8 threads', '8 threads')).toBe(true)
  })
})

describe('guard', () => {
  it('rejects once the duration elapses', async () => {
    vi.useFakeTimers()
    const timeout = guard(1_000, 'run timed out')
    const failure = timeout.promise.catch((error: unknown) => error)
    await vi.advanceTimersByTimeAsync(1_000)
    const error = await failure

    expect(error).toBeInstanceOf(Error)
    expect((error as Error).message).toBe('run timed out')
    timeout.dispose()
    vi.useRealTimers()
  })

  it('never rejects after it is disposed', async () => {
    vi.useFakeTimers()
    let rejected = false
    const timeout = guard(1_000, 'run timed out')
    void timeout.promise.catch(() => {
      rejected = true
    })
    timeout.dispose()
    await vi.advanceTimersByTimeAsync(5_000)

    expect(rejected).toBe(false)
    vi.useRealTimers()
  })
})

describe('settledWithin', () => {
  it('reports whether the work finished in time', async () => {
    expect(await settledWithin(Promise.resolve(), 1_000)).toBe(true)
    expect(await settledWithin(new Promise(() => {}), 10)).toBe(false)
  })

  it('clears its timer once the work is done', async () => {
    vi.useFakeTimers()
    await settledWithin(Promise.resolve(), 1_000)

    expect(vi.getTimerCount()).toBe(0)
    vi.useRealTimers()
  })
})

describe('collectRuns', () => {
  it('executes warmup passes and discards their results', async () => {
    const scenarios = ['a', 'b']
    const executed: string[] = []
    const collected = await collectRuns(
      scenarios,
      options({ repeats: 2, warmup: 1 }),
      async (name) => {
        executed.push(name)

        return name
      },
    )

    expect(executed).toHaveLength(6)
    expect(collected.scenarios.get('a')?.outcomes).toEqual(['a', 'a'])
    expect(collected.scenarios.get('b')?.outcomes).toEqual(['b', 'b'])
    expect(collected.abortReason).toBeUndefined()
  })

  it('alternates the direction of every pass', async () => {
    const passes: string[][] = []
    let current: string[] = []
    await collectRuns(['a', 'b', 'c'], options({ repeats: 2, warmup: 0 }), async (name) => {
      current.push(name)
      if (current.length === 3) {
        passes.push(current)
        current = []
      }

      return name
    })

    expect(passes).toEqual([
      ['a', 'b', 'c'],
      ['c', 'b', 'a'],
    ])
  })

  it('keeps failures of measured runs and only reports warmup failures', async () => {
    const reported: string[] = []
    let call = 0
    const collected = await collectRuns(
      ['a'],
      options({ repeats: 1, warmup: 1, report: (message) => reported.push(message) }),
      async () => {
        call += 1
        if (call === 1) throw new Error('warmup broke')
        if (call === 2) throw new Error('measured broke')

        return 'ok'
      },
    )

    expect(reported).toEqual(['warmup run failed: warmup broke'])
    expect(collected.scenarios.get('a')).toEqual({ outcomes: [], failures: ['measured broke'] })
  })

  it('stops the suite when a run leaves the process unusable', async () => {
    const reported: string[] = []
    const executed: string[] = []
    const collected = await collectRuns(
      ['a', 'b', 'c'],
      options({ repeats: 1, warmup: 0, report: (message) => reported.push(message) }),
      async (name) => {
        executed.push(name)
        if (name === 'b') throw new SuiteAbortError('workers did not stop')

        return name
      },
    )

    expect(executed).toEqual(['a', 'b'])
    expect(reported).toEqual(['suite aborted: workers did not stop'])
    expect(collected.abortReason).toBe('workers did not stop')
    expect(collected.scenarios.get('c')).toEqual({
      outcomes: [],
      failures: ['suite aborted: workers did not stop'],
    })
    expect(collected.scenarios.get('a')?.outcomes).toEqual(['a'])
  })
})

describe('renderMarkdown', () => {
  it('adds a notes column only when a scenario has notes', () => {
    const clean = renderMarkdown([result({ metrics: { 'jobs/sec': 10 } })])
    expect(clean).toContain('| scenario | jobs/sec |')
    expect(clean).not.toContain('notes')

    const broken = renderMarkdown([result({ notes: ['completed 1 of 2 | jobs'] })])
    expect(broken).toContain('| notes |')
    expect(broken).toContain('completed 1 of 2 \\| jobs')
  })

  it('groups scenarios by suite', () => {
    const markdown = renderMarkdown([
      result({ suite: 'one', scenario: 'a' }),
      result({ suite: 'two', scenario: 'b' }),
    ])

    expect(markdown).toContain('### one')
    expect(markdown).toContain('### two')
  })
})

describe('renderJson', () => {
  it('keeps raw samples and the validity flag', () => {
    const payload = JSON.parse(
      renderJson(
        { node: 'v0', platform: 'test', arch: 'test', cpu: 'test', cores: 1 },
        { repeats: 1 },
        [result({ samples: [{ 'jobs/sec': 42 }], ok: false, notes: ['nope'] })],
      ),
    ) as { results: BenchmarkResult[] }

    expect(payload.results[0]?.samples).toEqual([{ 'jobs/sec': 42 }])
    expect(payload.results[0]?.ok).toBe(false)
  })
})
