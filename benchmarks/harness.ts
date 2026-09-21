import { availableParallelism, cpus } from 'node:os'

export type MetricValue = number | string

/** Metric values of a single measured run, kept for later analysis. */
export type RunSample = Record<string, number>

/** One scenario, aggregated across measured repeats. */
export type BenchmarkResult = {
  suite: string
  scenario: string
  params: Record<string, MetricValue>
  metrics: Record<string, MetricValue>
  /** Every measured run in execution order. */
  samples: RunSample[]
  notes: string[]
  ok: boolean
}

export type SuiteResult = {
  results: BenchmarkResult[]
  abortReason: string | undefined
}

export type LatencySummary = {
  count: number
  mean: number
  p50: number
  p95: number
  p99: number
  max: number
}

export type Environment = {
  node: string
  platform: string
  arch: string
  cpu: string
  cores: number
}

export type RunOptions = {
  repeats: number
  warmup: number
  jobs: number
  only: string | undefined
  report: (message: string) => void
}

export type Deferred<Value> = {
  promise: Promise<Value>
  resolve: (value: Value) => void
  reject: (error: unknown) => void
}

/** Rejects with `message` once `duration` elapses, so callers can bound their awaits. */
export type Guard = {
  promise: Promise<never>
  dispose: () => void
}

/** Measured runs and failed attempts of one scenario. */
export type Collected<Outcome> = {
  outcomes: Outcome[]
  failures: string[]
}

export function deferred<Value>(): Deferred<Value> {
  let resolve: (value: Value) => void = () => {}
  let reject: (error: unknown) => void = () => {}
  const promise = new Promise<Value>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise
    reject = rejectPromise
  })
  // A failure may reject the deferred before anyone awaits it.
  void promise.catch(() => {})

  return { promise, resolve, reject }
}

export function guard(duration: number, message: string): Guard {
  const failure = deferred<never>()
  const timer = setTimeout(() => failure.reject(new Error(message)), duration)
  timer.unref()

  return { promise: failure.promise, dispose: () => clearTimeout(timer) }
}

/** Resolves true when `work` settles first and false once `duration` elapses. */
export async function settledWithin(work: Promise<unknown>, duration: number): Promise<boolean> {
  const expired = deferred<boolean>()
  const timer = setTimeout(() => expired.resolve(false), duration)
  try {
    return await Promise.race([work.then(() => true), expired.promise])
  } finally {
    clearTimeout(timer)
  }
}

/** A failure that leaves the process unsafe to measure in, so the suite must stop. */
export class SuiteAbortError extends Error {}

/** Split a total into `parts` counts, as evenly as possible. */
export function distribute(total: number, parts: number): number[] {
  const base = Math.floor(total / parts)
  const rest = total % parts

  return Array.from({ length: parts }, (_, index) => base + (index < rest ? 1 : 0))
}

export function errorMessage(error: unknown): string {
  if (error instanceof Error) return error.message
  try {
    return String(error)
  } catch {
    return 'unknown error'
  }
}

export function matches(name: string, only: string | undefined): boolean {
  return only === undefined || name.includes(only)
}

/**
 * Runs every scenario `warmup + repeats` times, alternating the direction of each pass so that
 * neighbouring variants do not drift with machine state. Warmup results are discarded; failures
 * are kept only for measured runs. A `SuiteAbortError` stops the suite, because continuing would
 * measure in a process that still has leftovers of the failed run.
 */
export async function collectRuns<Scenario, Outcome>(
  scenarios: Scenario[],
  options: RunOptions,
  execute: (scenario: Scenario) => Promise<Outcome>,
): Promise<{
  scenarios: Map<Scenario, Collected<Outcome>>
  abortReason: string | undefined
}> {
  const collected = new Map<Scenario, Collected<Outcome>>(
    scenarios.map((scenario): [Scenario, Collected<Outcome>] => [
      scenario,
      { outcomes: [], failures: [] },
    ]),
  )
  let aborted: string | undefined

  for (let pass = 0; pass < options.warmup + options.repeats && aborted === undefined; pass += 1) {
    const order = pass % 2 === 0 ? scenarios : [...scenarios].reverse()
    const measured = pass >= options.warmup
    for (const scenario of order) {
      if (aborted !== undefined) break

      const bucket = collected.get(scenario)
      if (bucket === undefined) continue

      try {
        const outcome = await execute(scenario)
        if (measured) bucket.outcomes.push(outcome)
      } catch (error) {
        const message = errorMessage(error)
        if (error instanceof SuiteAbortError) {
          aborted = message
          if (measured) bucket.failures.push(`suite aborted: ${message}`)
          options.report(`suite aborted: ${message}`)
        } else if (measured) bucket.failures.push(message)
        else options.report(`warmup run failed: ${message}`)
      }
    }
  }

  if (aborted !== undefined) {
    for (const bucket of collected.values()) {
      if (bucket.outcomes.length === 0 && bucket.failures.length === 0) {
        bucket.failures.push(`suite aborted: ${aborted}`)
      }
    }
  }

  return { scenarios: collected, abortReason: aborted }
}

export function describeEnvironment(): Environment {
  let cpu = 'unknown'
  try {
    cpu = cpus()[0]?.model ?? 'unknown'
  } catch {
    cpu = 'unknown'
  }

  return {
    node: process.version,
    platform: process.platform,
    arch: process.arch,
    cpu,
    cores: availableParallelism(),
  }
}

/** Percentile of an ascending array, using the nearest-rank method. */
export function percentile(sorted: number[], fraction: number): number {
  if (sorted.length === 0) return 0

  const rank = Math.ceil(fraction * sorted.length) - 1
  const index = Math.min(sorted.length - 1, Math.max(0, rank))

  return sorted[index] ?? 0
}

export function median(values: number[]): number {
  return percentile(
    [...values].sort((left, right) => left - right),
    0.5,
  )
}

export function mean(values: number[]): number {
  if (values.length === 0) return 0

  return values.reduce((total, value) => total + value, 0) / values.length
}

/** Relative distance between the extremes, as a percentage of the median. */
export function spread(values: number[]): number {
  if (values.length < 2) return 0

  const middle = median(values)
  if (middle === 0) return 0

  return ((Math.max(...values) - Math.min(...values)) / middle) * 100
}

export function summarize(samples: number[]): LatencySummary {
  const sorted = [...samples].sort((left, right) => left - right)
  let max = 0
  for (const sample of sorted) max = Math.max(max, sample)

  return {
    count: sorted.length,
    mean: mean(sorted),
    p50: percentile(sorted, 0.5),
    p95: percentile(sorted, 0.95),
    p99: percentile(sorted, 0.99),
    max,
  }
}

/** Percentiles of millisecond samples, reported in microseconds. */
export function summarizeMicros(samples: number[]): LatencySummary {
  const summary = summarize(samples)

  return {
    count: summary.count,
    mean: summary.mean * 1000,
    p50: summary.p50 * 1000,
    p95: summary.p95 * 1000,
    p99: summary.p99 * 1000,
    max: summary.max * 1000,
  }
}

/**
 * Fair latency summary across measured repeats: every run gets one vote. Each run's mean and
 * percentiles are computed first, then the median of every statistic is taken across runs, so a
 * run that happens to collect more samples cannot outweigh the others. Runs without samples are
 * ignored, and `count` stays the total number of samples across all runs. Input is milliseconds,
 * output is microseconds, matching {@link summarizeMicros}.
 */
export function summarizePerRunMicros(runs: number[][]): LatencySummary {
  const perRun = runs.filter((run) => run.length > 0).map((run) => summarize(run))
  if (perRun.length === 0) {
    return { count: 0, mean: 0, p50: 0, p95: 0, p99: 0, max: 0 }
  }

  return {
    count: runs.reduce((total, run) => total + run.length, 0),
    mean: median(perRun.map((summary) => summary.mean)) * 1000,
    p50: median(perRun.map((summary) => summary.p50)) * 1000,
    p95: median(perRun.map((summary) => summary.p95)) * 1000,
    p99: median(perRun.map((summary) => summary.p99)) * 1000,
    max: median(perRun.map((summary) => summary.max)) * 1000,
  }
}

/** Read a metric as a number, treating strings and missing values as zero. */
export function numeric(value: MetricValue | undefined): number {
  return typeof value === 'number' ? value : 0
}

export function formatValue(value: MetricValue): string {
  if (typeof value === 'string') return value
  if (!Number.isFinite(value)) return String(value)

  const magnitude = Math.abs(value)
  if (Number.isInteger(value) && magnitude < 1_000_000) return value.toFixed(0)
  if (magnitude >= 100) return value.toFixed(0)
  if (magnitude >= 10) return value.toFixed(1)
  if (magnitude >= 1) return value.toFixed(2)

  return value.toFixed(3)
}

function collectColumns(results: BenchmarkResult[], key: 'params' | 'metrics'): string[] {
  const columns: string[] = []
  for (const result of results) {
    for (const column of Object.keys(result[key])) {
      if (!columns.includes(column)) columns.push(column)
    }
  }

  return columns
}

function isNumeric(results: BenchmarkResult[], key: 'params' | 'metrics', column: string): boolean {
  return results.every((result) => typeof result[key][column] === 'number')
}

/** A column is numeric when it has at least one value and every present value is a number. */
function isNumericColumn(
  results: BenchmarkResult[],
  key: 'params' | 'metrics',
  column: string,
): boolean {
  let seen = false
  for (const result of results) {
    const value = result[key][column]
    if (value === undefined || value === '') continue
    if (typeof value !== 'number') return false
    seen = true
  }

  return seen
}

/** Visible width in code points, so surrogate pairs count as one cell. */
function displayWidth(value: string): number {
  return [...value].length
}

/** Collapse line breaks and tabs so a value cannot break the surrounding table layout. */
function flattenCell(value: string): string {
  return value.replaceAll(/\r\n|[\r\n\t]/g, ' ')
}

/**
 * Render a plain-text table with computed column widths: two-space gutters, text
 * left-aligned and numbers right-aligned, and no Markdown markers.
 */
function renderTerminalTable(
  headers: string[],
  alignments: Array<'left' | 'right'>,
  rows: string[][],
): string {
  const widths = headers.map((header, column) => {
    let width = displayWidth(header)
    for (const row of rows) width = Math.max(width, displayWidth(row[column] ?? ''))

    return width
  })
  const formatRow = (cells: string[]): string =>
    cells
      .map((cell, column) => {
        const width = widths[column] ?? 0
        const padding = ' '.repeat(Math.max(0, width - displayWidth(cell)))

        return alignments[column] === 'right' ? `${padding}${cell}` : `${cell}${padding}`
      })
      .join('  ')
      .trimEnd()

  return [
    formatRow(headers),
    widths.map((width) => '-'.repeat(width)).join('  '),
    ...rows.map((row) => formatRow(row)),
  ].join('\n')
}

function renderGroup(suite: string, results: BenchmarkResult[]): string {
  const params = collectColumns(results, 'params')
  const metrics = collectColumns(results, 'metrics')
  const hasNotes = results.some((result) => result.notes.length > 0)
  const headers = ['scenario', ...params, ...metrics, ...(hasNotes ? ['notes'] : [])]
  const alignment = [
    '---',
    ...params.map((column) => (isNumeric(results, 'params', column) ? '---:' : '---')),
    ...metrics.map((column) => (isNumeric(results, 'metrics', column) ? '---:' : '---')),
    ...(hasNotes ? ['---'] : []),
  ]
  const rows = results.map((result) => {
    const cells = [
      result.scenario,
      ...params.map((column) => formatValue(result.params[column] ?? '')),
      ...metrics.map((column) => formatValue(result.metrics[column] ?? '')),
      ...(hasNotes ? [result.notes.join('; ').replaceAll('|', '\\|')] : []),
    ]

    return `| ${cells.join(' | ')} |`
  })

  return [
    `### ${suite}`,
    '',
    `| ${headers.join(' | ')} |`,
    `| ${alignment.join(' | ')} |`,
    ...rows,
  ].join('\n')
}

export function renderMarkdown(results: BenchmarkResult[]): string {
  const suites = new Map<string, BenchmarkResult[]>()
  for (const result of results) {
    const group = suites.get(result.suite)
    if (group === undefined) suites.set(result.suite, [result])
    else group.push(result)
  }

  const sections: string[] = []
  for (const [suite, group] of suites) sections.push(renderGroup(suite, group))

  return sections.join('\n\n')
}

/** Which metrics best describe each suite, so the compact summary stays narrow. */
export type DomainSummaryProfile = {
  throughput: string
  spread: string
  highlights: string[]
  /** Parameters not already encoded in the scenario name. */
  params: string[]
}

const domainSummaryProfiles: Record<string, DomainSummaryProfile> = {
  coordinator: {
    throughput: 'jobs/sec',
    spread: 'spread (%)',
    highlights: ['claims/job', 'empty claims', 'claim p95 (µs)', 'first handler (ms)'],
    params: [],
  },
  contention: {
    throughput: 'drain jobs/sec',
    spread: 'spread (%)',
    highlights: ['enqueue jobs/sec', 'jobs/claim', 'claim p95 (µs)', 'complete p95 (µs)'],
    params: [],
  },
  'claim-grouping': {
    throughput: 'jobs/sec',
    spread: 'spread (%)',
    // Production reports one `claimQueues` call, the prototype reports one transaction, so both
    // label sets must stay visible instead of silently dropping production latency.
    highlights: [
      'jobs/claim call',
      'claim calls',
      'claim call p95 (µs)',
      'jobs/transaction',
      'commits',
      'transaction p95 (µs)',
      'event loop p95 (µs)',
    ],
    params: [],
  },
  'complete-batch': {
    throughput: 'complete jobs/sec',
    spread: 'spread (%)',
    highlights: ['commits', 'commit p95 (µs)', 'per-job mean (µs)'],
    params: [],
  },
  retention: {
    throughput: 'active jobs/sec',
    spread: 'spread (%)',
    highlights: ['cleanup (ms)', 'delete batches', 'delete batch p95 (µs)', 'db after (MiB)'],
    params: [],
  },
}

/** A value is worth a column only when at least one scenario carries a real signal. */
function hasSignal(results: BenchmarkResult[], key: 'params' | 'metrics', column: string): boolean {
  return results.some((result) => {
    const value = result[key][column]
    if (value === undefined) return false
    if (typeof value === 'string') return value.length > 0

    return value !== 0
  })
}

function domainProfile(results: BenchmarkResult[], metrics: string[]): DomainSummaryProfile {
  const suite = results[0]?.suite ?? ''
  const known = domainSummaryProfiles[suite]
  if (known !== undefined) return known

  const throughput =
    metrics.find((metric) => metric.endsWith('jobs/sec') || metric.endsWith('/sec')) ?? ''
  const spread = metrics.includes('spread (%)') ? 'spread (%)' : ''
  const highlights = metrics
    .filter((metric) => metric !== throughput && metric !== spread)
    .filter((metric) => hasSignal(results, 'metrics', metric))
    .slice(0, 4)

  return { throughput, spread, highlights, params: collectColumns(results, 'params') }
}

/**
 * Compact per-suite rendering for the console: the suite's key parameters, its
 * primary throughput, the repeat-to-repeat spread, and a handful of domain
 * metrics. Profile columns that carry no signal in any scenario are dropped, so
 * inapplicable zeros never widen the table.
 */
export function renderDomainSummary(
  title: string,
  environment: Environment,
  settings: Record<string, MetricValue>,
  results: BenchmarkResult[],
): string {
  if (results.length === 0) return ''

  const profile = domainProfile(results, collectColumns(results, 'metrics'))
  const metricColumns = [profile.throughput, profile.spread, ...profile.highlights].filter(
    (column) => column.length > 0 && hasSignal(results, 'metrics', column),
  )
  const params = profile.params.filter((column) => hasSignal(results, 'params', column))
  const headers = ['scenario', ...params, ...metricColumns]
  const alignments: Array<'left' | 'right'> = [
    'left',
    ...params.map((column): 'left' | 'right' =>
      isNumericColumn(results, 'params', column) ? 'right' : 'left',
    ),
    ...metricColumns.map((column): 'left' | 'right' =>
      isNumericColumn(results, 'metrics', column) ? 'right' : 'left',
    ),
  ]
  const rows = results.map((result) => [
    flattenCell(result.scenario),
    ...params.map((column) => flattenCell(formatValue(result.params[column] ?? ''))),
    ...metricColumns.map((column) => flattenCell(formatValue(result.metrics[column] ?? ''))),
  ])
  const settingsLine = Object.entries(settings)
    .map(([key, value]) => `${key}=${flattenCell(formatValue(value))}`)
    .join(' ')

  return [
    `domain summary — ${flattenCell(title)}`,
    '',
    `env  ${flattenCell(`${environment.node} ${environment.platform}/${environment.arch} · ${environment.cpu} · ${environment.cores} cores`)}`,
    settingsLine.length > 0 ? `opts ${settingsLine}` : 'opts',
    '',
    renderTerminalTable(
      headers.map((header) => flattenCell(header)),
      alignments,
      rows,
    ),
  ].join('\n')
}

export function renderJson(
  environment: Environment,
  options: Record<string, MetricValue>,
  results: BenchmarkResult[],
): string {
  return `${JSON.stringify({ environment, options, results }, null, 2)}\n`
}
