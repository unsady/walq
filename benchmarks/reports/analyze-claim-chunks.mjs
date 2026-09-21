#!/usr/bin/env node
// Renders the grouped-claim chunk-size report from benchmark artifacts.
//
// Artifacts are produced by the claim-grouping suite, which writes
// `bench.claim-grouping.json` per target directory:
//
//   BENCH_GRID=quick BENCH_REPEATS=5 BENCH_WARMUP=1 BENCH_JOBS=131072 \
//     BENCH_CLAIM_QUEUES=128,256 BENCH_CLAIM_LIMITS=16 BENCH_CLAIM_MODES=grouped \
//     BENCH_CLAIM_CHUNKS=all,16,32,64 BENCH_SYNCHRONOUS=normal \
//     BENCH_JSON=<out>/normal/bench.json pnpm exec vitest bench --run benchmarks/claim-grouping.bench.ts
//
// The competing-writer confirmation pass repeats the suite with
// `BENCH_ONLY=competing BENCH_REPEATS=8`.
//
// Usage: node analyze-claim-chunks.mjs <artifactsDir> [outputFile]
//
// Expects `{normal,full}/bench.claim-grouping.json` and, optionally,
// `confirm/{normal,full}/bench.claim-grouping.json` below <artifactsDir>.

import { existsSync, readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'

const root = process.argv[2] ?? 'artifacts'
const output = process.argv[3] ?? join(root, 'REPORT.md')

function readJson(path) {
  return existsSync(path) ? JSON.parse(readFileSync(path, 'utf8')) : undefined
}

const main = {
  normal: readJson(join(root, 'normal', 'bench.claim-grouping.json')),
  full: readJson(join(root, 'full', 'bench.claim-grouping.json')),
}
const confirm = {
  normal: readJson(join(root, 'confirm', 'normal', 'bench.claim-grouping.json')),
  full: readJson(join(root, 'confirm', 'full', 'bench.claim-grouping.json')),
}

const chunkOrder = ['all', '16', '32', '64']
function find(artifact, queues, chunk, placement) {
  return (artifact?.results ?? []).find(
    (result) =>
      result.scenario.includes(`/ ${queues} queues /`) &&
      String(result.params.chunk) === String(chunk) &&
      result.scenario.includes(`/ ${placement} /`),
  )
}
function f(value, digits = 0) {
  return value === undefined || value === null ? '—' : Number(value).toFixed(digits)
}
function pct(value, base) {
  return base === 0 ? '—' : `${((value / base) * 100).toFixed(0)}%`
}
// Pads every column to its widest cell so the generated markdown matches the
// repository formatter and a regeneration only changes the values.
function table(headers, rows) {
  const widths = headers.map((header, index) =>
    Math.max(3, header.length, ...rows.map((row) => row[index].length)),
  )
  function line(cells) {
    return `| ${cells.map((cell, index) => cell.padEnd(widths[index])).join(' | ')} |`
  }
  return [line(headers), line(widths.map((width) => '-'.repeat(width))), ...rows.map(line)]
}

const lines = []
lines.push('# Grouped claim chunk size — 16 / 32 / 64 versus one big transaction')
lines.push('')
lines.push(`Generated ${new Date().toISOString()}`)
lines.push('')
const env = main.normal?.environment ?? main.full?.environment
if (env !== undefined) {
  lines.push(
    ...table(
      ['', ''],
      [
        [
          'Environment',
          `node ${env.node}, ${env.cpu}, ${env.cores} cores, ${env.platform}/${env.arch}`,
        ],
        [
          'Grid',
          'quick, `BENCH_CLAIM_QUEUES=128,256`, `BENCH_CLAIM_LIMITS=16`, `BENCH_CLAIM_MODES=grouped`, `BENCH_CLAIM_CHUNKS=all,16,32,64`',
        ],
        [
          'Jobs',
          `${main.normal?.options.jobs} per run (~${Math.round(main.normal?.options.jobs / (128 * 16))} all-queue rounds at 128 queues, ~${Math.round(main.normal?.options.jobs / (256 * 16))} at 256)`,
        ],
        [
          'Repeats / warmup',
          `${main.normal?.options.repeats} measured + ${main.normal?.options.warmup} discarded per scenario (competitor confirmation: ${confirm.normal?.options.repeats} + ${confirm.normal?.options.warmup})`,
        ],
      ],
    ),
  )
  lines.push('')
}
lines.push(
  '`chunk all` is the original behaviour: one `BEGIN IMMEDIATE … COMMIT` for every queue in the',
)
lines.push(
  'round. `chunk N` splits the round into `ceil(queues / N)` transactions, each claiming at most',
)
lines.push(
  '`N × limit` jobs. The competitor is a separate worker thread with its own connection to the',
)
lines.push(
  'same WAL file; `jobs/sec` excludes the unmeasured competitor pauses and counts only claim time.',
)
lines.push('')
lines.push('## Findings')
lines.push('')
lines.push(
  '- **Chunking cuts the writer-lock hold roughly proportionally to the chunk.** At 256 queues the p95 transaction duration drops from ~100 ms (`all`) to ~29 ms (`64`), ~18 ms (`32`) and ~12 ms (`16`); at 128 queues `all` is ~57 ms. `synchronous` does not move these numbers — the hold is dominated by the per-queue `recover`/`select`/`acquire` work, not the commit fsync.',
)
lines.push(
  '- **Throughput barely moves.** Every chunk tier stays within ~5–12% of the single big transaction on both modes and queue counts; `all` is the fastest, the difference is mostly the extra commits.',
)
lines.push(
  '- **The neighbour writer sees the chunk size, not the durability mode.** Competitor p95 tracks the lock hold: `all` ~39–68 ms, `64` ~22–40 ms, `32` ~10–22 ms, `16` ~5–23 ms. Chunk 32 lands on the most stable low step because the observed competitor wait is quantised by SQLite’s busy-handler backoff.',
)
lines.push(
  '- Competitor p99 is noisy (rare scheduler/checkpoint events over a few hundred ops per run) and should be read as an order of magnitude, not a precise number. At 256 queues under FULL every tier lands in the 90–137 ms p99 range, so that tail is a write-volume/checkpoint effect rather than a chunk-size effect.',
)
lines.push('')
lines.push('## Outcome')
lines.push('')
lines.push(
  'Chunk 32 is implemented as a 512-job budget in `packages/better-sqlite3/src/chunking.ts`: a',
)
lines.push(
  'grouped round packs requests until their summed limits reach the budget, so a uniform limit',
)
lines.push(
  'covers `floor(512 / limit)` queues per immediate transaction (limit 16 -> 32 queues, 32 -> 16,',
)
lines.push(
  '64 -> 8) and a request above the budget takes a transaction of its own. `chunk 64` is the',
)
lines.push(
  'throughput-leaning alternative and `chunk 16` the latency-leaning one if the budget is retuned.',
)
lines.push(
  'Because the hold scales with `chunk × limit`, the job budget is a more portable default than a',
)
lines.push('fixed queue count.')
lines.push('')
lines.push(
  'A verification run of the shipped path (`BENCH_CLAIM_MODES=production`) lands within ~1% of the',
)
lines.push(
  '`grouped / chunk 32` prototype throughput at 128 and 256 queues, and the adapter tests cover the',
)
lines.push(
  'chunk boundaries: a failure in a later chunk keeps the earlier chunks committed, and a 70-queue',
)
lines.push('batch at limit 16 is claimed in three transactions without duplicates.')
lines.push(
  'The `production` mode can only time the whole `claimQueues` call, so its transaction percentiles,',
)
lines.push('commit count, and jobs/transaction describe the call, not one internal chunk.')
lines.push('')
lines.push('## Reproduce')
lines.push('')
lines.push('```sh')
lines.push('# main sweep, once per mode')
lines.push('BENCH_GRID=quick BENCH_REPEATS=5 BENCH_WARMUP=1 BENCH_JOBS=131072 \\')
lines.push('  BENCH_CLAIM_QUEUES=128,256 BENCH_CLAIM_LIMITS=16 BENCH_CLAIM_MODES=grouped \\')
lines.push('  BENCH_CLAIM_CHUNKS=all,16,32,64 BENCH_SYNCHRONOUS=normal \\')
lines.push(
  '  BENCH_JSON=<out>/normal/bench.json pnpm exec vitest bench --run benchmarks/claim-grouping.bench.ts',
)
lines.push('')
lines.push('# competing-writer confirmation with more repeats')
lines.push(
  'BENCH_GRID=quick BENCH_REPEATS=8 BENCH_WARMUP=1 BENCH_JOBS=131072 BENCH_ONLY=competing \\',
)
lines.push('  BENCH_CLAIM_QUEUES=128,256 BENCH_CLAIM_LIMITS=16 BENCH_CLAIM_MODES=grouped \\')
lines.push('  BENCH_CLAIM_CHUNKS=all,16,32,64 BENCH_SYNCHRONOUS=normal \\')
lines.push(
  '  BENCH_JSON=<out>/confirm/normal/bench.json pnpm exec vitest bench --run benchmarks/claim-grouping.bench.ts',
)
lines.push('')
lines.push('node benchmarks/reports/analyze-claim-chunks.mjs <out>')
lines.push('```')
lines.push('')

for (const mode of ['normal', 'full']) {
  const artifact = main[mode]
  if (artifact === undefined) continue
  lines.push(`## Throughput and lock hold — ${mode.toUpperCase()}`)
  lines.push('')
  const rows = []
  for (const queues of [128, 256]) {
    const baseline = find(artifact, queues, 'all', 'solo')?.metrics['jobs/sec']
    for (const chunk of chunkOrder) {
      const solo = find(artifact, queues, chunk, 'solo')
      const competing = find(artifact, queues, chunk, 'competing')
      const metrics = solo?.metrics ?? competing?.metrics
      if (metrics === undefined) continue
      rows.push([
        String(queues),
        String(chunk),
        f(metrics['jobs/transaction']),
        f(metrics.commits),
        f(metrics['jobs/sec']),
        pct(metrics['jobs/sec'], baseline),
        f(competing?.metrics['jobs/sec']),
        f(metrics['transaction p50 (µs)']),
        f(metrics['transaction p95 (µs)']),
        f(metrics['transaction p99 (µs)']),
      ])
    }
  }
  lines.push(
    ...table(
      [
        'queues',
        'chunk',
        'jobs/txn',
        'commits',
        'solo jobs/sec',
        'vs all',
        'competing jobs/sec',
        'txn p50 µs',
        'txn p95 µs',
        'txn p99 µs',
      ],
      rows,
    ),
  )
  lines.push('')
}

for (const mode of ['normal', 'full']) {
  const artifact = confirm[mode]
  if (artifact === undefined) continue
  lines.push(`## Competitor latency — ${mode.toUpperCase()} (${artifact.options.repeats} repeats)`)
  lines.push('')
  const rows = []
  for (const queues of [128, 256]) {
    for (const chunk of chunkOrder) {
      const result = find(artifact, queues, chunk, 'competing')
      if (result === undefined) continue
      const m = result.metrics
      rows.push([
        String(queues),
        String(chunk),
        f(m['enqueue p95 (µs)']),
        f(m['enqueue p99 (µs)']),
        f(m['complete p95 (µs)']),
        f(m['complete p99 (µs)']),
        f(m['competitor ops']),
      ])
    }
  }
  lines.push(
    ...table(
      ['queues', 'chunk', 'competitor enqueue p95', 'p99', 'competitor complete p95', 'p99', 'ops'],
      rows,
    ),
  )
  lines.push('')
}

lines.push('## Trade-off summary (256 queues, limit 16)')
lines.push('')
lines.push(
  ...table(
    [
      'chunk',
      'jobs/transaction',
      'lock hold p95',
      'competitor p95',
      'throughput vs all',
      'verdict',
    ],
    [
      ['all', '4096', '~100 ms', '~63–68 ms', '100%', 'fastest, holds the writer lock too long'],
      ['64', '1024', '~29 ms', '~23–40 ms', '~93–94%', 'throughput-leaning alternative'],
      ['**32**', '**512**', '**~18 ms**', '**~10–22 ms**', '**~91–92%**', '**shipped default**'],
      ['16', '256', '~12 ms', '~5–23 ms', '~88–90%', 'shortest hold, noisier neighbour tail'],
    ],
  ),
)

// The file ends on the table so the output matches the repository formatter.
writeFileSync(output, `${lines.join('\n')}\n`)
console.log(`wrote ${output}`)
