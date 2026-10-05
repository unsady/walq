# Benchmarks

Run real queue and SQLite workloads through a standalone CLI and shared harness.
Performance suites are not run by `pnpm test` or CI; their correctness and
reporting tests are. Vitest is used only for those tests.

| Suite            | Question                                                              |
| ---------------- | --------------------------------------------------------------------- |
| `adapters`       | SQLite drivers on Node.js; built-in adapter across Node.js, Bun, Deno |
| `coordinator`    | Shared vs isolated pollers on one in-memory connection                |
| `contention`     | Shared WAL file vs one file per writer thread                         |
| `journal`        | WAL vs DELETE with FULL durability and competing workers              |
| `claim-grouping` | Production grouped claims and prototype chunk-size comparisons        |
| `groups`         | Ready jobs, saturated/future groups, and round-robin fairness         |
| `retention`      | Retained history, production `cleanup()` batches, and optional VACUUM |

```sh
pnpm bench                              # quick grids
pnpm bench:full                         # full grids
pnpm bench groups                      # one suite
pnpm bench groups contention           # selected suites, sequentially
pnpm bench --help
BENCH_ONLY=blocked-future pnpm bench groups
BENCH_JSON=.cache/benchmarks/bench.json pnpm bench
```

`pnpm bench` builds packages, suites, and worker entrypoints before starting
measurements. To repeat runs without rebuilding, use
`node .cache/benchmarks/cli.js groups`. Suites run sequentially; a failed suite
stops the CLI with a nonzero exit code. Unknown suites, empty scenario selections,
and zero measured repeats are errors.

Each suite prints one summary table. JSON is optional and contains environment,
metrics, and per-run samples; the example creates
`.cache/benchmarks/bench.groups.json` and sibling suite files. Parent directories
are created automatically. Keep generated artifacts outside tracked source paths.

## Adapter comparisons

On Node.js, compare `better-sqlite3` and `node:sqlite`:

```sh
pnpm bench adapters
pnpm bench:adapters
```

All suites share the same collector and reporting. Suites are loaded only when
selected, so adapter comparisons do not load Node-only worker workloads under
Bun or Deno. Build once, then run sequentially on the same idle host:

```sh
pnpm bench:build
BENCH_ADAPTERS=node:sqlite BENCH_JSON=.cache/benchmarks/compare.json node .cache/benchmarks/cli.js adapters
BENCH_JSON=.cache/benchmarks/compare.json bun .cache/benchmarks/cli.js adapters
BENCH_JSON=.cache/benchmarks/compare.json deno run --allow-read --allow-write --allow-env --allow-sys .cache/benchmarks/cli.js adapters
```

The CLI loads built package entry points explicitly, including under Bun. Bun and Deno default to `node:sqlite` only; comparing the native `better-sqlite3` addon is limited to Node.js. CLI artifacts have runtime suffixes, for example `compare.adapters.bun.json`, and include actual runtime and SQLite versions, per-run counts/durations, and raw call timings. `BENCH_ADAPTERS=better-sqlite3,node:sqlite` selects Node.js drivers explicitly. Filter workloads with `BENCH_ONLY=wal/claim` or `BENCH_ONLY=lifecycle`.

Each adapter runs the same five workloads in memory and on a fresh WAL file:

- `enqueueMany`: batches of 64 jobs into an empty queue.
- `claim`: batches of 64 pre-seeded, ungrouped jobs.
- `claimQueues-grouped`: 16 queues with 16 eligible groups per queue, requesting 64 jobs per queue per sweep. This exercises multiple 512-job transaction chunks; groups are not saturated.
- `lifecycle`: enqueue a batch, claim it, then complete each job individually. Counts a job once, not once per operation.
- `cleanup`: remove pre-seeded completed jobs in batches of 64; creation and completion are outside the measured phase.

Quick runs use 1024 jobs and full runs use 10,000 unless `BENCH_JOBS` is set. File runs use WAL, the selected `BENCH_SYNCHRONOUS` mode, 4096-byte pages, a 2000-KiB cache, a 5000-ms busy timeout, and a 1000-page auto-checkpoint. Connection setup, schema initialization, seeding, correctness checks, and teardown are excluded from operation timings. The in-memory variant does not measure filesystem durability.

`jobs/sec` is actual processed jobs divided by the sum of timed storage-call durations across measured runs; it is **not** end-to-end application throughput. Call p95 is the median of per-run p95 values; lifecycle latency mixes enqueue, claim, and completion calls. The report shows sample counts: short runs, especially grouped sweeps, do not support reliable tail-latency claims. Increase jobs and repeats before drawing conclusions. Paired drivers are adjacent and execution order reverses between passes.

Cross-runtime results compare the whole runtime/driver/SQLite combination, not just JavaScript engines. Different bundled SQLite versions or compile options can contribute to differences. These benchmarks use one connection and no concurrent producers; they do not establish contention or multi-process scaling results.

## WAL vs DELETE journal

```sh
pnpm bench journal
BENCH_JOBS=10000 BENCH_REPEATS=5 BENCH_JSON=.cache/benchmarks/journal.json pnpm bench journal
```

Compares `journal_mode=WAL` and `journal_mode=DELETE` for 1 and 4 competing
worker threads, each with a separate `better-sqlite3` connection to the same
fresh file, at claim batch sizes 1 and 16. The total job count is fixed across
the pool, not multiplied by worker count. Only journal mode changes within a
pair; both use `synchronous=FULL` regardless of `BENCH_SYNCHRONOUS`, a 2000-ms
busy timeout, identical jobs and transaction boundaries. Each completion is
one production `complete()` call, not a batched transaction. SQLite's default
WAL auto-checkpoint is retained, including any checkpoints during measurement.
Quick runs use 2000 jobs, full runs 10,000; both use the same eight scenarios.

Schema setup, seeding and per-worker in-memory JIT warmup are outside the timed
phase. Workers start draining behind a shared barrier and each finishes with
128 empty claims after finding no ready jobs. `jobs/sec` is actual confirmed
completions divided by summed wall-clock drain spans across valid repeats,
including the empty tail and event-loop yields, not summed call durations.
Successful nonempty claims, single-job completions and empty claims have
separate p95 metrics. Sample counts and raw millisecond timings are kept in JSON;
p95 values are medians of per-run p95s, with worker samples pooled within a run.

The event-loop probe measures successive `setImmediate` turns inside each worker,
yielding once per claim batch or empty claim. Its intervals include synchronous
claim/completion work and lock waits, so this is worker responsiveness under the
specified batching policy, not idle timer jitter or the parent's event-loop delay.
Reported max is the median of per-run maxima. Short runs have noisy tail estimates.

`SQLITE_BUSY` counts returned errors (including extended busy codes), not lock
waits successfully resolved by SQLite's busy handler. `timeouts` counts explicit
`SQLITE_BUSY_TIMEOUT` codes and run deadline expirations; this driver may return
plain `SQLITE_BUSY` when its busy timeout expires, which cannot be separately
identified. Counts include invalid measured runs, not warmups. Other storage
errors are reported too. Incomplete, duplicate, lost-lease, aborted or erroring
runs fail the suite and are excluded from performance metrics; raw returned
outcomes and failure notes remain available in JSON.

## Options

| Variable                                    | Default              | Purpose                                         |
| ------------------------------------------- | -------------------- | ----------------------------------------------- |
| `BENCH_GRID`                                | `quick`              | `quick` or `full`                               |
| `BENCH_REPEATS` / `BENCH_WARMUP`            | `3` / `1`            | Measured / discarded runs                       |
| `BENCH_JOBS`                                | per suite            | Job count; groups requires at least 257         |
| `BENCH_ONLY`                                | unset                | Scenario-name substring filter                  |
| `BENCH_JSON`                                | unset                | Base path for per-suite JSON artifacts          |
| `BENCH_SYNCHRONOUS`                         | `normal`             | SQLite durability: `normal` or `full`           |
| `BENCH_RETENTION_BATCH`                     | grid                 | Production cleanup row limit                    |
| `BENCH_GROUPS` / `BENCH_FUTURE_GROUPS`      | 64 quick / 2000 full | Saturated / future group counts in groups suite |
| `BENCH_CLAIM_QUEUES` / `BENCH_CLAIM_LIMITS` | grid                 | Comma-separated claim-grouping tiers            |
| `BENCH_CLAIM_MODES`                         | grid                 | `current`, `grouped`, `production`              |
| `BENCH_CLAIM_CHUNKS`                        | `all`                | Queues per prototype transaction                |

The claim-grouping quick grid uses production only; full adds prototypes. To
revisit the [512-job chunk decision](reports/grouped-claim-chunk-size.md):

```sh
BENCH_CLAIM_QUEUES=128,256 BENCH_CLAIM_LIMITS=16 BENCH_CLAIM_MODES=grouped \
  BENCH_CLAIM_CHUNKS=all,16,32,64 pnpm bench claim-grouping
```

The groups suite preserves distinct scheduling and stress workloads. `ready`
measures 16 batch claims; most selection scenarios measure the first claim and
10 empty follow-ups. `mixed` measures 64 single claims, reporting groups served.
`heavy-fairness` checks four jobs per group in eight 256-job claims across 64
groups. `multiple-saturated-due-groups` checks a ready group after future-only
and saturated due groups. Its future-only prefix stays at eight groups.
Setup is outside claim timings; claim rates include empty calls, not just jobs.

## Reading results

Most suites aggregate per-run metrics by median; adapter throughput uses total jobs divided by total timed duration. Latency summaries give each run one
vote rather than pooling unequal sample counts. Compare like-for-like workloads
on the same idle host, retaining JSON for later analysis. Small differences need
more repeats; short-run tail percentiles are noisy.

- Coordinator is in-memory and does not measure disk contention.
- Contention changes caches and WAL files as well as locking; it does not isolate lock wait.
- Claim-grouping prototypes support ungrouped jobs only and do not implement all production features. Prototype transaction latency differs from production call latency, which may span multiple transactions and event-loop yields.
- The claim event-loop probe samples successive `setImmediate` turns throughout each round.
- Retention times production cleanup separately from active processing after cleanup. `reopened` reopens the connection before active processing, not before cleanup. VACUUM and checkpoint remain explicit maintenance, not production cleanup behavior.

The harness alternates scenario order across passes and rejects failed or
incomplete measured work. Workers and databases are closed after each run.
See [retired experiments](reports/retired-experiments.md) for comparisons no
longer maintained.
