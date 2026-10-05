# Benchmarks

Run real queue and SQLite workloads through a standalone CLI and shared harness.
Performance suites are not run by `pnpm test` or CI; their correctness and
reporting tests are. Vitest is used only for those tests.

| Suite               | Question                                                                       | Quick / full cases |
| ------------------- | ------------------------------------------------------------------------------ | ------------------ |
| `adapters`          | SQLite drivers on Node.js; built-in adapter across Node.js, Bun, Deno          | 6 / 6              |
| `journal`           | WAL vs DELETE at FULL; WAL/NORMAL vs WAL/FULL; claim batch 1 vs 16             | 6 / 6              |
| `claim-grouping`    | Production `claimQueues` throughput and responsiveness with a competing writer | 2 / 8              |
| **Total (Node.js)** |                                                                                | **14 / 20**        |

Keep a benchmark only when it informs a decision or detects a performance
regression. Scheduling correctness belongs in tests; completed experiments keep
their decision records, not maintained runners.

```sh
pnpm bench                              # 14 quick cases on Node.js
pnpm bench:full                         # 20 full cases on Node.js
pnpm bench journal                      # one suite
pnpm bench adapters journal          # selected suites, sequentially
pnpm bench --help
BENCH_ONLY=competing pnpm bench claim-grouping
BENCH_JSON=.cache/benchmarks/bench.json pnpm bench
```

`pnpm bench` builds packages, suites, and worker entrypoints before starting
measurements. To repeat runs without rebuilding, use
`node .cache/benchmarks/cli.js journal`. Suites run sequentially; a failed suite
stops the CLI with a nonzero exit code. Unknown suites, empty scenario selections,
and zero measured repeats are errors.

Each suite prints one summary table. JSON is optional and contains environment,
metrics, and per-run samples; the example creates
`.cache/benchmarks/bench.journal.json` and sibling suite files. Parent directories
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

Each adapter runs the same three workloads on a fresh WAL file:

- `enqueueMany`: batches of 64 jobs into an empty queue.
- `claim`: batches of 64 pre-seeded, ungrouped jobs.
- `lifecycle`: enqueue a batch, claim it, then complete each job individually. Counts a job once, not once per operation.

Quick runs use 1024 jobs and full runs use 10,000 unless `BENCH_JOBS` is set. Runs use WAL, the selected `BENCH_SYNCHRONOUS` mode, 4096-byte pages, a 2000-KiB cache, a 5000-ms busy timeout, and a 1000-page auto-checkpoint. Connection setup, schema initialization, seeding, correctness checks, and teardown are excluded from operation timings.

`jobs/sec` is actual processed jobs divided by the sum of timed storage-call durations across measured runs; it is **not** end-to-end application throughput. Call p95 is the median of per-run p95 values; lifecycle latency mixes enqueue, claim, and completion calls. The report shows sample counts: short runs do not support reliable tail-latency claims. Increase jobs and repeats before drawing conclusions. Paired drivers are adjacent and execution order reverses between passes.

Cross-runtime results compare the whole runtime/driver/SQLite combination, not just JavaScript engines. Different bundled SQLite versions or compile options can contribute to differences. These benchmarks use one connection and no concurrent producers; they do not establish contention or multi-process scaling results.

## Production claims

The full claim-grouping grid uses 1/32 queues, claim limits 1/16, and solo/competing
placements. Quick selects 32 queues at limit 16 in both placements. All cases call
production `claimQueues`; call latency can span multiple internal transactions.
The event-loop probe samples successive `setImmediate` turns throughout each
claim round. Jobs/sec counts claimed jobs over measured claim-round durations,
excluding deliberate pauses between rounds. Competing writer latency includes
its complete storage calls, not only SQLite lock wait. The default is 4096 jobs.

## Journal and durability comparisons

```sh
pnpm bench journal
BENCH_JOBS=10000 BENCH_REPEATS=5 BENCH_JSON=.cache/benchmarks/journal.json pnpm bench journal
```

Six scenarios use one `better-sqlite3` connection in one worker thread on a fresh
file, at claim batch sizes 1 and 16:

- WAL/NORMAL vs WAL/FULL isolates synchronous durability.
- WAL/FULL vs DELETE/FULL isolates journal mode.
- Batch 1 vs 16 isolates claim batching within each mode; completion is still
  one production `complete()` call per job.

Both grids use these six scenarios: 2000 jobs in quick, 10,000 in full.
All cases use a 2000-ms busy timeout and identical jobs and transaction
boundaries. `BENCH_SYNCHRONOUS` does not override these explicit scenario settings.
SQLite's default WAL auto-checkpoint is retained, including checkpoints during
measurement.

`FULL drop (%)` is `100 × (1 − WAL/FULL jobs/sec ÷ WAL/NORMAL jobs/sec)` at the
same batch size and job count. Positive values mean lower FULL throughput;
negative values mean higher FULL throughput in that run. It appears only for
valid WAL/FULL rows with a valid matching NORMAL baseline; filtering out the
baseline leaves the comparison blank. Raw timings and rates remain in JSON.

Schema setup, seeding and per-worker in-memory JIT warmup are outside the timed
phase. The worker starts draining behind a barrier and finishes with 128 empty
claims after finding no ready jobs. `jobs/sec` is actual confirmed completions
divided by summed wall-clock drain spans across valid repeats, including the
empty tail and event-loop yields, not summed call durations.
Successful nonempty claims, single-job completions and empty claims have
separate p95 metrics. Sample counts and raw millisecond timings are kept in JSON;
p95 values are medians of per-run p95s, with worker samples pooled within a run.

The event-loop probe measures successive `setImmediate` turns inside each worker,
yielding once per claim batch or empty claim. Its intervals include synchronous
claim/completion work, so this is worker responsiveness under the
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

| Variable                                    | Default           | Purpose                                                                         |
| ------------------------------------------- | ----------------- | ------------------------------------------------------------------------------- |
| `BENCH_GRID`                                | `quick`           | `quick` or `full`                                                               |
| `BENCH_REPEATS` / `BENCH_WARMUP`            | `3` / `1`         | Measured / discarded runs                                                       |
| `BENCH_JOBS`                                | per suite         | Total job count                                                                 |
| `BENCH_ONLY`                                | unset             | Scenario-name substring filter                                                  |
| `BENCH_JSON`                                | unset             | Base path for per-suite JSON artifacts                                          |
| `BENCH_SYNCHRONOUS`                         | `normal`          | SQLite durability: `normal` or `full`; journal uses explicit per-scenario modes |
| `BENCH_ADAPTERS`                            | runtime-dependent | Comma-separated driver selection                                                |
| `BENCH_CLAIM_QUEUES` / `BENCH_CLAIM_LIMITS` | grid              | Comma-separated production claim tiers                                          |

## Reading results

Most suites aggregate per-run metrics by median; adapter throughput and journal
drain throughput use actual elapsed totals. Latency summaries give each run one
vote rather than pooling unequal sample counts. Compare like-for-like workloads
on the same idle host, retaining JSON for later analysis. Small differences need
more repeats; short-run tail percentiles are noisy.

The harness alternates scenario order across passes and rejects failed or
incomplete measured work. Workers and databases are closed after each run.
See the [512-job chunk decision](reports/grouped-claim-chunk-size.md) and
[retired experiments](reports/retired-experiments.md) for historical comparisons.
