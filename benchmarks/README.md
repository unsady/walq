# Benchmarks

Benchmarks run real queue and SQLite workloads through Vitest Bench. They are not run by `pnpm test` or in CI; their unit tests are.

| Suite            | Measures                                                                               |
| ---------------- | -------------------------------------------------------------------------------------- |
| `coordinator`    | Shared vs isolated queue pollers on one in-memory connection                           |
| `contention`     | SQLite writes across threads, one shared file vs one file per thread                   |
| `claim-grouping` | Production grouped claims; the full grid also compares benchmark-only claim prototypes |
| `retention`      | Active work with retained history, batched deletion, and VACUUM                        |
| `complete-batch` | Benchmark-only batched completion prototype (not included in `pnpm bench`)             |

```sh
pnpm bench                 # quick production grids
pnpm bench:full            # full production grids
pnpm bench:experiments     # full prototype comparisons
pnpm bench:queue           # coordinator only
pnpm bench:contention
pnpm bench:claim-grouping
pnpm bench:retention
pnpm bench:complete-batch   # prototype only
```

Each suite prints one domain summary table. The default is three measured runs and one discarded warmup per scenario; results report the median across measured runs. Set `BENCH_JSON=bench.json` to save per-suite artifacts (for example, `bench.coordinator.json`) with environment details, all metrics, and raw per-run samples. A failed or incomplete run invalidates its scenario and fails the command.

## Options

| Variable                                    | Default   | Purpose                                                                                        |
| ------------------------------------------- | --------- | ---------------------------------------------------------------------------------------------- |
| `BENCH_GRID`                                | `quick`   | `quick` or `full`                                                                              |
| `BENCH_REPEATS` / `BENCH_WARMUP`            | `3` / `1` | Measured / discarded runs per scenario                                                         |
| `BENCH_JOBS`                                | per suite | Jobs per run (1000 coordinator/retention, 2000 contention, 4096 claim-grouping/complete-batch) |
| `BENCH_ONLY`                                | unset     | Keep scenario names containing this text                                                       |
| `BENCH_JSON`                                | unset     | Base filename for per-suite JSON artifacts                                                     |
| `BENCH_SYNCHRONOUS`                         | `normal`  | SQLite durability: `normal` or `full` (file-backed suites)                                     |
| `BENCH_RETENTION_BATCH`                     | grid      | Override retention deletion batch size                                                         |
| `BENCH_CLAIM_QUEUES` / `BENCH_CLAIM_LIMITS` | grid      | Comma-separated claim-grouping tiers                                                           |
| `BENCH_CLAIM_MODES`                         | grid      | `current`, `grouped`, `production`                                                             |
| `BENCH_CLAIM_CHUNKS`                        | `all`     | Queues per `grouped` transaction: `all` or comma-separated sizes                               |

To run a particular suite, use its `pnpm bench:*` command or filter files with `pnpm exec vitest bench --run benchmarks/coordinator.bench.ts`. The production claim-grouping quick grid uses `production` only; the full grid includes the prototypes. For example, to compare prototype chunk sizes:

```sh
BENCH_CLAIM_QUEUES=128,256 BENCH_CLAIM_LIMITS=16 BENCH_CLAIM_MODES=grouped \
  BENCH_CLAIM_CHUNKS=all,16,32,64 pnpm bench:claim-grouping
```

The [chunk-size decision](reports/grouped-claim-chunk-size.md) records why the adapter uses a 512-job budget.

The standalone [round-robin write-cost experiment](claim-write-cost.mjs) compares identical
writes on the old v10 and new v12 SQLite schemas. Run `pnpm build && node benchmarks/claim-write-cost.mjs`.

The [group scheduling experiment](group-scheduling.mjs) measures ready ungrouped batches,
saturated groups, future-due groups, and group fairness. Run
`pnpm build && BENCH_FUTURE_GROUPS=20000 node benchmarks/group-scheduling.mjs`.
For the saturated-group-plus-future-groups regression, run
`BENCH_JOBS=100001 BENCH_FUTURE_GROUPS=8 node benchmarks/group-scheduling.mjs`
and inspect `blocked-future`.

## Reading results

The summary shows throughput, spread across runs, and selected suite-specific metrics. JSON contains the remaining metrics and individual runs. Compare like-for-like scenarios on the same idle machine; small differences (roughly below 10%) need more repeats. `synchronous=NORMAL` and `FULL` can produce different absolute rates. The coordinator suite is in-memory and does not measure disk contention.

`claim-grouping` prototype transaction timings and production `claimQueues` call timings are **not** equivalent: one production call may span several transactions. Likewise, `complete-batch` compares production async completion with direct synchronous prototype SQL, not just batch size. In `contention`, separate files also change caches and WAL files, so the throughput gap is not a pure measurement of lock wait.

The grouped claim prototype targets ungrouped jobs and uses the production pending index and ordering. It does not implement all production features, so prototype vs production is not a pure chunk-size comparison. Its chunks execute synchronously back-to-back, whereas production yields to the event loop between committed chunks. The claim event-loop probe observes successive `setImmediate` turns throughout each round; JSON includes per-run sample counts and p95/p99. Production call latency includes all chunks and yields, not an individual transaction.

The harness logs scenario names and warmup/repeat progress, alternates scenario order between passes, rejects incomplete work, lost leases and duplicate completions, and closes workers/databases after each run. Every run has a 60-second budget. Claim-grouping checks that budget between synchronous operations; an individual SQLite call cannot be interrupted by a JavaScript timer. Keep JSON artifacts when comparing changes; the console summary is intentionally compact.
