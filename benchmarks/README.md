# Benchmarks

Run real queue and SQLite workloads through Vitest Bench. Performance suites are
not run by `pnpm test` or CI; their correctness and reporting tests are.

| Suite            | Question                                                              |
| ---------------- | --------------------------------------------------------------------- |
| `coordinator`    | Shared vs isolated pollers on one in-memory connection                |
| `contention`     | Shared WAL file vs one file per writer thread                         |
| `claim-grouping` | Production grouped claims and prototype chunk-size comparisons        |
| `groups`         | Ready jobs, saturated/future groups, and round-robin fairness         |
| `retention`      | Retained history, production `cleanup()` batches, and optional VACUUM |

```sh
pnpm bench                              # quick grids
pnpm bench:full                         # full grids
pnpm bench benchmarks/groups.bench.ts   # one suite
BENCH_ONLY=blocked-future pnpm bench benchmarks/groups.bench.ts
BENCH_JSON=.cache/benchmarks/bench.json pnpm bench
```

Each suite prints one summary table. JSON is optional and contains environment,
metrics, and per-run samples; the example creates
`.cache/benchmarks/bench.groups.json` and sibling suite files. Parent directories
are created automatically. Keep generated artifacts outside tracked source paths.

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
  BENCH_CLAIM_CHUNKS=all,16,32,64 pnpm bench benchmarks/claim-grouping.bench.ts
```

The groups suite preserves distinct scheduling and stress workloads. `ready`
measures 16 batch claims; most selection scenarios measure the first claim and
10 empty follow-ups. `mixed` measures 64 single claims, reporting groups served.
`heavy-fairness` checks four jobs per group in eight 256-job claims across 64
groups. `multiple-saturated-due-groups` checks a ready group after future-only
and saturated due groups. Its future-only prefix stays at eight groups.
Setup is outside claim timings; claim rates include empty calls, not just jobs.

## Reading results

Results aggregate per-run metrics by median; latency summaries give each run one
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
