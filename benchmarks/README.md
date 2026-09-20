# walq benchmarks

Two suites measure different things. Both run through Vitest Bench with the custom provider in
`benchmarks/vitest-provider.ts`. The provider drives the real integration workloads through
`collectRuns()` from `benchmarks/harness.ts`, so every scenario still runs `warmup + repeats`
times with the order alternating between passes, and it returns Tinybench-shaped rows so the
standard Vitest table renders one row per scenario.

```sh
pnpm bench             # both suites, quick grids (~10s)
pnpm bench:queue       # coordinator suite only
pnpm bench:contention  # SQLite contention suite only
pnpm bench:full        # full matrices (~1min)
```

The scripts run `vitest bench --run --reporter=verbose`. The verbose reporter is used because the
default reporter switches to the minimal renderer in CI/agent environments, which hides the
benchmark table. `.bench.ts` files are only picked up by `vitest bench`; `pnpm test` still runs
only `*.test.ts`, so the heavy workloads never run there.

## Selecting and tuning

| Setting         | Default   | Meaning                                               |
| --------------- | --------- | ----------------------------------------------------- |
| `BENCH_GRID`    | `quick`   | `quick` or `full` matrices (`bench:full` sets `full`) |
| `BENCH_REPEATS` | 3         | Measured runs per scenario                            |
| `BENCH_WARMUP`  | 1         | Discarded runs per scenario                           |
| `BENCH_JOBS`    | per suite | Jobs per run (1000 for coordinator, 2000 for SQLite)  |
| `BENCH_ONLY`    |           | Run scenarios whose name contains the text            |
| `BENCH_JSON`    |           | Base path for the per-suite domain JSON artifacts     |

The previous `--suite`, `--repeats`, `--warmup`, `--jobs`, `--only`, `--json`, `--full` and
`--help` flags, and the `BENCH_SUITES` variable, are gone because `vitest bench` owns the CLI.
Select a suite with the `pnpm bench:*` scripts or a Vitest file filter, for example
`vitest bench --run benchmarks/coordinator.bench.ts`, and pass the remaining settings through the
`BENCH_*` variables. Unrelated flags are forwarded to Vitest unchanged.

The Vitest table shows the `hz` column (the primary rate), the latency min/max/mean/p75/p99/
p995/p999 columns, `rme`, and the sample count. The primary rate is `jobs/sec` for coordinator and
`drain jobs/sec` for contention; the latency columns are the per-run elapsed or drain time in
milliseconds. `BENCH_JSON=bench.json` additionally writes `bench.coordinator.json` and
`bench.contention.json` with the environment, the resolved options, every domain metric, and the
raw per-run samples, which the standard table cannot show.

The process exits with code 1 when any scenario reports a problem; the failing `expect` names the
domain reason (incomplete work, lost lease, duplicate completion, storage error, or an abort).

## coordinator — runtime and poller topology

Compares two ways to wire queues onto one connection:

- `shared` — every queue receives the **same** `Storage` object, so they share one coordinator
  and one poller.
- `isolated` — every queue receives its own delegating wrapper over the same connection, so each
  queue gets its own coordinator and poller. This isolates the effect of sharing without
  reintroducing the previous runtime.

| Profile     | Workload                                                                                                    |
| ----------- | ----------------------------------------------------------------------------------------------------------- |
| `saturated` | Every queue is preloaded before the workers start.                                                          |
| `sparse`    | Only a quarter of the queues has jobs; the rest keep polling empty.                                         |
| `bursty`    | Workers start on empty queues, wait for the poller to go idle, then a burst of `queue.add()` calls arrives. |

Grids: quick uses 1 and 8 queues, full uses 1, 4, 16, and 64. Scenarios are ordered so that the
`shared` and `isolated` runs of one configuration sit next to each other.

Reported: `jobs/sec` (median), `spread (%)` (noise across repeats), `claims/job`,
`empty claims` per run, claim/complete latencies, and `first handler (ms)` — the delay between the
start of the run and the first handler invocation.

A run ends when storage **confirms** the last job, not when its handler returns, so the measured
window covers the final `complete()` round trip. Every confirmed job is also tracked by id, so
lost leases and duplicate completions make the run invalid.

This suite runs on one thread against an in-memory database, so it reports the combined cost of
the runtime and of the storage statements, without disk or cross-thread effects. It cannot show
writer contention: `better-sqlite3` executes synchronously, so one thread never blocks another.
That is what the second suite is for.

## contention — SQLite writer lock across threads

Every worker thread opens its own connection and runs two barrier-synchronized phases:

1. `enqueue` — each worker inserts its share of the jobs.
2. `drain` — workers claim in batches and complete every claimed job.

| Dimension | Quick                                                      | Full         |
| --------- | ---------------------------------------------------------- | ------------ |
| threads   | 1, 4                                                       | 1, 2, 4, 8   |
| batch     | 1, 16                                                      | 1, 4, 16, 64 |
| placement | `shared` (one file) and `per-thread` (one file per thread) | same         |

`threads = 1` with `per-thread` is skipped, because a single thread owns a single file either
way. The total job count is identical in both placements, so `per-thread` is the control
condition: the throughput gap between the placements at the same thread count is the price of
one writer lock.

Reported: `enqueue jobs/sec`, `drain jobs/sec`, `jobs/claim`, `empty claims`, claim and complete
percentiles, `errors`, and the drain duration.

Every `complete()` result is inspected: only `applied` counts as a completion, `lease_lost` is
counted separately and fails the scenario. Completed ids travel back to the parent, which checks
uniqueness across all threads and files, so a job confirmed twice is reported. Enqueue
throughput is based on the number of successful enqueues, not on the requested job count.

The comparison is not only about the writer lock. Separate files also mean separate WAL files,
separate page caches, and more file descriptors, so the measured gap mixes all of those effects.
Tail latency is end-to-end operation duration (including lock waits and thread scheduling), not a
direct measurement of time spent waiting for a lock.

## Methodology

- SQLite settings: `journal_mode = WAL`, `synchronous = NORMAL`, `busy_timeout = 2000`.
- The schema and WAL mode are created in the parent before workers start, so startup never races
  and never lands inside a measurement.
- Every worker thread warms its own JIT with 50 enqueue/claim/complete cycles on a private
  in-memory database before the barrier. Repeats therefore do not measure cold code paths of a
  freshly created thread; what they still cannot warm are OS-level caches and scheduler state.
- Runs alternate direction: even passes walk the scenario list forward, odd passes walk it
  backwards, so `shared` and `isolated` (or `per-thread`) runs do not drift with machine state.
- Warmup passes execute the whole scenario and are only dropped from the results. Failures of a
  warmup pass are printed to stderr, failures of a measured pass fail the scenario.
- Threads report `performance.timeOrigin + performance.now()` timestamps, which share one
  process-wide origin, so phase windows are comparable across threads.
- Rates are medians over the measured runs; percentiles pool the samples of every measured run
  using the nearest-rank method. `BENCH_JSON` also stores the raw per-run values.
- Runs that did not confirm every job, lost a lease, completed a job twice, hit a storage error,
  or aborted are treated as invalid: they are described in the JSON notes, shown as zero rows in
  the table, and **excluded** from every rate and percentile. The `errors` metric deliberately
  counts every run, so an excluded run stays visible.
- A failed run is reported as a scenario note with the first error message, and the remaining
  runs still execute; the process then exits with code 1.
- Every run has a 60 second budget. The timer covers waits on threads and timers; because the
  coordinator suite can run entirely on microtasks, it also checks the budget inside every storage
  operation and aborts that operation, so a long enqueue phase cannot outrun its budget.
- Worker threads are terminated and workers are closed even on failure. When workers do not stop
  within the grace period, the run is invalid and the suite stops instead of measuring in a
  process that still has leftovers of the failed run. Vitest then terminates the benchmark worker
  process, so a leaked handle cannot keep the run alive.
- Databases live in a `walq-bench-*` directory under the system temp directory and are removed
  afterwards.

## Limits

- Runs are short. Treat differences below roughly 10% as noise and raise `BENCH_REPEATS` before
  drawing conclusions.
- The machine must be idle; background load dominates the absolute numbers.
- WAL with `synchronous = NORMAL` matches common production tuning. With `synchronous = FULL`
  every commit would fsync and all absolute numbers would change.
- Benchmarks themselves are excluded from `pnpm test` and never run in CI. The harness unit tests
  (`benchmarks/*.test.ts`) are pure and do run with `pnpm test`.

Use `BENCH_JSON` to keep a baseline and compare it against a later change.
