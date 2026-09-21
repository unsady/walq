# walq benchmarks

Five suites measure different things. All run through Vitest Bench with the custom provider in
`benchmarks/vitest-provider.ts`. The provider drives the real integration workloads through
`collectRuns()` from `benchmarks/harness.ts`, so every scenario still runs `warmup + repeats`
times with the order alternating between passes, and it returns Tinybench-shaped rows so the
standard Vitest table renders one row per scenario.

```sh
pnpm bench                 # all suites, quick grids
pnpm bench:queue           # coordinator suite only
pnpm bench:contention      # SQLite contention suite only
pnpm bench:claim-grouping  # grouped claim paths only
pnpm bench:complete-batch  # batched completion prototype only
pnpm bench:retention       # history growth and cleanup only
pnpm bench:full            # full matrices
```

The scripts run `vitest bench --run --reporter=verbose`. The verbose reporter is used because the
default reporter switches to the minimal renderer in CI/agent environments, which hides the
benchmark table. `.bench.ts` files are only picked up by `vitest bench`; `pnpm test` still runs
only `*.test.ts`, so the heavy workloads never run there.

## Selecting and tuning

| Setting                 | Default   | Meaning                                                                                                    |
| ----------------------- | --------- | ---------------------------------------------------------------------------------------------------------- |
| `BENCH_GRID`            | `quick`   | `quick` or `full` matrices (`bench:full` sets `full`)                                                      |
| `BENCH_REPEATS`         | 3         | Measured runs per scenario                                                                                 |
| `BENCH_WARMUP`          | 1         | Discarded runs per scenario                                                                                |
| `BENCH_JOBS`            | per suite | Jobs per run (1000 coordinator, 2000 contention, 4096 claim grouping, 4096 complete batch, 1000 retention) |
| `BENCH_ONLY`            |           | Run scenarios whose name contains the text                                                                 |
| `BENCH_JSON`            |           | Base path for the per-suite domain JSON artifacts                                                          |
| `BENCH_SYNCHRONOUS`     | `normal`  | SQLite `synchronous` mode for the file-backed suites (`normal` or `full`)                                  |
| `BENCH_RETENTION_BATCH` |           | Replaces every retention cleanup batch size with one value                                                 |
| `BENCH_CLAIM_QUEUES`    | grid      | Replaces the claim-grouping queue tiers, for example `32,64,128`                                           |
| `BENCH_CLAIM_LIMITS`    | grid      | Replaces the claim-grouping claim limits, for example `16`                                                 |
| `BENCH_CLAIM_MODES`     | grid      | Replaces the claim-grouping modes (`current`/`grouped`/`production`), for example `grouped`                |
| `BENCH_CLAIM_CHUNKS`    | `all`     | Queues per `grouped` prototype transaction; `all` or positive integers, for example `all,16,32,64`         |

The previous `--suite`, `--repeats`, `--warmup`, `--jobs`, `--only`, `--json`, `--full` and
`--help` flags, and the `BENCH_SUITES` variable, are gone because `vitest bench` owns the CLI.
Select a suite with the `pnpm bench:*` scripts or a Vitest file filter, for example
`vitest bench --run benchmarks/coordinator.bench.ts`, and pass the remaining settings through the
`BENCH_*` variables. Unrelated flags are forwarded to Vitest unchanged.

The Vitest table shows the `hz` column (the primary rate), the latency min/max/mean/p75/p99/
p995/p999 columns, `rme`, and the sample count. The primary rate is `jobs/sec` for coordinator,
`drain jobs/sec` for contention, and `jobs/sec` for claim grouping and retention; the latency
columns are the per-run elapsed, drain, or workload time in milliseconds. `BENCH_JSON=bench.json`
additionally writes one artifact per suite, such as `bench.coordinator.json`,
`bench.contention.json`, `bench.claim-grouping.json`, and `bench.retention.json`, with the
environment, resolved options, every domain metric, and raw per-run samples that the standard
table cannot show.

The process exits with code 1 when any scenario reports a problem; the failing `expect` names the
domain reason (incomplete work, lost lease, duplicate completion, storage error, or an abort).

## Reports

`benchmarks/reports/` keeps the tuning evidence behind shipped defaults. Each report is generated
from `BENCH_JSON` artifacts, so a run can be re-rendered or re-checked against current code:

- [grouped-claim-chunk-size.md](reports/grouped-claim-chunk-size.md) — why grouped claims use a
  512-job chunk budget (`floor(512 / limit)` queues at a uniform limit) and what 16/32/64 cost in
  lock hold, throughput, and competitor latency.

Run `node benchmarks/reports/analyze-claim-chunks.mjs <artifactsDir>` after the commands listed in
the report to reproduce it; the script writes a formatter-clean markdown file.

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
`empty claims` per run, claim/complete latencies, `grouped calls` and `requests/grouped call`, and
`first handler (ms)` — the delay between the start of the run and the first handler invocation.
The storage wrapper forwards the optional `claimQueues` capability, so the shared coordinator
coalesces same-sweep worker claims into one grouped call (roughly one per sweep) while isolated
coordinators issue one request per call. `claim p50/p95` is the latency of one `claimQueues` call,
so it covers a whole grouped batch in shared mode and a single request in isolated mode.

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

## claim-grouping — multi-queue transaction prototype

Compares the grouped claim path with a benchmark-only prototype that predates the production
`Storage.claimQueues` capability:

- `current` calls `claim()` separately for every queue, creating one immediate transaction per
  call.
- `grouped` runs the same recover/select/acquire sequence for every queue inside immediate
  transactions sized by the `BENCH_CLAIM_CHUNKS` tier. It is a prototype: it keeps its own prepared
  statements so chunk sizes can be swept without touching adapter code.
- `production` calls the shipped `Storage.claimQueues` once per round, so the adapter's own 512-job
  chunk budget is what gets measured. The benchmark can only time the whole call, so its
  transaction percentiles, commit count, and jobs/transaction describe the call rather than one
  internal chunk; see [grouped-claim-chunk-size.md](reports/grouped-claim-chunk-size.md) for the
  chunk tiers behind that budget.

The coordinator benchmark exercises the same production method end to end. This suite keeps its own
prepared statements so the grouped and current paths can be compared under one grid; only the
`production` mode imports adapter behaviour.

The quick grid uses 1, 8, and 32 queues with limits 1 and 16. The full grid covers 1, 4, 8, and 32
queues with limits 1, 4, and 16. Both compare `solo` against `competing`. A competing scenario
opens a second connection in a worker thread against the same WAL file and repeatedly performs
measured enqueue and complete writes against pre-created leases, alternating their order and
yielding for one millisecond between cycles to avoid artificial writer starvation.

Reported: claimed `jobs/sec`; individual transaction p50/p95/p99; average jobs per transaction;
the commit count; event-loop p95/p99; and the competing connection's enqueue and complete
p95/p99. Transaction latency intentionally has a different unit of work: one queue claim for
`current`, one grouped transaction for `grouped` (all queues by default, or a `chunk` of queues
when the chunk tiers are configured), and one whole-round `claimQueues` call for `production`.
Before each round the suite schedules a `setImmediate`; the time until that
callback runs is the event-loop stall sample. It includes the whole claim round and scheduler
latency, so use it as a responsiveness comparison rather than exact CPU time. Competing runs add
short, unmeasured pauses between selected rounds so the second connection produces enough latency
samples instead of being starved for the entire run; `jobs/sec` uses only summed claim-round time.

Jobs are distributed evenly across queues. Every claimed id is checked for uniqueness and a run
is invalid when work is incomplete, duplicated, or the competing writer reports an error.

`BENCH_CLAIM_QUEUES`, `BENCH_CLAIM_LIMITS`, `BENCH_CLAIM_MODES` and `BENCH_CLAIM_CHUNKS`
replace the queue, limit, mode and chunk tiers so a longer `BEGIN IMMEDIATE` can be probed without
editing the grid. `chunk all` keeps the original behaviour of one transaction per round; smaller
chunks split the round into several transactions:

```sh
BENCH_CLAIM_QUEUES=128,256 BENCH_CLAIM_LIMITS=16 BENCH_CLAIM_MODES=grouped \
  BENCH_CLAIM_CHUNKS=all,16,32,64 pnpm bench:claim-grouping
```

## complete-batch — batched completion prototype

Measures the headroom of completing several jobs inside one transaction instead of one autocommit
`UPDATE` per job. Each run seeds live leases with benchmark-only SQL inside a single transaction,
then measures only the completion phase. `complete batch 1` calls the production
`Storage.complete` path, so every other tier is compared against the shipped code rather than a
rewritten baseline; the larger tiers use a benchmark-only prototype that runs the same
`UPDATE ... WHERE status = 'active' AND leaseToken = ... AND expiresAt > now` for the batch inside
one immediate transaction.

Both grids use batch sizes 1, 4, and 16. Reported: `complete jobs/sec`; the commit p50/p95/p99;
the commit count; and the mean per-job cost. Seeding and lease creation stay outside the measured
phase, so the numbers isolate the completion commit. As with the claim prototype, the tier is a
trade-off: a larger batch holds the writer lock longer, which matters more under
`synchronous = FULL`.

## retention — completed/failed history growth and cleanup

Measures how retained terminal history changes active-workload performance and whether batched
cleanup restores it, before considering database sharding. The public `Storage` API is unchanged:
history is inserted with benchmark-only prepared SQL inside a single transaction, never through
`enqueue` or `complete` calls, so seeding stays outside every measured phase.

Each run seeds a fixed number of `completed` and `failed` rows (half and half), optionally cleans
them up, then runs the same fixed active workload on the real adapter: enqueue every active job,
claim in batches of 20, and complete every claimed job.

| Dimension  | Quick                                 | Full                                                    |
| ---------- | ------------------------------------- | ------------------------------------------------------- |
| history    | 0, 1k, 25k                            | 0, 1k, 25k, 250k, 1M                                    |
| cleanup    | `retained`, `delete`, `delete-vacuum` | same                                                    |
| batch      | 10k                                   | 10k; 50k compared at 250k                               |
| connection | `warm`                                | `warm`; `reopened` at 25k/250k and after vacuum at 250k |

- `retained` — no cleanup; the active workload runs against the full history.
- `delete` — terminal rows are removed in batches with `DELETE ... RETURNING`; no VACUUM.
- `delete-vacuum` — batched deletion, then `VACUUM`, then `wal_checkpoint(TRUNCATE)`.

`BENCH_RETENTION_BATCH` replaces every tier's batch list with one value, for example
`BENCH_RETENTION_BATCH=25000 pnpm bench:retention`.

Reported: active `jobs/sec`; enqueue/claim/complete p50/p95/p99; event-loop p95/p99; cleanup
duration; per-batch execution p50/p95/p99 and event-loop stall p95/p99; `vacuum` and `checkpoint`
duration with the VACUUM stall; and database, WAL, and page counts before and after cleanup. Every
claimed id must be unique, every claimed job must complete with `applied`, and an unmeasured probe
checks `heartbeat`, `fail` with retry, terminal `fail`, and stale-lease rejection before the
measured phase. A storage error, lost lease, duplicate, incomplete job, or failed probe
invalidates the run.

`VACUUM` rewrites the database but does not shrink the on-disk file until a WAL checkpoint runs,
so `delete-vacuum` measures `wal_checkpoint(TRUNCATE)` separately. Without `VACUUM`, `DELETE` does
not shrink the file either: it moves pages to the freelist, which later inserts can reuse.
`pages before`/`pages after` and the `db`/`wal` sizes show that directly — `delete` leaves the page
count near its pre-cleanup value while `delete-vacuum` drops it.

Cleanup runs through `better-sqlite3`, so every batch is synchronous. The per-batch execution time
is the event-loop blocking time; a `setImmediate` is scheduled before each batch and before
`VACUUM`, so the same stall is also measured end to end. A larger batch blocks the event loop
longer, so the batch size is a latency/throughput trade-off, not a free speedup.

The `reopened` connection dimension closes and reopens the database after seeding and cleanup,
then warms prepared statements on a private queue, so it reports first measured access with a cold
page cache. `warm` reuses the seeding connection.

## Methodology

- SQLite settings: `journal_mode = WAL`, `synchronous = NORMAL`, `busy_timeout = 2000`.
  `BENCH_SYNCHRONOUS=full` switches every file-backed connection to `synchronous = FULL`, so the
  durability trade-off can be measured with the same scenarios. The coordinator suite runs on an
  in-memory database, where the setting has no effect and stays a control condition.
- A run records its effective `synchronous` mode in the JSON artifact metadata.
- The schema and WAL mode are created in the parent before workers start, so startup never races
  and never lands inside a measurement.
- Every contention-suite worker thread warms its own JIT with 50 enqueue/claim/complete cycles on
  a private in-memory database before the barrier. Repeats therefore do not measure cold code
  paths of a freshly created thread; what they still cannot warm are OS-level caches and scheduler
  state.
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
- Databases live in a `walq-*` directory under the system temp directory and are removed
  afterwards.

## Limits

- Runs are short. Treat differences below roughly 10% as noise and raise `BENCH_REPEATS` before
  drawing conclusions.
- The machine must be idle; background load dominates the absolute numbers.
- WAL with `synchronous = NORMAL` matches common production tuning. With `synchronous = FULL`
  every commit would fsync and all absolute numbers would change; run both modes with
  `BENCH_SYNCHRONOUS` to quantify the difference instead of guessing.
- Benchmarks themselves are excluded from `pnpm test` and never run in CI. The harness unit tests
  (`benchmarks/*.test.ts`) are pure and do run with `pnpm test`.

Use `BENCH_JSON` to keep a baseline and compare it against a later change.
