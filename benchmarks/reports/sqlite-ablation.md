# SQLite performance ablation for an article

## Scope and methodology

Measured locally on Apple M1 Pro (8 cores), macOS arm64, Node **24.21.0**,
better-sqlite3 **13.0.3**, SQLite **3.53.4**. Source base:
`2e989c65c962cd37ef5fdc8cec505e478ebb1be4`, plus this benchmark patch.
Every variant has **one successful discarded warmup and seven measured repeats**.
The existing harness alternates forward/reverse scenario order on each pass.
Suites run sequentially on the same host; these are not CPU-pinned or
power-management-controlled laboratory measurements.

Each run opens a fresh on-disk database in the OS temporary directory.
Actual PRAGMAs are saved in raw JSON: 4096-byte pages, cache_size -2000,
busy_timeout 2000, WAL auto-checkpoint 1000 pages, and the selected
journal/synchronous modes. Checkpoints during the drain are not excluded.
Connection/schema setup, seeding, competing-writer setup, verification and
teardown are outside the measured span. Warmup outcomes are discarded by the
shared harness; all measured operation timings are retained.

Fixed primary payload: **302 serialized bytes**,
`{event: "delivery", customerId: 42, body: "x".repeat(256)}`.
Jobs are ungrouped, immediately ready, priority 0, one attempt; no producers
change the primary queues during a run. Direct SQL seeding is setup only.
Retention is disabled equally for all process variants; completions still use
the production lease-checked, single-job `complete()` implementation.

### The five stages

1. DELETE, per-queue single-job claim transactions.
2. WAL, the identical single-job claim policy.
3. WAL, one production per-queue claim for all available slots.
4. WAL, production coordinator coalescing across queues, one unbounded
   grouped claim transaction.
5. WAL, production coordinator and production 512-job chunking.

Both NORMAL and FULL are measured for **all five stages**, including DELETE,
so WAL/DELETE never needs an unlike-durability comparison.

All process variants call real `Queue.process()`, share one storage/coordinator,
and retain its scheduling, schedule-materialization checks, parsing, heartbeat
lifecycle, handler execution and completion. Thus stage 3→4 isolates **grouped
storage claims**, not replacing independent historical pollers with the current
coordinator. No old scheduler is reimplemented.

Two explicitly benchmark-only seams are necessary:

- The public worker always requests all free slots. Stages 1–2 fill that same
  capacity with sequential production `claim(limit=1)` calls, then return the
  batch to the real worker. This preserves concurrency 16 instead of secretly
  turning it into concurrency 1. It adds promise/instrumentation overhead and
  is **not an exact reconstruction of an old Walq release**. Per-request
  transaction atomicity differs intentionally; equivalence here is restricted
  to preseeded ungrouped jobs without primary producers, failures or retries.
- Stage 4 calls the shipped adapter's `prepareClaim` and
  `claimTransaction.immediate` internals instead of its chunked public method.
  Validation, recovery, selection, lease acquisition and SQL are production;
  only chunk partitioning/yields are bypassed. This is a benchmark-only path,
  deliberately guarded against internal implementation changes. It does not
  establish multi-chunk rollback/failure equivalence.

The suite instantiates the shipped shared SQLite storage with an instrumented
connection equivalent to the public better-sqlite3 adapter: the same driver,
`safeIntegers(false)`, statements and `db.transaction().immediate()`.
Transaction timing wraps the driver call through its successful commit, so it
includes lock acquisition, SQL and commit, **not just SQL execution or lock hold**.
No package API or production implementation is changed.

### Workloads

**A — process:** 32 queues, **8192 total jobs**, concurrency **1 or 16 per queue**
(total capacity 32 or 512, not a pool-wide concurrency limit). The minimal
async handler awaits a shared `setImmediate` turn. All handlers awaiting that
turn resume together, allowing full claims while yielding to the event loop.
This is a controlled asynchronous drain/capacity workload, **not a measurement
of a real application's network/CPU handler cost**. There is no competing writer
in A, so its writer latency metrics are `N/A`, not zero.

With 32 × 16 capacity the whole sweep fits the 512 budget. Stages 4 and 5 should
therefore be effectively identical in A; no fictitious chunking benefit is claimed.

**B — stress:** 256 queues, limit **16 per queue**, **131072 total jobs**.
Each round submits 256 requests (4096 jobs), with a common `setImmediate` yield
after the round in both variants. This deliberately uses the existing
claim-grouping storage-round shape, **not `Queue.process()`**: primary jobs are
claimed once and left active. It isolates claim transaction shape without
mixing in 131072 completion writes. The no-chunk variant uses one transaction
per round; production uses eight transactions of 512 jobs. There is no
single-request limit above 512, which production cannot split.

The competing writer runs in another worker/thread and SQLite connection on the
same file, using **production `enqueue()` and `complete()`**. Completion leases
are preseeded outside timing. Each cycle does one enqueue and one completion,
alternating their order, then waits 1 ms **after completion of the cycle**.
This is a closed-loop probe, not a fixed-rate producer. The number of successful
probe cycles naturally changes with contention; it is not primary throughput.
Each run must produce ≥20 samples per operation, no errors, and must not exhaust
the 16384 prepared leases. Samples are taken only while the primary drain is
active; the run is not padded with idle writes to improve percentiles.
The legacy claim-grouping suite retains its default raw-SQL probe; these
production-writer results must not be relabeled as that older probe's numbers.

**Diagnostic:** a separate seven-repeat stage-5 NORMAL/concurrency-16 run
uses independent `setImmediate` callbacks instead of a shared turn. This is
explicitly a **different handler scheduling shape**, not a row in the ablation.

### Metric definitions and limitations

- Jobs/s = actual completed jobs (A) or claimed jobs (B), divided by the
  **sum of measured wall-clock spans** across valid repeats. Setup and teardown
  are excluded, but worker scheduling, event-loop yields, checkpoints and the
  final drain continuation are included. A and B rates are different operations.
- Elapsed is the median per-run span, not the reciprocal of aggregate throughput.
- Latencies are nearest-rank p50/p95 per run, then the median of those values
  across repeats. The raw values are milliseconds; the existing harness reports
  microseconds; tables below convert to milliseconds.
- Claim latency is **storage API invocation-to-promise-settlement**. In per-queue
  modes it can include the microtask backlog from other queues; it is not
  transaction latency. A grouped call can span multiple transactions and yields.
- Event-loop samples are successive `setImmediate` turns from the existing
  `measureClaimRound` probe. They measure responsiveness under this scheduling
  policy, not idle timer jitter. A/concurrency-16 has only **17–18 turns per run**
  and 18 grouped calls: its p95 is a coarse tail estimate, not a reliable SLO.
  Stress has 33 turns/32 transactions per unbounded run versus
  257 turns/256 transactions per chunked run. Sampling opportunities differ
  because yielding is the optimization being tested.
- Writer latency covers the entire production storage call, not SQLite lock
  wait alone. Writer p95 is sensitive to busy-handler scheduling and sample size.
- “Claim API calls” counts both individual and grouped calls; “grouped calls”
  is the subset. Counter tables give median counts/ratios per measured run.
- Counter collection stays active through the final event-loop probe turn,
  so empty polls during that turn can appear in raw counters even after the
  last completion settles. Counters are observed, not inferred from throughput.
- Explicit transactions are **observed successful driver transactions/commits**,
  including empty claims and schedule checks. Completion autocommits are separate
  successful single-statement writes (8192 in A, zero for primary work in B).
  Primary total transactions/commits = explicit transactions + completion
  autocommits. Competing-writer commits are not included in primary counters.
  Jobs/claim transaction includes empty claim transactions; it is not the count
  of all row mutations or total lifecycle jobs per commit.
- Spread = 100 × (max per-run jobs/s − min per-run jobs/s) / median per-run jobs/s.
  It is a range statistic, **not a confidence interval**. Raw min/max, sample
  counts and every repeat are available. Some small differences fall within it.
- These instrumented results do not establish power-loss durability guarantees,
  handler-heavy application throughput, multi-process scaling, dedupe/group
  scheduling performance, or steady-state producer/retention behavior.

## A. Process ablation: all modes

| Stage | Sync   | Concurrency / queue | Jobs/s | Elapsed s | Claim p50 / p95 ms | Transaction p50 / p95 ms | Event-loop p95 ms | Spread % |
| ----- | ------ | ------------------- | ------ | --------- | ------------------ | ------------------------ | ----------------- | -------- |
| 1     | NORMAL | 1                   | 1371   | 5.941     | 5.830 / 10.999     | 0.346 / 0.426            | 25.47             | 3.82     |
| 1     | FULL   | 1                   | 1273   | 6.364     | 6.233 / 11.771     | 0.371 / 0.451            | 27.07             | 4.61     |
| 2     | NORMAL | 1                   | 8773   | 0.927     | 0.812 / 1.618      | 0.046 / 0.055            | 6.64              | 4.18     |
| 2     | FULL   | 1                   | 6044   | 1.344     | 1.295 / 2.551      | 0.074 / 0.089            | 6.94              | 4.61     |
| 3     | NORMAL | 1                   | 8781   | 0.932     | 0.810 / 1.622      | 0.046 / 0.055            | 6.62              | 1.72     |
| 3     | FULL   | 1                   | 6023   | 1.353     | 1.301 / 2.577      | 0.075 / 0.091            | 7.10              | 3.90     |
| 4     | NORMAL | 1                   | 10333  | 0.785     | 1.067 / 3.710      | 1.065 / 3.707            | 6.02              | 6.66     |
| 4     | FULL   | 1                   | 8151   | 0.998     | 1.134 / 2.693      | 1.132 / 2.691            | 5.61              | 8.20     |
| 5     | NORMAL | 1                   | 10320  | 0.784     | 1.067 / 3.674      | 1.062 / 3.670            | 6.13              | 7.97     |
| 5     | FULL   | 1                   | 8150   | 1.002     | 1.138 / 2.698      | 1.134 / 2.694            | 5.82              | 4.55     |
| 1     | NORMAL | 16                  | 1293   | 6.318     | 6.405 / 12.226     | 0.379 / 0.486            | 416.50            | 4.16     |
| 1     | FULL   | 16                  | 1216   | 6.738     | 6.838 / 13.062     | 0.404 / 0.514            | 455.34            | 6.18     |
| 2     | NORMAL | 16                  | 9535   | 0.857     | 0.856 / 3.126      | 0.047 / 0.073            | 57.38             | 2.33     |
| 2     | FULL   | 16                  | 6261   | 1.304     | 1.375 / 2.872      | 0.077 / 0.108            | 87.46             | 10.77    |
| 3     | NORMAL | 16                  | 16795  | 0.487     | 4.085 / 8.965      | 0.234 / 0.277            | 34.59             | 1.29     |
| 3     | FULL   | 16                  | 11365  | 0.719     | 4.644 / 9.129      | 0.274 / 0.324            | 49.00             | 5.26     |
| 4     | NORMAL | 16                  | 17560  | 0.463     | 6.322 / 9.551      | 6.315 / 9.543            | 33.12             | 5.28     |
| 4     | FULL   | 16                  | 12028  | 0.674     | 6.468 / 8.640      | 6.462 / 8.633            | 45.62             | 5.30     |
| 5     | NORMAL | 16                  | 17548  | 0.466     | 6.307 / 9.729      | 6.295 / 9.720            | 33.05             | 4.06     |
| 5     | FULL   | 16                  | 12031  | 0.675     | 6.417 / 8.832      | 6.403 / 8.821            | 46.67             | 5.58     |

Writer enqueue/complete p50/p95 are N/A for every row in A (no competing writer).

### Actual transaction shape and commits

| Stage | Sync   | Concurrency / queue | Claim API calls | Grouped calls | Requests / grouped call | Claim transactions | All explicit transactions | Completion autocommits | Jobs / claim transaction |
| ----- | ------ | ------------------- | --------------- | ------------- | ----------------------- | ------------------ | ------------------------- | ---------------------- | ------------------------ |
| 1     | NORMAL | 1                   | 8224            | 0             | 0.00                    | 8224               | 16448                     | 8192                   | 1.00                     |
| 1     | FULL   | 1                   | 8224            | 0             | 0.00                    | 8224               | 16448                     | 8192                   | 1.00                     |
| 2     | NORMAL | 1                   | 8224            | 0             | 0.00                    | 8224               | 16448                     | 8192                   | 1.00                     |
| 2     | FULL   | 1                   | 8224            | 0             | 0.00                    | 8224               | 16448                     | 8192                   | 1.00                     |
| 3     | NORMAL | 1                   | 8224            | 0             | 0.00                    | 8224               | 16448                     | 8192                   | 1.00                     |
| 3     | FULL   | 1                   | 8224            | 0             | 0.00                    | 8224               | 16448                     | 8192                   | 1.00                     |
| 4     | NORMAL | 1                   | 258             | 258           | 31.88                   | 258                | 8482                      | 8192                   | 31.75                    |
| 4     | FULL   | 1                   | 258             | 258           | 31.88                   | 258                | 8482                      | 8192                   | 31.75                    |
| 5     | NORMAL | 1                   | 258             | 258           | 31.88                   | 258                | 8482                      | 8192                   | 31.75                    |
| 5     | FULL   | 1                   | 258             | 258           | 31.88                   | 258                | 8482                      | 8192                   | 31.75                    |
| 1     | NORMAL | 16                  | 8224            | 0             | 0.00                    | 8224               | 8768                      | 8192                   | 1.00                     |
| 1     | FULL   | 16                  | 8224            | 0             | 0.00                    | 8224               | 8768                      | 8192                   | 1.00                     |
| 2     | NORMAL | 16                  | 8224            | 0             | 0.00                    | 8224               | 8768                      | 8192                   | 1.00                     |
| 2     | FULL   | 16                  | 8224            | 0             | 0.00                    | 8224               | 8768                      | 8192                   | 1.00                     |
| 3     | NORMAL | 16                  | 544             | 0             | 0.00                    | 544                | 1088                      | 8192                   | 15.06                    |
| 3     | FULL   | 16                  | 544             | 0             | 0.00                    | 544                | 1088                      | 8192                   | 15.06                    |
| 4     | NORMAL | 16                  | 18              | 18            | 30.22                   | 18                 | 562                       | 8192                   | 455.11                   |
| 4     | FULL   | 16                  | 18              | 18            | 30.22                   | 18                 | 562                       | 8192                   | 455.11                   |
| 5     | NORMAL | 16                  | 18              | 18            | 30.22                   | 18                 | 562                       | 8192                   | 455.11                   |
| 5     | FULL   | 16                  | 18              | 18            | 30.22                   | 18                 | 562                       | 8192                   | 455.11                   |

For example, stage 5/concurrency 16 performs 562 explicit transactions and 8192
completion autocommits: **8754 total primary commits**, of which only 18 are
claims. At concurrency 1 the corresponding total is **16674**. This is why
grouping claim commits cannot remove the cost of individual durable completions.

## B. Chunking stress, with production competing writer

| Stage / sync | Claimed jobs/s | Elapsed s | Claim API p50 / p95 ms | Transaction p50 / p95 ms | Event-loop p95 ms | Writer enqueue p50 / p95 ms | Writer complete p50 / p95 ms | Spread % |
| ------------ | -------------- | --------- | ---------------------- | ------------------------ | ----------------- | --------------------------- | ---------------------------- | -------- |
| 4 / NORMAL   | 46854          | 2.812     | 86.450 / 97.056        | 86.430 / 97.034          | 99.98             | 0.077 / 64.513              | 0.058 / 63.942               | 4.34     |
| 4 / FULL     | 45927          | 2.833     | 87.584 / 98.028        | 87.559 / 98.006          | 102.15            | 0.117 / 87.260              | 0.093 / 85.697               | 8.45     |
| 5 / NORMAL   | 43548          | 3.014     | 92.500 / 106.357       | 9.070 / 21.966           | 22.40             | 0.084 / 10.241              | 0.065 / 10.654               | 2.18     |
| 5 / FULL     | 43993          | 2.976     | 90.864 / 106.086       | 8.819 / 21.346           | 21.58             | 0.112 / 36.911              | 0.092 / 36.708               | 2.43     |

| Stage / sync | Grouped API calls | Requests / call | Claim transactions / commits | Jobs / transaction | Claim / transaction samples (7 runs) | Event-loop samples (7 runs) | Writer samples (each operation, 7 runs) | Writer operations / run           |
| ------------ | ----------------- | --------------- | ---------------------------- | ------------------ | ------------------------------------ | --------------------------- | --------------------------------------- | --------------------------------- |
| 4 / NORMAL   | 32                | 256             | 32                           | 4096               | 224 / 224                            | 231                         | 1061                                    | 136, 185, 137, 138, 161, 157, 147 |
| 4 / FULL     | 32                | 256             | 32                           | 4096               | 224 / 224                            | 231                         | 837                                     | 119, 145, 131, 110, 128, 108, 96  |
| 5 / NORMAL   | 32                | 256             | 256                          | 512                | 224 / 1792                           | 1799                        | 1956                                    | 279, 323, 288, 314, 208, 280, 264 |
| 5 / FULL     | 32                | 256             | 256                          | 512                | 224 / 1792                           | 1799                        | 1753                                    | 140, 276, 257, 274, 270, 266, 270 |

No primary completions, schedule checks or empty tail calls are included in B.
Claim API calls are all grouped: 32 calls per run in every row. Each processes
the same 131072 primary jobs. The internal transaction count changes intentionally.

## Answers and publication guidance

1. **WAL vs DELETE at matched durability:** NORMAL is **6.40×** faster at
   concurrency 1 and **7.37×** at 16; FULL is **4.75×** and **5.15×** respectively.
   Publish as a result of this specified asynchronous queue drain on this host,
   with the single-job benchmark wrapper disclosed, not “SQLite WAL is 7× faster”
   or an old/new Walq release comparison.
2. **Single-queue batching:** at concurrency 16, stage 2→3 improves throughput
   **76.2% NORMAL / 81.5% FULL** and reduces claim transactions from 8224 to 544.
   At concurrency 1 there is no batching opportunity; the difference is noise.
   Larger per-queue transactions have higher individual transaction latency:
   NORMAL transaction p95 goes from 0.073 to 0.277 ms.
3. **Grouping across queues:** at concurrency 16, stage 3→4 adds
   **4.6% NORMAL / 5.8% FULL**, reducing claim transactions from 544 to 18.
   Those throughput deltas are modest relative to some repeat spreads; publish
   them as observations, not statistically established universal gains.
   At concurrency 1, gains are **17.7% / 35.3%**, with 8224→258 claim transactions.
   Neither comparison isolates the historical independent-poller architecture.
4. **Unbounded grouped transactions:** stress transaction p95 is roughly
   **97–98 ms**, event-loop p95 **100–102 ms**, writer p95 **64–87 ms**.
   One call requests 4096 jobs. These are blocking/operation spans, not pure
   writer-lock hold times.
5. **Production budget 512:** stress transaction p95 falls to **21–22 ms**
   (about **4.4–4.6×** shorter), event-loop p95 to **22 ms** (about **4.5–4.7×**).
   Writer enqueue/complete p95 drops to **10–11 ms NORMAL**, **37 ms FULL**.
   Throughput falls **7.1% NORMAL / 4.2% FULL**, with 32→256 claim commits.
   The overall grouped API p95 actually increases to **106 ms**: splitting does
   not make a 4096-job call finish faster, it lets other work proceed between
   commits. Avoid promising that every competing writer gets a 22-ms bound.
6. **FULL vs NORMAL:** process stage 3 costs roughly **31–32%** throughput
   under FULL; stage 5 costs **21.0% at concurrency 1 / 31.4% at 16**. Single-job
   completions still pay durability cost. Stress (claims only) shows about a
   **2% FULL decrease without chunking**, and a **1% apparent increase with
   chunking**; these small differences are within run variability and do not
   establish “FULL is free” or “FULL is faster”.
7. **Old figures / transaction-shape sensitivity:** the historical
   [512-job decision](grouped-claim-chunk-size.md) used retired raw SQL prototypes,
   a different payload/probe and five repeats. Its approximate ~100-ms
   unbounded / ~18-ms chunk transaction p95 and ~91–92% retained throughput
   agree directionally with this production-path stress run (~97–98 / ~21–22 ms,
   **92.9–95.8%** retained throughput), but are **not an exact reproduction**.
   Do not mix the historical transaction p95 with a maintained
   `claimQueues()` API p95, or storage claim throughput with completed jobs/s.
   The retired runners were not executed here, so no claim that their precise
   numbers are proven reproducible or disproven is justified.

### Diagnostic: handlers can destroy the batching opportunity

Same stage 5, WAL/NORMAL, 32 queues, concurrency 16, 8192 jobs and 302-byte payload;
only the handler's yield policy changes:

| Handler policy   | Completed jobs/s | Grouped calls / run | Requests / grouped call | Jobs / claim transaction | Event-loop p95 ms | Spread % |
| ---------------- | ---------------- | ------------------- | ----------------------- | ------------------------ | ----------------- | -------- |
| shared-turn      | 17548            | 18                  | 30.222                  | 455.111                  | 33.05             | 4.06     |
| independent-turn | 8187             | 8193                | 1.004                   | 1.000                    | 67.70             | 4.52     |

Independent callbacks let slots finish one at a time. Most later sweeps then
contain one claim of one job despite configured concurrency 16. The shared-turn
run reaches **17548 jobs/s**, the independent-turn run **8187 jobs/s**; the latter
has almost no cross-queue grouping. This is a concrete non-transferability
example, **not a controlled optimization delta**. Do not quietly substitute
a storage-only capacity benchmark or a favorable completion schedule for a
real `Queue.process()` application.

Safe public headlines: the matched-durability WAL result for the declared
workload, the batching opportunity at full slots, and the stress
throughput/responsiveness trade-off with 512. Keep whole-machine/environment
details, raw results and the benchmark-only seams attached.
Unsafe headlines: exact universal tail improvements, application SLOs from
these sparse percentiles, a universal coordinator multiplier, pure lock-wait
claims, or treating NORMAL/FULL as equally durable.

## Reproduce from the repository root

```sh
pnpm bench:build
# Force emission when iterating on suites; build/setup are never timed.
pnpm exec tsc -b benchmarks/tsconfig.build.json --force

BENCH_ABLATION_WORKLOAD=process BENCH_ABLATION_HANDLER=shared-turn \
BENCH_JOBS=8192 BENCH_REPEATS=7 BENCH_WARMUP=1 \
BENCH_JSON=.cache/benchmarks/article-process.json \
node .cache/benchmarks/cli.js ablation

BENCH_ABLATION_WORKLOAD=stress \
BENCH_JOBS=131072 BENCH_REPEATS=7 BENCH_WARMUP=1 \
BENCH_JSON=.cache/benchmarks/article-stress-production-writer.json \
node .cache/benchmarks/cli.js ablation

BENCH_ABLATION_WORKLOAD=process BENCH_ABLATION_HANDLER=independent-turn \
BENCH_JOBS=8192 BENCH_REPEATS=7 BENCH_WARMUP=1 \
BENCH_ONLY='process / 5 / WAL / normal / concurrency 16' \
BENCH_JSON=.cache/benchmarks/article-independent.json \
node .cache/benchmarks/cli.js ablation
```

The commands above use explicit per-scenario synchronous settings;
`BENCH_SYNCHRONOUS` does not override this suite's NORMAL/FULL pairs.
The opt-in suite does not alter the default benchmark grid.

Raw JSON and compressed copies are saved locally outside tracked source:

- `.cache/benchmarks/article-process.ablation.json[.gz]`
- `.cache/benchmarks/article-stress-production-writer.ablation.json[.gz]`
- `.cache/benchmarks/article-independent.ablation.json[.gz]`

SHA-256 of the uncompressed JSON, in that order:

```text
fa2ab464adf68379f38f3dec8f4b0fef425448005b7307b1e333a64eec182f27
07e99cc68939b6ecd535984332e23acbc7e24263818791e245dd8e261695e540
8617889c3deaaa5fce7996693b63aa61ee6ea5ad2a7e3d7bbd33ba021718e2c4
```

Each result contains `raw` outcomes, not just aggregate percentiles:
call timings/jobs/request counts, successful transaction timings/jobs/kind,
every event-loop interval, every competing-writer call, PRAGMAs, driver and
SQLite versions. In these saved artifacts the top-level synchronous option
records the harness default; the effective durability is the explicit per-result
parameter/PRAGMA, not that default. Subsequent runs label this option “per scenario”.
Execution logs sit beside the JSON. Initial smoke runs and
the preliminary raw-SQL-writer stress run are **not article results**.

Implementation:
[ablation suite](../ablation.suite.ts),
[correctness tests](../ablation.test.ts),
[shared claim-grouping helpers](../claim-grouping.ts),
[competing writer](../fixtures/claim-competitor-worker.ts).
The shared CLI, collector, event-loop probe and reporter are reused.
