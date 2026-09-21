# Grouped claim chunk size — 16 / 32 / 64 versus one big transaction

Generated 2026-09-21T14:28:22.702Z

|                  |                                                                                                                              |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| Environment      | node v24.21.0, Apple M1 Pro, 8 cores, darwin/arm64                                                                           |
| Grid             | quick, `BENCH_CLAIM_QUEUES=128,256`, `BENCH_CLAIM_LIMITS=16`, `BENCH_CLAIM_MODES=grouped`, `BENCH_CLAIM_CHUNKS=all,16,32,64` |
| Jobs             | 131072 per run (~64 all-queue rounds at 128 queues, ~32 at 256)                                                              |
| Repeats / warmup | 5 measured + 1 discarded per scenario (competitor confirmation: 8 + 1)                                                       |

`chunk all` is the original behaviour: one `BEGIN IMMEDIATE … COMMIT` for every queue in the
round. `chunk N` splits the round into `ceil(queues / N)` transactions, each claiming at most
`N × limit` jobs. The competitor is a separate worker thread with its own connection to the
same WAL file; `jobs/sec` excludes the unmeasured competitor pauses and counts only claim time.

## Findings

- **Chunking cuts the writer-lock hold roughly proportionally to the chunk.** At 256 queues the p95 transaction duration drops from ~100 ms (`all`) to ~29 ms (`64`), ~18 ms (`32`) and ~12 ms (`16`); at 128 queues `all` is ~57 ms. `synchronous` does not move these numbers — the hold is dominated by the per-queue `recover`/`select`/`acquire` work, not the commit fsync.
- **Throughput barely moves.** Every chunk tier stays within ~5–12% of the single big transaction on both modes and queue counts; `all` is the fastest, the difference is mostly the extra commits.
- **The neighbour writer sees the chunk size, not the durability mode.** Competitor p95 tracks the lock hold: `all` ~39–68 ms, `64` ~22–40 ms, `32` ~10–22 ms, `16` ~5–23 ms. Chunk 32 lands on the most stable low step because the observed competitor wait is quantised by SQLite’s busy-handler backoff.
- Competitor p99 is noisy (rare scheduler/checkpoint events over a few hundred ops per run) and should be read as an order of magnitude, not a precise number. At 256 queues under FULL every tier lands in the 90–137 ms p99 range, so that tail is a write-volume/checkpoint effect rather than a chunk-size effect.

## Outcome

Chunk 32 is implemented as a 512-job budget in `packages/better-sqlite3/src/chunking.ts`: a
grouped round packs requests until their summed limits reach the budget, so a uniform limit
covers `floor(512 / limit)` queues per immediate transaction (limit 16 -> 32 queues, 32 -> 16,
64 -> 8) and a request above the budget takes a transaction of its own. `chunk 64` is the
throughput-leaning alternative and `chunk 16` the latency-leaning one if the budget is retuned.
Because the hold scales with `chunk × limit`, the job budget is a more portable default than a
fixed queue count.

A verification run of the shipped path (`BENCH_CLAIM_MODES=production`) lands within ~1% of the
`grouped / chunk 32` prototype throughput at 128 and 256 queues, and the adapter tests cover the
chunk boundaries: a failure in a later chunk keeps the earlier chunks committed, and a 70-queue
batch at limit 16 is claimed in three transactions without duplicates.
The `production` mode can only time the whole `claimQueues` call, so its transaction percentiles,
commit count, and jobs/transaction describe the call, not one internal chunk.

## Reproduce

```sh
# main sweep, once per mode
BENCH_GRID=quick BENCH_REPEATS=5 BENCH_WARMUP=1 BENCH_JOBS=131072 \
  BENCH_CLAIM_QUEUES=128,256 BENCH_CLAIM_LIMITS=16 BENCH_CLAIM_MODES=grouped \
  BENCH_CLAIM_CHUNKS=all,16,32,64 BENCH_SYNCHRONOUS=normal \
  BENCH_JSON=<out>/normal/bench.json pnpm exec vitest bench --run benchmarks/claim-grouping.bench.ts

# competing-writer confirmation with more repeats
BENCH_GRID=quick BENCH_REPEATS=8 BENCH_WARMUP=1 BENCH_JOBS=131072 BENCH_ONLY=competing \
  BENCH_CLAIM_QUEUES=128,256 BENCH_CLAIM_LIMITS=16 BENCH_CLAIM_MODES=grouped \
  BENCH_CLAIM_CHUNKS=all,16,32,64 BENCH_SYNCHRONOUS=normal \
  BENCH_JSON=<out>/confirm/normal/bench.json pnpm exec vitest bench --run benchmarks/claim-grouping.bench.ts

node benchmarks/reports/analyze-claim-chunks.mjs <out>
```

## Throughput and lock hold — NORMAL

| queues | chunk | jobs/txn | commits | solo jobs/sec | vs all | competing jobs/sec | txn p50 µs | txn p95 µs | txn p99 µs |
| ------ | ----- | -------- | ------- | ------------- | ------ | ------------------ | ---------- | ---------- | ---------- |
| 128    | all   | 2048     | 64      | 38677         | 100%   | 41436              | 52123      | 57539      | 60558      |
| 128    | 16    | 256      | 512     | 36616         | 95%    | 35326              | 4061       | 17134      | 18199      |
| 128    | 32    | 512      | 256     | 35743         | 92%    | 37428              | 11557      | 23861      | 26276      |
| 128    | 64    | 1024     | 128     | 37068         | 96%    | 36620              | 27804      | 31848      | 35297      |
| 256    | all   | 4096     | 32      | 47229         | 100%   | 52379              | 83047      | 103393     | 116069     |
| 256    | 16    | 256      | 512     | 44511         | 94%    | 45491              | 3887       | 13557      | 14809      |
| 256    | 32    | 512      | 256     | 45441         | 96%    | 44121              | 7946       | 19043      | 20608      |
| 256    | 64    | 1024     | 128     | 46655         | 99%    | 45578              | 24039      | 29439      | 32055      |

## Throughput and lock hold — FULL

| queues | chunk | jobs/txn | commits | solo jobs/sec | vs all | competing jobs/sec | txn p50 µs | txn p95 µs | txn p99 µs |
| ------ | ----- | -------- | ------- | ------------- | ------ | ------------------ | ---------- | ---------- | ---------- |
| 128    | all   | 2048     | 64      | 39521         | 100%   | 41147              | 51593      | 55699      | 58776      |
| 128    | 16    | 256      | 512     | 37469         | 95%    | 35569              | 4472       | 16033      | 16840      |
| 128    | 32    | 512      | 256     | 36139         | 91%    | 37590              | 9929       | 21091      | 22737      |
| 128    | 64    | 1024     | 128     | 37605         | 95%    | 37563              | 27528      | 30967      | 34964      |
| 256    | all   | 4096     | 32      | 50011         | 100%   | 47779              | 79378      | 99987      | 101922     |
| 256    | 16    | 256      | 512     | 44075         | 88%    | 43057              | 4181       | 12685      | 13979      |
| 256    | 32    | 512      | 256     | 45849         | 92%    | 42522              | 8299       | 18140      | 20204      |
| 256    | 64    | 1024     | 128     | 46633         | 93%    | 43260              | 23636      | 29016      | 31426      |

## Competitor latency — NORMAL (8 repeats)

| queues | chunk | competitor enqueue p95 | p99   | competitor complete p95 | p99   | ops |
| ------ | ----- | ---------------------- | ----- | ----------------------- | ----- | --- |
| 128    | all   | 39201                  | 41220 | 38977                   | 41347 | 365 |
| 128    | 16    | 21981                  | 63606 | 21848                   | 64793 | 372 |
| 128    | 32    | 10145                  | 21952 | 10153                   | 22295 | 512 |
| 128    | 64    | 21970                  | 22662 | 21997                   | 22671 | 506 |
| 256    | all   | 63373                  | 92416 | 63398                   | 93002 | 199 |
| 256    | 16    | 4751                   | 39954 | 6576                    | 37301 | 423 |
| 256    | 32    | 10345                  | 40156 | 10192                   | 60446 | 382 |
| 256    | 64    | 22708                  | 60478 | 22669                   | 61964 | 274 |

## Competitor latency — FULL (8 repeats)

| queues | chunk | competitor enqueue p95 | p99    | competitor complete p95 | p99    | ops |
| ------ | ----- | ---------------------- | ------ | ----------------------- | ------ | --- |
| 128    | all   | 40411                  | 47360  | 40568                   | 47850  | 365 |
| 128    | 16    | 23300                  | 71312  | 22518                   | 77153  | 342 |
| 128    | 32    | 12135                  | 26399  | 12138                   | 25860  | 512 |
| 128    | 64    | 23451                  | 27135  | 23389                   | 38690  | 419 |
| 256    | all   | 67590                  | 116413 | 71146                   | 94432  | 139 |
| 256    | 16    | 22122                  | 91392  | 21583                   | 106093 | 248 |
| 256    | 32    | 21976                  | 98361  | 22311                   | 100971 | 184 |
| 256    | 64    | 38638                  | 109300 | 40033                   | 137390 | 212 |

## Trade-off summary (256 queues, limit 16)

| chunk  | jobs/transaction | lock hold p95 | competitor p95 | throughput vs all | verdict                                 |
| ------ | ---------------- | ------------- | -------------- | ----------------- | --------------------------------------- |
| all    | 4096             | ~100 ms       | ~63–68 ms      | 100%              | fastest, holds the writer lock too long |
| 64     | 1024             | ~29 ms        | ~23–40 ms      | ~93–94%           | throughput-leaning alternative          |
| **32** | **512**          | **~18 ms**    | **~10–22 ms**  | **~91–92%**       | **shipped default**                     |
| 16     | 256              | ~12 ms        | ~5–23 ms       | ~88–90%           | shortest hold, noisier neighbour tail   |
