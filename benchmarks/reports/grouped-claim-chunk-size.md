# Why grouped claims use a 512-job chunk budget

Measured on an Apple M1 Pro (8 cores, Node 24), with 128/256 queues, claim limit 16, 131072 jobs per run, 5 measured repeats and 1 warmup. A second connection wrote to the same WAL file in the competing scenarios. Results below are approximate: short-run tails are noisy.

| Queues per transaction | Jobs per transaction | Transaction p95 at 256 queues | Competing writer p95 | Throughput vs one big transaction |
| ---------------------- | -------------------: | ----------------------------: | -------------------: | --------------------------------: |
| All (256)              |                 4096 |                       ~100 ms |            ~63–68 ms |                              100% |
| 64                     |                 1024 |                        ~29 ms |            ~23–40 ms |                           ~93–94% |
| **32**                 |              **512** |                    **~18 ms** |        **~10–22 ms** |                       **~91–92%** |
| 16                     |                  256 |                        ~12 ms |             ~5–23 ms |                           ~88–90% |

**Decision:** split grouped claims at a budget of 512 jobs (`packages/better-sqlite3/src/chunking.ts`). At limit 16 this corresponds to 32 queues per transaction. It substantially shortens writer-lock holds without a large throughput loss; a job budget also adapts to other claim limits. The production path was separately checked against the 32-queue prototype. Adapter tests cover chunk boundaries, rollback within a chunk, and commits across chunks.

To repeat the comparison on current code, run the prototype under both `BENCH_SYNCHRONOUS=normal` and `full` (and save JSON with `BENCH_JSON` if needed):

```sh
BENCH_REPEATS=5 BENCH_JOBS=131072 BENCH_CLAIM_QUEUES=128,256 \
  BENCH_CLAIM_LIMITS=16 BENCH_CLAIM_MODES=grouped \
  BENCH_CLAIM_CHUNKS=all,16,32,64 pnpm bench:claim-grouping
```

`BENCH_ONLY=competing BENCH_REPEATS=8` gives more samples for the competing writer. The prototype reports transaction latency; the production `claimQueues` metric covers an entire call, possibly multiple transactions. Neither competitor latency nor p99 should be interpreted as a direct or precise measurement of SQLite lock wait.
