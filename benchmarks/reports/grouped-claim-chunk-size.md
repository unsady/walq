# Why grouped claims use a 512-job chunk budget

Historical prototype measurements. See the [production-path ablation](sqlite-ablation.md)
for the current seven-repeat comparison and its transaction/API latency distinction.

Measured on an Apple M1 Pro (8 cores, Node 24), with 128/256 queues, claim limit 16, 131072 jobs per run, 5 measured repeats and 1 warmup. A second connection wrote to the same WAL file in the competing scenarios. Results below are approximate: short-run tails are noisy.

| Queues per transaction | Jobs per transaction | Transaction p95 at 256 queues | Competing writer p95 | Throughput vs one big transaction |
| ---------------------- | -------------------: | ----------------------------: | -------------------: | --------------------------------: |
| All (256)              |                 4096 |                       ~100 ms |            ~63–68 ms |                              100% |
| 64                     |                 1024 |                        ~29 ms |            ~23–40 ms |                           ~93–94% |
| **32**                 |              **512** |                    **~18 ms** |        **~10–22 ms** |                       **~91–92%** |
| 16                     |                  256 |                        ~12 ms |             ~5–23 ms |                           ~88–90% |

**Decision:** split grouped claims at a budget of 512 jobs (`packages/sqlite-common/src/chunking.ts`). At limit 16 this corresponds to 32 queues per transaction. It substantially shortens writer-lock holds without a large throughput loss; a job budget also adapts to other claim limits. The production path was separately checked against the 32-queue prototype. Adapter tests cover chunk boundaries, rollback within a chunk, and commits across chunks.

The ungrouped SQL prototypes used for this comparison are retired; their runner
is available in Git history before the benchmark-suite reduction. Maintained
`claim-grouping` cases measure only production `claimQueues` calls, which can span
multiple transactions and are not directly comparable to prototype transaction
timings. Competitor latency and p99 are not precise measurements of SQLite lock wait.
