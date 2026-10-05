# Retired experiments

- **Schema v10/v12 write cost:** compared identical raw SQLite inserts, status
  transitions, and reschedules with WAL/NORMAL and WAL/FULL, including v12 with
  the obsolete active-group index. This isolated schema write overhead, not
  end-to-end queue throughput. The historical runner is available in Git;
  supported schema upgrades remain covered by adapter migration tests.
- **Batched completion prototype:** compared production asynchronous single-job
  completion with direct synchronous SQL transactions of 4/16 jobs. It did not
  isolate batching and was not a complete storage implementation. No batching
  API or performance claim is adopted from this experiment; a future proposal
  needs equivalent semantics and execution paths.

- **Grouped-claim SQL prototypes:** per-queue and grouped ungrouped-job runners
  supported the [512-job chunk decision](grouped-claim-chunk-size.md). The
  architecture is shipped; maintained cases now measure only production calls.
- **In-memory coordinator comparison:** shared vs isolated pollers did not measure
  durable SQLite workloads. Production grouped claims remain benchmarked, while
  coordinator behavior remains covered by core tests.
- **Group scheduling workloads:** saturated/future groups and fairness belong in
  correctness and query-plan tests, not a permanent timing matrix. Existing
  adapter tests cover blocked selection; storage conformance preserves the
  repeated 64-group, 256-job-claim fairness check.
- **Retention history matrix:** warm/reopened history, cleanup, VACUUM and
  checkpoint combinations are retired. Cleanup semantics remain covered by
  storage conformance and adapter retention tests; a specific large-history
  performance issue should get a targeted experiment.

The maintained grids also drop per-thread databases, intermediate scaling tiers,
in-memory adapter runs, and duplicate grouped-claim/cleanup adapter workloads.
Historical runners remain available in Git.
