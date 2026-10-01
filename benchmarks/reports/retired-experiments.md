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

Group scheduling and stress checks were not retired: their distinct workloads
now share the maintained `groups` suite.
