# walq

Typed queue API for walq storage adapters.

## API

- `new Queue(name, { storage, attempts?, onError? })` creates a queue. `attempts` defaults to 1.
- `queue.add(data)` serializes the data and enqueues a job.
- `queue.process(handler, { concurrency? })` registers the queue with the shared poller. `concurrency` defaults to 1.
- Handlers receive `(data, context)`. Context contains `signal`, `jobId`, and the current `attempt`.
- `worker.close()` stops new claims and waits for active handlers without aborting them.

Queues created with the same `Storage` instance share one queue-aware poller. Ready queues are polled in rotating order, and adapters with `claimQueues` can claim for one sweep in a single transaction. A separate `Storage` instance has its own coordinator.

The poller checks empty queues once per second and wakes on `add()` and on handler completion. Active jobs use a 30-second lease with a heartbeat every 10 seconds. Handler failures retry immediately while attempts remain.

Handlers run concurrently as asynchronous tasks in the current Node.js process. They are not worker threads. The context signal aborts when the job loses its lease, but handlers must stop cooperatively. Delivery is at-least-once, so handlers must tolerate repeated execution.

## Error reporting

`onError(err, ctx)` receives errors the queue would otherwise swallow. `ctx` is discriminated by `operation`:

- `claim` — a claim failed before any job was acquired. Grouped claim failures are reported once per affected queue.
- `heartbeat`, `complete`, `fail` — a lease mutation for a claimed job failed.
- `handler` — the handler threw or rejected.

Every context carries `queue`; job-scoped operations also carry `jobId` and `attempt`. Handler contexts add `attemptsExhausted`, derived from the claimed job's attempt limits. It means no retry can be issued; it does not confirm that a terminal failure was committed to storage.

`lease_lost` is a normal protocol outcome, not an error, and is never reported. Without `onError`, errors are written to `console.error` with the same context. The callback may be sync or async; errors it throws or rejects are logged safely and never affect polling, acknowledgement, or shutdown.
