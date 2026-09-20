# walq

Typed queue API for walq storage adapters.

## API

- `new Queue(name, { storage, attempts? })` creates a queue. `attempts` defaults to 1.
- `queue.add(data)` serializes the data and enqueues a job.
- `queue.process(handler, { concurrency? })` registers the queue with the shared poller. `concurrency` defaults to 1.
- Handlers receive `(data, context)`. Context contains `signal`, `jobId`, and the current `attempt`.
- `worker.close()` stops new claims and waits for active handlers without aborting them.

Queues created with the same `Storage` instance share one poller and one serialized storage path, polled round-robin. A separate `Storage` instance has its own poller.

The poller checks empty queues once per second and wakes on `add()` and on handler completion. Active jobs use a 30-second lease with a heartbeat every 10 seconds. Handler failures retry immediately while attempts remain.

Handlers run concurrently as asynchronous tasks in the current Node.js process. They are not worker threads. The context signal aborts when the job loses its lease, but handlers must stop cooperatively. Delivery is at-least-once, so handlers must tolerate repeated execution.
