# walq

Typed queue API for walq storage adapters.

## API

- `new Queue(name, { storage, attempts? })` creates a queue. `attempts` defaults to 1.
- `queue.add(payload)` serializes and enqueues a payload.
- `queue.process(handler, { concurrency? })` starts one poller. `concurrency` defaults to 1.
- `worker.close()` stops new claims and waits for active handlers.

The poller checks an empty queue once per second. Active jobs use a 30-second lease with a heartbeat every 10 seconds. Handler failures retry immediately while attempts remain.

Handlers run concurrently as asynchronous tasks in the current Node.js process. They are not worker threads. Delivery is at-least-once, so handlers must tolerate repeated execution.
