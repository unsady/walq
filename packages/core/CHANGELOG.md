# @walq/core

## 0.2.1

### Patch Changes

- Change retry backoff jitter to reduce delays, with jitter `1` providing full jitter from zero up to the base delay.

## 0.2.0

### Minor Changes

- 911499e: Add atomic `queue.addMany()` support and require storage adapters to implement `enqueueMany()`.

### Patch Changes

- b619265: Add fixed and exponential handler retry backoff with positive-only jitter.
- f24e1e7: Support delayed and scheduled jobs through `queue.add()` options.
