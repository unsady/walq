---
'@walq/core': patch
---

Include error cause chains in persisted job.error diagnostic text. Formatting is depth-bounded, handles circular references, and does not invoke custom inspection hooks. Errors without causes keep their existing text; onError still receives the original error and retry behavior is unchanged.
