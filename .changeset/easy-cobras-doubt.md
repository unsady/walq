---
'@walq/core': patch
---

Report malformed job JSON as `onError` operation `parse` instead of `handler`, with the same job ID, attempt, and attemptsExhausted fields. Handlers still receive only valid payloads, and retry/backoff behavior is unchanged in both process and processMany. Consumers switching on context.operation should handle the new parse operation.
