---
'@walq/core': patch
---

Preserve the original error and context when onError fails, retain cron validation causes, and handle unexpected background rejections without letting logging failures disrupt polling. Public APIs and retry policies are unchanged.
