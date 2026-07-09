# Expiry Jobs

Timed relations stay active until a worker records the expiry transition. Run
that worker from cron, a queue worker, or a service loop outside request paths.

Keep the worker loop small and repeatable:

```rust
let repo = repo.at(chrono::Utc::now());
let expired = repo.expire_due_timed(500).await?;
```

Use a batch size that your database can commit quickly. Multiple workers may run
if each worker claims bounded batches and treats already-terminal rows as
no-ops.

Stable ordering matters because it makes retries and audit records easier to
reason about. Due rows are ordered by due time, relation id, subject id, and
keepsake id.

Keep alerting outside Keepsake. A common production check is "oldest due timed
expiry age" so stalled workers are visible before request paths start returning
unexpected active sanctions or holds.
