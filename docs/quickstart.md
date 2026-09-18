# Quickstart

This example defines a manual `tag:trusted` relation, applies it to an account,
then reads the active relations for that account.

For a complete example that installs both schemas in a disposable PostgreSQL
database, run `cargo run -p postgres-tags` with `DATABASE_URL` set. The example
source is in [postgres-tags](../examples/postgres-tags).

The application code below starts with a migrated repository. `pool` is a `sqlx::PgPool` connected to the
Postgres database where Keepsake and
[Dovecote](https://github.com/plethu/dovecote) store lifecycle and audit rows.
The `relation_spec!` macro keeps the stable id, natural key, and expiry policy
together so normal call sites do not repeat strings.

```rust,no_run
# #[tokio::main]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
# let pool = sqlx::postgres::PgPoolOptions::new()
#     .connect_lazy("postgres://keepsake@example.invalid/keepsake")?;
use keepsake::{ActorRef, ApplyKeepsake, CommandContext, ExpiryPolicy, SubjectRef};
use keepsake_sqlx::KeepsakeRepository;
use time::OffsetDateTime;

let root = KeepsakeRepository::new(pool, "https://accounts.example.test/keepsake")?;
root.check_schema().await?;
let tenant = keepsake::TenantId::new("account-group-a")?;
let repo = root.for_tenant(tenant.clone());

keepsake::relation_spec! {
    struct TrustedTag {
        id: 0x018f_0000_0000_7000_8000_0000_0000_0001;
        key: ("tag", "trusted");
        expiry(_at) => ExpiryPolicy::ManualOnly;
    }
}

let now = OffsetDateTime::now_utc();
let timed_repo = repo.at(now);
timed_repo.upsert_relation_spec::<TrustedTag>().await?;

let subject = SubjectRef::new("account", "acct_123")?;
let command = ApplyKeepsake::for_spec::<TrustedTag>(
    tenant.clone(),
    subject.clone(),
    now,
    CommandContext::new(ActorRef::new("system", "worker")?),
);
let applied = repo.apply(&command).await?;

let active = repo.active_relations_for_subject(&subject).await?;
# let _ = (applied, active);
# Ok(())
# }
```

Applying the same tag while it is active returns the existing row with
`duplicate_prevented` set. This prevents duplicate active assignments; it is not
an exact receipt for an earlier operation. To recover an uncertain commit
without repeating business effects, use a stable command occurrence and inspect
`replayed`, as shown in [transactional lifecycle](reference/transactional-lifecycle.md).

Use `active_relations_for_subject_by_keys` when the request only needs a small
known set of dynamic relation keys. Use `active_relations_for_subject_by_ids`
when the request already has typed `RelationSpec` ids. Use membership scans when
a worker needs all active subjects for one relation.
