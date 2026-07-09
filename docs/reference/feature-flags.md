# Feature Flags

`keepsake` has no backend features. The core model and evaluator stay small,
synchronous, and persistence-agnostic.

## Core Crate

| Feature | Default | Use |
| --- | --- | --- |
| `test` | No | Exposes in-memory test helpers such as `InMemoryActiveRelations`, `ActiveRelationSeed`, and `InMemoryAuditSink`. |

In production, implement provider traits with durable sinks instead of using
test helpers.

`InMemoryActiveRelations` is the same active-relation read contract used by
adapters that depend on `ActiveRelationSource`. Test and example code can seed
typed relation specs directly without building `ActiveRelation` fixtures by
hand. The helper still takes caller-owned time and a caller-owned instance id:

```rust
use keepsake::{
    ExpiryPolicy, InMemoryActiveRelations, SubjectRef, relation_spec,
};

relation_spec! {
    struct TrustedTag {
        id: 0x1111_1111_1111_1111_1111_1111_1111_1111;
        key: ("tag", "trusted");
        expiry(_at) => ExpiryPolicy::ManualOnly;
    }
}

let source = InMemoryActiveRelations::empty();
let subject = SubjectRef::new("account", "acct_123")?;
let at = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")?
    .with_timezone(&chrono::Utc);

source.insert_active_for_spec::<TrustedTag>(
    0xaaaa_aaaa_aaaa_aaaa_aaaa_aaaa_aaaa_aaaa,
    subject,
    at,
)?;
```

Use `ActiveRelationSeed` when the fixture needs metadata:

```rust
use keepsake::ActiveRelationSeed;

source.insert_active_relation(
    ActiveRelationSeed::<TrustedTag>::from_u128(
        0xbbbb_bbbb_bbbb_bbbb_bbbb_bbbb_bbbb_bbbb,
        subject,
        at,
    )
    .with_attribute("fixture", "trusted-account")
    .with_attribute("reason", "integration-test"),
)?;
```

## SQLx Adapter

`keepsake-sqlx` uses feature flags for integration surface area, not lifecycle
semantics.

| Feature | Default | Use |
| --- | --- | --- |
| `migrations` | Yes | Exposes embedded SQLx migrations through `KeepsakeRepository::migrate()`. |
| `cache` | Yes | Exposes opt-in relation-definition caching through repository cache helpers. |
| `fulfillment-counters` | Yes | Exposes simple built-in counter projection writes. |

Disable `migrations` when your service vendors the SQL into a separate
migration framework. Disable `fulfillment-counters` when fulfillment state is
owned entirely by views, materialized views, event projections, or service
lookups.

The `cache` feature is inert until configured with
`KeepsakeRepository::with_local_relation_cache` or an application-provided
`RelationCache` adapter.

## Stable Contract

These are always part of the contract:

- idempotent mutation behavior;
- duplicate active prevention;
- stable ordering for jobs and scans;
- opaque subject identifiers;
- bounded, indexed query shapes;
- application-owned authorization and joins.

Indexes are not feature flags. They are part of the Postgres schema contract
because disabling them can make otherwise correct APIs too slow or
contention-prone in production.

Relation-definition caching is a feature because the key space is bounded and
the invalidation rules are small. Local caching is the default implementation,
and the generic `RelationCache` trait supports application Valkey, Redis, or
control-plane adapters. Applications handle active lifecycle state caching
because they know TTLs, invalidation, partitioning, and memory budgets.
