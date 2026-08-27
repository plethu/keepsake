# SQLx Adapter

The SQLx adapter stores Keepsake lifecycle state in Postgres, SQLite, or MySQL.
It provides migrations, relation upsert, idempotent apply and revoke, active
subject lookups, membership scans, timed expiry scans, and simple fulfillment
counter projections. Audit persistence and delivery belong to Dovecote.

Construct a repository with an application-owned, stable absolute source URI:

```rust
let repo = KeepsakeRepository::new(pool, "https://accounts.example.test/keepsake")?;
```

The adapter uses the `keepsake-audit` stream, the `keepsake.audit_event_recorded`
event type, and `application/json` content. It does not invent a source. The
same `AuditEventId` is carried from command construction into the CloudEvents
id (`keepsake-audit-<audit id>`) so a retry of one logical operation remains
deduplicable.

The schema stores opaque subject identifiers and does not join application
entity tables.

Keepsake rows use the same flat lifecycle record shape as `KeepsakeRecord`.
Each backend migration rejects state/timestamp combinations that the core model
would reject during record conversion.

## Relation Definitions

| API | Use |
| --- | --- |
| `upsert_relation` | Runtime relation definitions from configuration or external data. |
| `upsert_relation_spec::<Spec>` | Code-owned relation catalogues. |
| `relation_by_id` | Cacheable lookup by stable id. |
| `relation_by_key` | Cacheable lookup by kind/name. |

`upsert_relation` returns the stored relation definition. If a relation already
exists for the same kind/name, the existing stable id is preserved and returned.
Use the returned id for follow-up commands.

Prefer `upsert_relation_spec::<Spec>` for application-owned relation catalogues.
`RelationSpec` keeps the stable id, natural key, and expiry policy together.
Spec upsert rejects an existing natural key with a different stored id instead
of applying a typed marker to the wrong relation.

Use `keepsake::relation_spec!` for static catalogues when you only need to
declare the marker type, stable id, key, enabled state, and expiry policy. Write
the `RelationSpec` implementation manually when a catalogue entry needs custom
comments, conditional compilation, or unusual organization.

## Mutation Helpers

`apply(&ApplyKeepsake)` is idempotent for active duplicates and writes one
Dovecote event in the same transaction. Duplicate commands return the existing
active keepsake with `duplicate_prevented = true`. Disabled relations reject new
non-duplicate applies, but duplicate applies still return the existing active
keepsake so retry loops do not turn a committed apply into an error.

Apply validates `SubjectRef` before writing. Empty subject kinds or ids fail
without inserting a keepsake row.

Use `ApplyKeepsake::for_spec::<Spec>` for typed relation catalogues and
`ApplyKeepsake::new` when the relation id is dynamic. `revoke(&RevokeKeepsake)`
records explicit revocation audit. `CommandContext::idempotency_key` is copied
into audit context attributes; duplicate active prevention is still based on the
active `(subject_kind, subject_id, relation_id)` relation, not on that key.

Mutation methods take explicit timestamps instead of reading database time.
Pass the same timestamp through related relation and keepsake writes when they
belong to one deterministic application operation. For request or job scopes,
use `repo.at(now)` to bind one explicit timestamp and call forwarding helpers
without repeating the timestamp argument:

```rust
use keepsake::{ActorRef, ApplyKeepsake, CommandContext};

let now = chrono::Utc::now();
let timed_repo = repo.at(now);

timed_repo.upsert_relation_spec::<TrustedTag>().await?;

let command = ApplyKeepsake::for_spec::<TrustedTag>(
    subject,
    now,
    CommandContext::new(ActorRef::new("system", "worker")?),
);
repo.apply(&command).await?;
timed_repo.expire_due_timed(500).await?;
```

## Audit history and delivery

Keepsake 2.0 does not expose `append_audit_event`, Keepsake audit repositories,
outbox cursors, or claim/ack/release methods. Those APIs belonged to the 1.x
schema and are deliberately absent from the maintained 2.0 SQLx surface.

For typed history, page Dovecote's live or snapshot stream with the selected
backend adapter and decode each `PagedEvent` payload as `keepsake::AuditEvent`.
The Dovecote delivery snapshot is the only durable delivery state. Publication
workers and transport clients remain application concerns; Dovecote provides
the lease and token-fenced lifecycle operations. Consumers deduplicate
at-least-once delivery with CloudEvents `(source, id)`.

The Dovecote event is enqueued in the same SQLx transaction as the lifecycle
mutation. A validation or enqueue failure rolls back the Keepsake state change.
For SQLite, the repository starts Dovecote's required `BEGIN IMMEDIATE` write
transaction before the domain mutation.

## Read Helpers

`active_relations_for_subject` returns active keepsakes with their stored
relation definitions in one query. Use it when the caller needs relation keys or
policies immediately after the subject lookup, instead of calling
`active_for_subject` and then resolving each relation separately.

`active_relations_for_subject_by_ids` is the bounded variant for typed relation
catalogues and other callers that already know stable relation ids:

```rust
let ids = [TrustedTag::ID, BetaFeature::ID];

let active = repo
    .active_relations_for_subject_by_ids(&subject, &ids)
    .await?;
```

`active_relations_for_subject_by_keys` is the bounded variant for request paths
that only care about a small known set of dynamic relation keys:

```rust
let keys = [
    TrustedTag::KEY.to_relation_key()?,
    BetaFeature::KEY.to_relation_key()?,
];

let active = repo
    .active_relations_for_subject_by_keys(&subject, &keys)
    .await?;
```

The bounded reads deduplicate requested ids or keys before joining. Results
include only requested relations that currently have active keepsakes for the
subject. Missing relations, revoked rows, and expired rows are omitted. Disabled
relation definitions are still returned when the keepsake itself is active,
matching `active_for_subject` semantics.

`KeepsakeRepository` implements `keepsake::ActiveRelationSource`. Prefer generic
adapter code such as `S: ActiveRelationSource` over storing the repository
behind `Arc<dyn ...>`. Use the erased `DynActiveRelationSource` only at
application composition boundaries that need heterogeneous runtime storage.
Adapters that translate relation state into policy facts should call the bounded
`*_by_ids` or `*_by_keys` methods when the request already determines the
relation catalogue. That is the intended shape for integrations such as
`gatekeep-keepsake`, where the policy supplies stable relation ids and the
application supplies the request subject.

For large relation memberships, prefer `active_membership_scan_after` with a
`MembershipCursor` over repeat calls to the first-page scan. The cursor is based
on `(subject_kind, subject_id, keepsake_id)`, matching the default membership
index order.

## Caching

With the default `cache` feature, callers may enable a local TTL cache for
relation definitions. This example assumes `pool` is a `sqlx::PgPool`.

```rust
use std::time::Duration;

use keepsake_sqlx::{KeepsakeRepository, LocalRelationCacheConfig};

let repo = KeepsakeRepository::new(pool, "https://accounts.example.test/keepsake")?
    .with_local_relation_cache(LocalRelationCacheConfig::new(Duration::from_mins(1)));
```

The cache only affects relation read helpers. Lifecycle mutations still read
authoritative state from the configured database.

`KeepsakeRepository` is generic over the `RelationCache` trait. Unconfigured
repositories use `NoopRelationCache`.
Applications that need cross-pod invalidation can provide their own cache
adapter.

Put active subject lookup and membership scan caches in an application wrapper
keyed by bounded values such as relation id, relation kind/name, subject
kind/id, cursor, limit, and any application partition scope.
