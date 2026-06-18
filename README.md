# keepsake

`keepsake` is a Rust crate for relation lifecycles: tags, sanctions,
entitlements, holds, risk flags, feature gates, and other "subject has relation
until policy changes" workflows. It keeps those rows queryable, idempotent, and
auditable. The docs define the same behavior for implementations in other
stacks.

Keepsake covers one pattern: relation state that survives retries, expires on a
known schedule, and remains queryable. It handles idempotent mutations,
deterministic expiry, stable batch ordering, opaque application subjects,
explicit indexes, audit records, and cacheable read shapes.

The core crate is persistence-agnostic and synchronous. The SQLx adapter adds
Postgres access, migrations, and query helpers.

## Dependency Boundary

Use the crate when a Rust/Postgres service can use its schema and repository
API. Use the docs as a reference when another language, migration framework,
database topology, tenancy model, cache layer, or audit sink needs the same
behavior.

Keepsake does not join application entity tables, handle authorization,
invalidate distributed caches, consume domain events, or replace application
migration review. Authorization can use these relations later, but this crate
only stores relation state and expiry.

## Install

```sh
cargo add keepsake keepsake-sqlx
cargo add sqlx --features postgres,runtime-tokio-rustls
```

Run the embedded migration with a `sqlx::PgPool`:

```rust
use keepsake_sqlx::KeepsakeRepository;
use sqlx::PgPool;

let pool = PgPool::connect(&database_url).await?;
let repo = KeepsakeRepository::new(pool);
repo.migrate().await?;
```

## Operations

- Migrations: `keepsake-sqlx` embeds SQLx migrations. Call them from startup or
  your normal migration runner. Disable the `migrations` feature if your
  service vendors the SQL into another migration framework.
- Versioning: crate releases follow semver. Check the changelog for schema
  impact before upgrading.
- Indexing: the initial schema includes indexes for active subject lookups,
  active relation scans, timed expiry jobs, fulfillment scans, and duplicate
  active prevention. Treat additional tenancy or partitioning indexes as
  application-specific.
- Large databases: use bounded query shapes and keyset pagination for hot
  reads. Cache relation definitions and request-scoped active lookups in
  application code when needed; keep mutation paths authoritative and
  idempotent.

## Feature Direction

The default SQLx adapter includes migrations, indexed query helpers, idempotent
writes, timed expiry scans, and simple fulfillment counters. Extra integration
points use feature flags when they add schema, dependencies, or runtime cost.

Core lifecycle semantics are not feature flags. Idempotency, duplicate active
prevention, deterministic ordering, opaque subjects, and indexed read shapes are
part of the contract.

Indexes ship with the schema because the default query helpers rely on them.
Keepsake includes an optional relation-definition cache for the SQLx adapter.
Applications decide how to cache active lifecycle state because they know the
right staleness, partitioning, invalidation, and memory limits.

## License

Licensed under either of:

- Apache License, Version 2.0
- MIT license

at your option.
