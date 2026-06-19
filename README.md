# keepsake

> Let it be forgotten, as a flower is forgotten,
> Forgotten as a fire that once was singing gold.
>
> — Sara Teasdale, "Let It Be Forgotten" (1920)

`keepsake` stores relations that a subject holds until policy ends them: a
trusted tag, a 24-hour mute, an entitlement, a hold, a risk flag, a feature gate.
It keeps those writes idempotent, expires them on a schedule you set, makes them
queryable, and records an audit trail.

The core crate is persistence-agnostic and synchronous. The SQLx adapter stores
the state in Postgres, with migrations and query helpers. The same contract is
documented for services on other stacks.

## Where it fits

Use the crate directly if a Rust and Postgres service fits how it works. When it
doesn't, the docs and structure carry the pattern itself, written so the same
contract, indexes, and lifecycle rules adapt to another language, framework,
database, or stack that needs deterministic lifecycle modeling.

Some responsibilities stay with your application. Keepsake does not join your
entity tables, make authorization decisions, invalidate distributed caches,
consume domain events, or stand in for migration review. Authorization can read
these relations later; this crate stores relation state and expiry.

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
- Large databases: use bounded queries and keyset pagination for hot
  reads. Cache relation definitions and request-scoped active lookups in
  application code when needed; keep mutation paths authoritative and
  idempotent.

## Defaults and feature flags

The default SQLx adapter includes migrations, indexed query helpers, idempotent
writes, timed expiry scans, and simple fulfillment counters. Anything that adds
schema, dependencies, or runtime cost goes behind a feature flag instead.

The lifecycle semantics are always on. Idempotency, duplicate active prevention,
deterministic ordering, opaque subjects, and indexed reads are part of the
contract, not switches to flip.

Indexes ship with the schema because the default query helpers rely on them.
There is an optional relation-definition cache for the SQLx adapter. Caching
active lifecycle state is left to the application, which knows the right
staleness, partitioning, invalidation, and memory limits.

## Why it exists

I'd been writing this pattern ad-hoc across production services for
compliance-heavy domains, where auditability and determinism are hard
requirements. It worked, so keepsake is the consolidated version, one robust
implementation to pull in instead of re-deriving the same lifecycle rules in
every project.

## License

Licensed under either of:

- Apache License, Version 2.0
- MIT license

at your option.
