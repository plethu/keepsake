# Keepsake

Keepsake stores relation lifecycles for Rust applications: tags, sanctions,
entitlements, holds, risk flags, feature gates, and other cases where a subject
has a relation until policy changes.

It gives you deterministic writes, expiry, reads, worker scans, and typed audit
records. Users, accounts, authorization, tenancy, display data, and
product-specific joins stay in your application.

API reference: [docs.rs/keepsake](https://docs.rs/keepsake) and
[docs.rs/keepsake-sqlx](https://docs.rs/keepsake-sqlx).

## Start here

1. [Overview](overview.md) — what Keepsake is for
2. [Installation](installation.md) — add the crates to a Rust service
3. [Quickstart](quickstart.md) — apply a relation and read active state

## Guides

- [Tags](guides/tags.md)
- [Sanctions](guides/sanctions.md)
- [Fulfillment projections](guides/fulfillment-projections.md)
- [Expiry jobs](guides/expiry-jobs.md)
- [Audit sinks](guides/audit-sinks.md)
- [Observability](guides/observability.md)

## Reference

- [Core concepts](reference/core-concepts.md)
- [Lifecycle model](reference/lifecycle-model.md)
- [Command API](reference/command-api.md)
- [SQLx adapter](reference/sqlx-adapter.md)
- [Feature flags](reference/feature-flags.md)
- [Error model](reference/error-model.md)

## Operations

- [Migrations](operations/migrations.md)
- [Versioning](operations/versioning.md)
- [Indexes](operations/indexes.md)
- [Cron and workers](operations/cron-workers.md)
- [Query performance](operations/query-performance.md)
