# keepsake

`keepsake` is implementation guidance first and a Rust dependency second for
deterministic, auditable, queryable relation lifecycles: tags, sanctions,
entitlements, holds, risk flags, feature gates, and other "subject has relation
until policy changes" workflows.

Keepsake covers one pattern: relation state that must survive retries, expire
deterministically, and stay queryable. It records the parts that are easy to
forget when re-implementing this pattern in different stacks: idempotent
mutations, deterministic expiry, stable batch ordering, opaque application
subjects, explicit indexes, audit boundaries, and cacheable read shapes. The
crates provide the Rust/Postgres implementation; the docs also serve as an
implementation guide for teams porting the same contracts without inheriting
this dependency.

The core crate is persistence-agnostic and synchronous. The SQLx adapter is
Postgres-first and owns async database access, migrations, and query helpers.

## Dependency Boundary

Use the dependency when a Rust/Postgres service can accept the schema and
repository contracts. Use the docs as implementation guidance when your service
needs a different language, migration framework, database topology, tenancy
model, cache layer, or audit sink.

Keepsake deliberately does not join application entity tables, own
authorization, own distributed cache invalidation, consume every domain event,
or replace application migration review. It stores and evaluates lifecycle
state for opaque subjects.

## Workspace

- `crates/keepsake`: typed domain model, deterministic lifecycle evaluator,
  commands, provider traits, audit, observability, and errors.
- `crates/keepsake-sqlx`: Postgres migrations and SQLx repositories.
- `examples/postgres-tags`: tag assignment example using Postgres.
- `examples/postgres-sanctions`: sanction example using Postgres.
- `docs-site`: Astro Starlight documentation site managed with pnpm.

## Tooling

Tool versions are pinned in `.mise.toml`. The workspace uses Rust 2024 and a
strict lint profile that denies common low-quality patterns such as `unwrap`,
`expect`, `panic!`, `todo!`, and `dbg!`.

```sh
mise install
make check
```

Database integration tests run against Docker:

```sh
make test-db
```

## Operations

- Migrations: `keepsake-sqlx` embeds SQLx migrations. Applications may call them
  from startup or a dedicated migration job, but production rollout order,
  backups, and online DDL policy stay application-owned. Disable the
  `migrations` feature if your service vendors or rewrites migrations in its
  own migration framework.
- Versioning: crate releases follow semver. Schema changes must be documented
  with their operational impact; backwards-incompatible schema changes require a
  major release or an explicit pre-1.0 migration note.
- Indexing: the initial schema includes indexes for active subject lookups,
  active relation scans, timed expiry jobs, fulfillment scans, and duplicate
  active prevention. Treat additional tenancy or partitioning indexes as
  application-specific.
- Large databases: hot reads should use bounded query shapes and keyset
  pagination. Cache relation definitions and request-scoped active lookups in
  application code when needed; keep mutation paths authoritative and
  idempotent.

## Feature Direction

The default SQLx adapter is batteries-included for the common Rust/Postgres
case: migrations, indexed query helpers, idempotent writes, timed expiry scans,
and simple fulfillment counters. Situational integration points are feature-flagged when they add schema surface,
dependencies, or runtime integration cost.

Core lifecycle semantics are not feature flags. Idempotency, duplicate active
prevention, deterministic ordering, opaque subjects, and indexed read shapes are
part of the contract.

Indexes should ship with the schema rather than hide behind features; otherwise
the dependency quietly becomes unsafe at production volume. Keepsake ships an
optional relation-definition cache for the SQLx adapter, but active lifecycle
state caching stays application-owned because applications own staleness,
partitioning, invalidation, and memory budgets.

## License

Licensed under either of:

- Apache License, Version 2.0
- MIT license

at your option.
