# keepsake

> Let it be forgotten, as a flower is forgotten,
> Forgotten as a fire that once was singing gold.
>
> — Sara Teasdale, "Let It Be Forgotten" (1920)

`keepsake` stores relations that a subject holds until policy ends them: a
trusted tag, a 24-hour mute, an entitlement, a hold, a risk flag, a feature
gate. Writes are idempotent, expiry runs on a schedule you set, and state is
queryable. Lifecycle commands produce typed audit events; the SQLx adapter
persists those events through Dovecote.

The core crate is persistence-agnostic and synchronous. The `keepsake-sqlx`
adapter stores domain state through SQLx with migrations and query helpers.
Postgres is the default backend; SQLite and MySQL are available behind feature
flags. Dovecote is the sole SQL audit and delivery model in 3.0.

I'd written this pattern ad-hoc across production services in compliance-heavy
domains, where auditability and determinism are requirements. keepsake is the
consolidated version, so you pull in one implementation instead of re-deriving
the same rules in every project.

## Boundaries

Use the crate directly for a Rust service backed by Postgres, SQLite, or MySQL.
For other stacks, the schema, indexes, and lifecycle rules are documented so you
can port them to another language, framework, or database.

Some responsibilities stay with your application. Keepsake does not join your
entity tables, make authorization decisions, invalidate distributed caches, or
consume domain events. It stores relation state and expiry; authorization reads
those relations later.

## Install

```sh
cargo add keepsake@3 keepsake-sqlx@3
cargo add dovecote-sqlx-postgres@0.2
cargo add sqlx --features postgres,runtime-tokio,tls-rustls
```

Dovecote 0.2 and its SQLx adapters are published on crates.io. Add the adapter
for the backend you use; the [installation guide](docs/installation.md) covers
SQLite and MySQL as well as Postgres.

Run the embedded migration with a `sqlx::PgPool`:

```rust
use keepsake_sqlx::KeepsakeRepository;
use sqlx::PgPool;

let pool = PgPool::connect(&database_url).await?;
let root = KeepsakeRepository::new(pool, "https://accounts.example.test/keepsake")?;
root.migrate().await?;
// Install the selected Dovecote schema before this check.
root.check_schema().await?;
let tenant = keepsake::TenantId::new("account-group-a")?;
let repo = root.for_tenant(tenant);
```

For SQLite or MySQL, disable default features and enable the target backend and
matching Dovecote SQLx adapter. Construct `SqliteKeepsakeRepository` or
`MySqlKeepsakeRepository` with the matching SQLx pool and the same kind of
application-owned absolute source URI. The tenant, source, and event id form
the Dovecote event identity; there is no library-owned source default.

See [docs/installation.md](docs/installation.md) and
[docs/reference/feature-flags.md](docs/reference/feature-flags.md) for backend
setup.

## Documentation

- [docs/](docs/README.md) — guides and reference for integrators
- [docs.rs/keepsake](https://docs.rs/keepsake) — core crate API
- [docs.rs/keepsake-sqlx](https://docs.rs/keepsake-sqlx) — SQLx adapter API

Examples: `examples/postgres-tags`, `examples/postgres-sanctions` (require
`DATABASE_URL` and a running Postgres instance).

## Operations

- Migrations: a new installation uses the explicit 3.0 clean domain baseline
  and a separately installed Dovecote schema. Upgrades use
  `upgrade_migrate()` and retain historical audit tables for reconciliation;
  they are never selected implicitly.
- Audit: `apply`, `revoke`, and expiry helpers write one Dovecote event in the
  same transaction as the Keepsake mutation. Page and claim delivery through
  Dovecote; Keepsake has no parallel audit repository or outbox API.

Lifecycle semantics are always on. Idempotency, duplicate-active prevention,
deterministic ordering, opaque subjects, and indexed read paths are part of the
contract. An optional relation-definition cache is available; caching active
state is left to the application.

## License

Licensed under either of:

- Apache License, Version 2.0
- MIT license

at your option.
