# keepsake

> Let it be forgotten, as a flower is forgotten,
> Forgotten as a fire that once was singing gold.
>
> — Sara Teasdale, "Let It Be Forgotten" (1920)

`keepsake` stores relations that a subject holds until policy ends them: a
trusted tag, a 24-hour mute, an entitlement, a hold, a risk flag, a feature
gate. Writes are idempotent, expiry runs on a schedule you set, state is
queryable, and `apply`/`revoke` produce typed audit records.

The core crate is persistence-agnostic and synchronous. The `keepsake-sqlx`
adapter stores state through SQLx with migrations and query helpers. Postgres is
the default backend; SQLite and MySQL are available behind feature flags.

## Where it fits

Use the crate directly for a Rust service backed by Postgres, SQLite, or MySQL.
For other stacks, the schema, indexes, and lifecycle rules are documented so you
can port them to another language, framework, or database.

Some responsibilities stay with your application. Keepsake does not join your
entity tables, make authorization decisions, invalidate distributed caches, or
consume domain events. It stores relation state and expiry; authorization reads
those relations later.

## Install

```sh
cargo add keepsake keepsake-sqlx
cargo add sqlx --features postgres,runtime-tokio,tls-rustls
```

Run the embedded migration with a `sqlx::PgPool`:

```rust
use keepsake_sqlx::KeepsakeRepository;
use sqlx::PgPool;

let pool = PgPool::connect(&database_url).await?;
let repo = KeepsakeRepository::new(pool);
repo.migrate().await?;
```

For SQLite or MySQL, disable default features and enable the target backend,
then construct `SqliteKeepsakeRepository` or `MySqlKeepsakeRepository` with the
matching SQLx pool. The repository type and pool type are coupled at compile
time, and migrations also record a backend marker so a schema initialized for
one driver is not silently reused by another.

## Operations

- Migrations: `keepsake-sqlx` embeds SQLx migrations. Run them at startup or
  from your migration runner. Disable the `migrations` feature to vendor the SQL
  into another framework.
- Audit: `apply` and `revoke` take command objects and record actor/context
  metadata with each lifecycle change. They are the canonical mutation path.

Lifecycle semantics are always on. Idempotency, duplicate-active prevention,
deterministic ordering, opaque subjects, and indexed read paths are part of the
contract. An optional relation-definition cache is available; caching active
state is left to the application.

## Why it exists

I'd written this pattern ad-hoc across production services in compliance-heavy
domains, where auditability and determinism are requirements. keepsake is the
consolidated version, so you pull in one implementation instead of re-deriving
the same rules in every project.

## License

Licensed under either of:

- Apache License, Version 2.0
- MIT license

at your option.
