# Installation

Add the core crate and the SQLx adapter to a Rust service that already uses
Postgres:

```sh
cargo add keepsake keepsake-sqlx
cargo add sqlx --features postgres,runtime-tokio,tls-rustls
```

The core crate has no backend features. The SQLx adapter enables embedded
migrations, relation-definition caching, and simple fulfillment counters by
default.

Run the embedded migration with a `sqlx::PgPool`:

```rust
use sqlx::PgPool;
use keepsake_sqlx::KeepsakeRepository;

let pool = PgPool::connect(&database_url).await?;
let repo = KeepsakeRepository::new(pool);
repo.migrate().await?;
```

Applications own authorization, entity tables, and any domain-specific joins.
Keepsake stores opaque subject identifiers and relation lifecycle state.

## SQLite

Select SQLite explicitly when the application does not need the default
Postgres backend:

```toml
[dependencies]
keepsake = "1"
keepsake-sqlx = { version = "1", default-features = false, features = ["sqlite", "migrations"] }
sqlx = { version = "0.9", default-features = false, features = ["sqlite", "runtime-tokio", "tls-rustls"] }
```

Construct `SqliteKeepsakeRepository` with a `sqlx::SqlitePool`. SQLite
serializes competing writers; retry `SQLITE_BUSY` failures at the job or
request boundary when multiple workers share a database file.

## MySQL

For MySQL, select the matching backend and migration feature:

```toml
[dependencies]
keepsake = "1"
keepsake-sqlx = { version = "1", default-features = false, features = ["mysql", "migrations"] }
sqlx = { version = "0.9", default-features = false, features = ["mysql", "runtime-tokio", "tls-rustls"] }
```

Construct `MySqlKeepsakeRepository` with a `sqlx::MySqlPool`. MySQL lifecycle
commands use InnoDB row locks, so configure lock-wait timeouts and retries for
the service's expected contention.

Add `cache` or `fulfillment-counters` only when those integration surfaces are
needed. The [feature reference](reference/feature-flags.md) lists the complete
matrix.
