# Installation

Keepsake 2.0 has two persistence boundaries. Keepsake owns relation and
entitlement state. [Dovecote](https://github.com/plethu/dovecote) owns
immutable audit events and their at-least-once deliveries. Dovecote is published
on crates.io; install both schemas before serving requests.

For Postgres:

```toml
[dependencies]
keepsake = "2"
keepsake-sqlx = "2"
dovecote-sqlx-postgres = "0.1"
sqlx = { version = "0.9", features = ["postgres", "runtime-tokio", "tls-rustls"] }
```

The normal setup is deliberately small:

```rust
use keepsake_sqlx::KeepsakeRepository;
use sqlx::PgPool;

let pool = PgPool::connect(&database_url).await?;
let repo = KeepsakeRepository::new(pool, "https://accounts.example.test/keepsake")?;
repo.migrate().await?;       // clean Keepsake 2.0 domain baseline
// Install the Dovecote schema with dovecote-sqlx-postgres before this call.
repo.check_schema().await?;
```

The source URI is application-owned, stable, and absolute. It is copied into
every Keepsake audit event and is part of the consumer deduplication identity.
The adapter supplies the `keepsake-audit` stream and its durable event type.
There is no migration-mode enum, legacy table configuration, bridge worker, or
duplicate publication setting in the normal constructor.

## SQLite

Select SQLite explicitly and use the `dovecote-sqlx-sqlite` adapter:

```toml
[dependencies]
keepsake = "2"
keepsake-sqlx = { version = "2", default-features = false, features = ["sqlite", "migrations"] }
dovecote-sqlx-sqlite = "0.1"
sqlx = { version = "0.9", default-features = false, features = ["sqlite", "runtime-tokio", "tls-rustls"] }
```

Construct `SqliteKeepsakeRepository` with a `sqlx::SqlitePool` and an absolute
source. Lifecycle writes use Dovecote's `BEGIN IMMEDIATE` boundary, so one
transaction contains the domain mutation and audit enqueue. SQLite serializes
competing writers; retry a bounded `SQLITE_BUSY` result at the request or job
boundary.

## MySQL

For MySQL, select the matching backend and use the Dovecote adapter:

```toml
[dependencies]
keepsake = "2"
keepsake-sqlx = { version = "2", default-features = false, features = ["mysql", "migrations"] }
dovecote-sqlx-mysql = "0.1"
sqlx = { version = "0.9", default-features = false, features = ["mysql", "runtime-tokio", "tls-rustls"] }
```

Construct `MySqlKeepsakeRepository` with a `sqlx::MySqlPool` and an absolute
source. MySQL lifecycle commands use InnoDB row locks; configure lock-wait
timeouts and retries for the service's expected contention.

## Upgrade versus clean installation

`migrate()` selects only the clean 2.0 domain baseline. It refuses a schema
whose metadata identifies the historical 1.x track. For an existing Keepsake
1.x installation, call `upgrade_migrate()` explicitly after installing and
checking Dovecote, then run the complete-history importer described in the
project migration runbook. Call `activate_upgrade()` only after reconciliation;
until then, the normal 2.0 schema check remains blocked. The upgrade track leaves old audit and outbox
tables available for reconciliation and rollback; 2.0 runtime code never
writes them and the migration does not drop them.

Do not point the clean baseline at an existing 1.x database, and do not point
the upgrade track at a clean 2.0 database. The adapter fails loudly when the
metadata does not match the requested track.

Applications own authorization, entity tables, and domain-specific joins.
Keepsake stores opaque subject identifiers and relation lifecycle state;
Dovecote stores audit occurrences, not live domain state.
