# Installation

Keepsake 3.0 has two persistence boundaries. Keepsake owns relation and
entitlement state. [Dovecote](https://github.com/plethu/dovecote) owns
immutable audit events and their at-least-once deliveries. Dovecote is published
on crates.io; install both schemas before serving requests.

For Postgres:

```toml
[dependencies]
keepsake = "3"
keepsake-sqlx = "3"
dovecote-sqlx-postgres = "0.2"
sqlx = { version = "0.9", features = ["postgres", "runtime-tokio", "tls-rustls"] }
```

The normal setup is deliberately small:

```rust
use keepsake_sqlx::KeepsakeRepository;
use sqlx::PgPool;

let pool = PgPool::connect(&database_url).await?;
let repo = KeepsakeRepository::new(pool, "https://accounts.example.test/keepsake")?;
repo.migrate().await?;       // clean Keepsake 3.0 domain baseline
// Install the Dovecote schema with dovecote-sqlx-postgres before this call.
repo.check_schema().await?;
let tenant = keepsake::TenantId::new("account-group-a")?;
let scoped = repo.for_tenant(tenant);
```

The source URI is application-owned, stable, and absolute. It is copied into
every Keepsake audit event. Together with the stored tenant and event id, it
forms the tenant-scoped Dovecote deduplication identity. The adapter supplies
the `keepsake-audit` stream and its durable event type.
There is no migration-mode enum, legacy table configuration, bridge worker, or
duplicate publication setting in the normal constructor.

## SQLite

Select SQLite explicitly and use the `dovecote-sqlx-sqlite` adapter:

```toml
[dependencies]
keepsake = "3"
keepsake-sqlx = { version = "3", default-features = false, features = ["sqlite", "migrations"] }
dovecote-sqlx-sqlite = "0.2"
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
keepsake = "3"
keepsake-sqlx = { version = "3", default-features = false, features = ["mysql", "migrations"] }
dovecote-sqlx-mysql = "0.2"
sqlx = { version = "0.9", default-features = false, features = ["mysql", "runtime-tokio", "tls-rustls"] }
```

Construct `MySqlKeepsakeRepository` with a `sqlx::MySqlPool` and an absolute
source. MySQL lifecycle commands use InnoDB row locks; configure lock-wait
timeouts and retries for the service's expected contention.

## Upgrade versus clean installation

`migrate()` selects only the clean 3.0 domain baseline. It refuses a schema
whose metadata identifies the historical 1.x track. For an existing Keepsake
1.x installation, call `upgrade_migrate()` explicitly after installing and
checking Dovecote, then run the complete-history importer described in the
project migration runbook. Call `activate_upgrade()` only after reconciliation;
until then, the normal 3.0 schema check remains blocked. The upgrade track leaves old audit and outbox
tables available for reconciliation and rollback; 3.0 runtime code never
writes them and the migration does not drop them.

Do not point the clean baseline at an existing 1.x database, and do not point
the upgrade track at a clean 3.0 database. The adapter fails loudly when the
metadata does not match the requested track.

For an existing Keepsake 2.x installation on PostgreSQL, MySQL, or SQLite, call
`prepare_tenant_upgrade()`, apply an independently reviewed mapping that fills
every nullable `tenant_id`, then call `activate_tenant_upgrade()`. The adapter
never infers a tenant or uses a sentinel value. MySQL deployments serving
regulated tenants should prefer a separate database per tenant; SQLite's
strongest boundary is one file per tenant. Shared-schema tenancy is supported
by the v3 adapter, but physical separation can make backup, retention, access
review, and incident containment easier to demonstrate.

Applications own authorization, entity tables, and domain-specific joins.
Keepsake stores opaque subject identifiers and relation lifecycle state;
Dovecote stores audit occurrences, not live domain state.
