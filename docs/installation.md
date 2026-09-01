# Installation

Keepsake 4.0 has two persistence boundaries. Keepsake owns relation and
entitlement state. [Dovecote](https://github.com/plethu/dovecote) owns
immutable audit events and their at-least-once deliveries. Dovecote is published
on crates.io; install both schemas before serving requests.

For Postgres:

```toml
[dependencies]
keepsake = "4"
keepsake-sqlx = "4"
dovecote-sqlx-postgres = "0.2"
sqlx = { version = "0.9", features = ["postgres", "runtime-tokio", "tls-rustls"] }
time = "0.3"
```

A clean Postgres setup is short:

```rust
use keepsake_sqlx::KeepsakeRepository;
use sqlx::PgPool;

let pool = PgPool::connect(&database_url).await?;
let repo = KeepsakeRepository::new(pool, "https://accounts.example.test/keepsake")?;
repo.migrate().await?;       // clean Keepsake 4.0 domain baseline
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
keepsake = "4"
keepsake-sqlx = { version = "4", default-features = false, features = ["sqlite", "migrations"] }
dovecote-sqlx-sqlite = "0.2"
sqlx = { version = "0.9", default-features = false, features = ["sqlite", "runtime-tokio", "tls-rustls"] }
time = "0.3"
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
keepsake = "4"
keepsake-sqlx = { version = "4", default-features = false, features = ["mysql", "migrations"] }
dovecote-sqlx-mysql = "0.2"
sqlx = { version = "0.9", default-features = false, features = ["mysql", "runtime-tokio", "tls-rustls"] }
time = "0.3"
```

Construct `MySqlKeepsakeRepository` with a `sqlx::MySqlPool` and an absolute
source. MySQL lifecycle commands use InnoDB row locks; configure lock-wait
timeouts and retries for the service's expected contention.

## Keepsake 4.0 contracts

The public API uses `time::OffsetDateTime`; `chrono::DateTime<Utc>` is no
longer accepted. Serde wire timestamps remain RFC3339, while SQLx writes
canonicalize instants to UTC microsecond precision before persistence and
Dovecote publication.

Tenant ids, relation kind/name components, subject and actor components, and
built-in fulfillment keys share a portable textual identifier contract: values
must be non-empty, have no leading or trailing Unicode whitespace, and be at
most 191 UTF-8 bytes. Values are byte-preserving, case-sensitive, and are not
Unicode-normalized. Constructors, serde boundaries, and SQL writes enforce the
exact boundary. The v4 migration preflight applies the same Rust validator to
existing rows; database checks provide an additional non-empty, byte-length,
and ordinary edge-space defence.

The MySQL v4 schema changes these identifier columns to explicit
`utf8mb4_bin` collation. This is required for both MySQL and MariaDB; a schema
with an implicit or case-insensitive collation does not satisfy
`check_schema()`.

## Upgrade versus clean installation

`migrate()` selects the clean 4.0 domain baseline. It refuses a schema
whose metadata identifies the historical 1.x track. For an existing Keepsake
1.x installation, call `upgrade_migrate()` explicitly after installing and
checking Dovecote, then run the complete-history importer described in the
project migration runbook. Call `activate_upgrade()` only after reconciliation;
until then, the normal 4.0 schema check remains blocked. The upgrade track leaves old audit and outbox
tables available for reconciliation and rollback; 4.0 runtime code never
writes them and the migration does not drop them.

Do not point the clean baseline at an existing 1.x database, and do not point
the upgrade track at a clean 4.0 database. The adapter fails loudly when the
metadata does not match the requested track.

For an existing Keepsake 2.x installation on PostgreSQL, MySQL, or SQLite, call
`prepare_tenant_upgrade()`, apply an independently reviewed mapping that fills
every nullable `tenant_id`, then call `activate_tenant_upgrade()`. The adapter
never infers a tenant or uses a sentinel value. MySQL deployments serving
regulated tenants should prefer a separate database per tenant; SQLite's
strongest boundary is one file per tenant. Shared-schema tenancy is supported
by the v3 adapter, but physical separation can make backup, retention, access
review, and incident containment easier to demonstrate.

For an existing Keepsake 3.x clean database, stop 3.x writers, run
`repo.migrate()` with the 4.0 binary, and then run `repo.check_schema()` before
accepting new writes. This applies the additive v4 identifier-contract
migration; published v3 migration files are not edited. The preflight applies
the exact Rust identifier validator to existing rows on every backend before
schema activation. Review and remap any incompatible legacy identifiers before
retrying.

Audit payload compatibility is explicit. Current payloads carry
`schema_version = 4`. Payloads from v3 that omit the field, or explicitly carry
version 3, are routed by `decode_audit_event` to its typed legacy outcome and
require an application-owned decoder. Unknown versions are rejected. Historical
outer identities such as `keepsake-outbox-N` and
`keepsake-audit-legacy-N` remain an explicit legacy path and are never accepted
as current events.

Applications own authorization, entity tables, and domain-specific joins.
Keepsake stores opaque subject identifiers and relation lifecycle state;
Dovecote stores audit occurrences, not live domain state.
