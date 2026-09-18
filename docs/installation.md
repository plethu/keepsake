# Installation

Keepsake has two persistence boundaries. Keepsake owns relation and
entitlement state. [Dovecote](https://github.com/plethu/dovecote) owns
immutable audit events and their at-least-once deliveries. Dovecote is published
on crates.io; install both schemas before serving requests.

Keepsake 6.0 retains the v4 database and JSON contracts. Existing v4 databases
need no new migration; the [versioning guide](operations/versioning.md) covers
the Rust API changes.

For Postgres:

```toml
[dependencies]
keepsake = "6"
keepsake-sqlx = "6"
dovecote-sqlx-postgres = "0.2"
sqlx = { version = "0.9", features = ["postgres", "runtime-tokio", "tls-rustls"] }
time = "0.3"
```

For a complete first write, run the [Postgres tags example](../examples/postgres-tags/src/main.rs)
against a local development database:

```sh
DATABASE_URL=postgres://keepsake:keepsake@localhost:55432/keepsake \
  cargo run -p postgres-tags
```

From a repository checkout, `mise exec -- just db-up` starts that database.
The example installs both schemas, checks them, defines a tag, and applies it.
See the [quickstart](quickstart.md) for the application code.

The source URI is application-owned, stable, and absolute. It is copied into
every Keepsake audit event. Together with the stored tenant and event id, it
forms the tenant-scoped Dovecote deduplication identity. The adapter supplies
the `keepsake-audit` stream and its durable event type.

## SQLite

Select SQLite explicitly and use the `dovecote-sqlx-sqlite` adapter:

```toml
[dependencies]
keepsake = "6"
keepsake-sqlx = { version = "6", default-features = false, features = ["sqlite", "migrations"] }
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
keepsake = "6"
keepsake-sqlx = { version = "6", default-features = false, features = ["mysql", "migrations"] }
dovecote-sqlx-mysql = "0.2"
sqlx = { version = "0.9", default-features = false, features = ["mysql", "runtime-tokio", "tls-rustls"] }
time = "0.3"
```

Construct `MySqlKeepsakeRepository` with a `sqlx::MySqlPool` and an absolute
source. MySQL lifecycle commands use InnoDB row locks; configure lock-wait
timeouts and retries for the service's expected contention.

## Persistence contracts (v4)

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

## Existing databases

Use the [migration guide](operations/migrations.md) for an existing database;
it covers each supported upgrade track, tenant mapping, and history import.
The [versioning guide](operations/versioning.md) covers Rust API changes.
Published migrations and historical audit bytes must remain unchanged.
