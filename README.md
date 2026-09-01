# keepsake

> Let it be forgotten, as a flower is forgotten,
> Forgotten as a fire that once was singing gold.
>
> — Sara Teasdale, "Let It Be Forgotten" (1920)

`keepsake` remembers relations with an end: a trusted tag, a 24-hour mute, an
entitlement, a hold, a risk flag. Writes are idempotent, expiry runs on a
schedule you choose, and every lifecycle change produces a typed audit event.

I kept writing versions of this pattern in compliance-heavy services, where an
expiry that *mostly* works is still wrong. Keepsake is the version I wanted to
stop rewriting.

Keepsake owns relation lifecycle state. Your application still owns its users,
display data, tenant choice, and authorization rules.
[Gatekeep](https://github.com/plethu/gatekeep) can turn active relations into
authorization facts; [Dovecote](https://github.com/plethu/dovecote) stores the
audit events and their delivery state.

## Install

For Postgres:

```sh
cargo add keepsake@4 keepsake-sqlx@4
cargo add dovecote-sqlx-postgres@0.2
cargo add sqlx --features postgres,runtime-tokio,tls-rustls
cargo add time@0.3
```

SQLite and MySQL use the matching `keepsake-sqlx` feature and Dovecote adapter.
The [installation guide](docs/installation.md) covers all three backends and
schema setup.

Keepsake 4.0 uses `time::OffsetDateTime` throughout its public API and persists
timestamps at canonical microsecond precision. Persisted textual identifiers
are byte-preserving and case-sensitive: they must be non-empty, have no
leading or trailing Unicode whitespace, and fit within 191 UTF-8 bytes. Existing 3.x
databases require the v4 migration track before accepting 4.0 writes.

Start with the [quickstart](docs/quickstart.md), browse the
[guides and reference](docs/README.md), or read the API docs for
[`keepsake`](https://docs.rs/keepsake) and
[`keepsake-sqlx`](https://docs.rs/keepsake-sqlx).

The repository includes complete Postgres examples for
[tags](examples/postgres-tags) and [sanctions](examples/postgres-sanctions).

Licensed under `MIT OR Apache-2.0`.
