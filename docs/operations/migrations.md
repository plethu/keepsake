# Migrations

`keepsake-sqlx` embeds SQLx migrations behind the default `migrations` feature.
Call `KeepsakeRepository::migrate()` from startup or from your normal migration
runner.

```rust
use keepsake_sqlx::KeepsakeRepository;
use sqlx::PgPool;

let pool = PgPool::connect(&database_url).await?;
let repo = KeepsakeRepository::new(pool);
repo.migrate().await?;
```

The initial v0 migration creates:

- relation definitions keyed by `(kind, key)`;
- current lifecycle rows for keepsakes;
- simple fulfillment counter projections;
- append-only audit rows with deterministic context attribute rows.

Keepsake 1.2.0 adds only forward bridge migrations (`0007` for PostgreSQL,
`0006` for SQLite/MySQL). They create bridge configuration and identity-ledger
tables; historical migrations remain byte-for-byte immutable. The ledger keeps
the exact payload bytes plus `payload_codec`, `payload_sha256`, and explicit
`payload_origin` (`bridge_exact`, `legacy_outbox_reencoded`, or
`reconstructed_v1`). Apply the matching Dovecote adapter migration separately
in the application's migration runner. Do not enable a bridge feature until
both schemas are present.

Dovecote's MySQL/MariaDB schema creates validation triggers. The migration
account needs trigger DDL authority; with MySQL binary logging enabled, an
administrator may also need to enable `log_bin_trust_function_creators` for
schema installation. Ordinary Keepsake and bridge operations do not require
that server setting after the schema is installed.

Before activation, stop and fence every legacy writer and publisher. Hold the
legacy audit and outbox tables read-only while the final reconciliation runs
and through the rollback window. `finalize_upgrade_reconciliation()` cannot
acquire an application-wide writer fence; it rejects source or Dovecote drift,
but a deployment must provide the external fence and read-only precondition.

The bridge worker owns expiry fencing: it clears only claims whose lease has
expired according to database time and reports a still-live claim as blocked.
Operators do not need to clear claims manually, and must not copy an active
legacy claim into Dovecote. A bridge-aware publisher must use
`claim_delivery`, persist its opaque returned generation, and pass that
generation to `acknowledge_delivery`; the operation updates both delivery
representations atomically. The generation is stored in an additive bridge
table because the historical outbox schema has no token. It rotates on every
reclaim, including an identical worker and expiry timestamp.

The v0.2 lifecycle migration adds check constraints that keep the SQL record
shape aligned with the core `KeepsakeRecord` conversion rules:

- `expires_at` must match timed expiry policies and stay null for manual or
  fulfillment policies;
- applied rows cannot have terminal timestamps;
- revoked rows require `revoked_at` and cannot have `fulfilled_at`;
- expired rows must be either timed expiry with `expires_at` or fulfillment
  expiry with `fulfilled_at`;
- manual-only rows cannot be expired.

Keep application entity tables outside the Keepsake migration. Store opaque
subject identifiers in Keepsake and join them in application-owned queries when
you need display data or authorization context.

If your service keeps all DDL in one migration system, disable the default
`migrations` feature and copy the SQL there. The schema contract is the same.
