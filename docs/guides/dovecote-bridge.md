# Dovecote migration bridge

Keepsake 1.2.0 includes an additive, opt-in bridge for applications moving the
legacy SQL audit outbox to Dovecote. It is migration machinery, not a second
permanent publisher API. Existing constructors and the default feature set
remain legacy-only.

Enable only the adapter used by the application:

```toml
keepsake-sqlx = { version = "1.2", features = ["dovecote-postgres"] }
```

Then configure one stable absolute source. The source belongs to the producer;
it must not contain a hostname, pod name, database name, or other ephemeral
deployment identity. The default stream is `keepsake-audit`.

```rust
use keepsake_sqlx::{DovecoteBridgeConfig, KeepsakeRepository};

let bridge = KeepsakeRepository::new(pool)
    .with_dovecote_bridge(DovecoteBridgeConfig::new(
        "https://example.org/keepsake",
    )?);
let applied = bridge.apply(&command).await?;
```

An enabled mutation commits the Keepsake domain row, the existing normalized
audit row, the existing legacy outbox row, and one pending Dovecote event in a
single concrete backend transaction. Dovecote identity is
`source + keepsake-outbox-<legacy decimal outbox id>`. The event uses the
existing `keepsake.audit_event_recorded` type, `application/json`, the
`keepsake.audit.json.v1` deterministic UTF-8 codec, and the audit occurrence
timestamp. Subject, schema, partition key, and extensions remain absent. The
legacy publisher remains the owner while the Dovecote delivery is pending.

`publisher_identity(outbox_id)` reads the persisted bridge ledger, including
the exact payload bytes. This is the identity the legacy publisher must expose
unchanged so a consumer can deduplicate a row delivered during cutover. The
ledger's `payload_origin` is `bridge_exact` for these rows and is the
authoritative source if legacy JSON is later normalized or rewritten.

Use `claim_delivery` for bridge-aware legacy workers. It atomically claims only
rows already dual-written or imported into the bridge ledger and returns an
opaque `BridgeClaimToken` alongside each legacy outbox record. Persist and pass
that token to `acknowledge_delivery`; it is a claim generation, not a
timestamp. Reclaiming the same row rotates the token even when the worker and
lease expiry are unchanged. Legacy-only rows remain the responsibility of
reconciliation.

## History import

Call `import_history` in a bounded worker. Its audit and outbox high-water
marks are independent: `BridgeImportOptions::new(audit_high_water)` sets both
to the same value, and `with_outbox_high_water` supplies a different outbox
bound when the two tables have advanced independently. Both cursors are
durably checkpointed in the additive bridge configuration row. Rows with a
matching legacy outbox use `keepsake-outbox-<id>`; audit rows from before the
outbox migration use the reserved `keepsake-audit-legacy-<audit id>` identity
and the declared reconstruction codec. The importer traverses outboxes by
outbox id and audit-only rows by audit id with a `NOT EXISTS` outbox test; it
does not use a left join with one shared cursor, because one audit event may
have more than one outbox row.

PostgreSQL and MySQL decode legacy JSON values and deterministically re-encode
them as `legacy_outbox_reencoded`; SQLite validates the original outbox TEXT
but retains its exact bytes as `legacy_outbox_exact_text`. Rows reconstructed
from normalized audit columns and context attributes use `reconstructed_v1`.
These provenance values state what was available in the source and do not
claim byte preservation where the database representation discarded bytes.
Every imported payload is retained in the bridge ledger with its SHA-256
digest.

The project-owned `LegacyAuditEventV1` representation and
`encode_reconstructed_audit_v1` function are the only supported reconstruction
codec path. They validate normalized labels, references, decisions, and
context before deterministic JSON encoding. Migration tooling must not build a
second JSON envelope or mark reconstructed bytes as exact source bytes.

Delivery mapping is deliberately conservative: never-claimed and expired
claims become pending at Dovecote database time. Claims are evaluated against
database time (caller timestamps cannot bypass a live lease); active claims are
reported blocked and retried after expiry; delivered rows become delivered
with their legacy delivery timestamp. The bridge owns expired-claim fencing
inside its transaction. Callers do not need to clear a claim, and must not turn
an active legacy claim into a Dovecote claim. Rerunning a range is safe:
identical immutable content is an acknowledged import, while a changed
`source + id` is an error.

After the moving import reaches its persisted bounds and writers have been
fenced, call `finalize_upgrade_reconciliation()`. Before calling it, the
operator must stop and fence every legacy writer and publisher and hold the
legacy audit and outbox tables read-only through this operation and the
rollback window. The method cannot acquire an application-wide writer fence;
concurrent writes are rejected as reconciliation drift. This is the only
activation evidence writer. It performs a typed full reread of both independent source
sequences from zero, compares source identity, type, occurrence time, and
payload with the ledger, checks delivery state and active claims, verifies
that the source maxima still equal the persisted high-waters, and counts
unmapped Dovecote events. It records immutable singleton evidence only when
all deltas are zero. Ordinary `import_history` never writes that evidence, so
later old-writer rows cannot make an earlier activation marker appear final.

The bridge API is intentionally small:

- `import_history` imports complete normalized history through independent,
  inclusive, resumable audit and outbox high-water marks. It imports delivered
  rows as terminal Dovecote deliveries and never publishes them.
- `publisher_identity` returns the persisted source, id, event type, occurrence
  time, and exact payload bytes for a dual-written outbox row. A legacy
  publisher should use these values rather than recomputing them.
- `claim_delivery` atomically claims bridge-known outbox rows and returns an
  opaque generation for each worker claim.
- `acknowledge_delivery` is for the bridge-aware legacy publisher. It requires
  the worker to supply the exact opaque generation returned by
  `claim_delivery` and still own that live legacy lease, updates the legacy row and
  finalizes the corresponding canonical pending Dovecote delivery in one
  transaction. An exact retry is idempotent; a missing row, wrong/expired
  owner or generation, or different delivery timestamp is a typed error.

## Supported writers

| Writer | Default behavior | When the bridge is enabled | Publication owner |
| --- | --- | --- | --- |
| Keepsake 1.1.x | Legacy audit and outbox only | Not bridge-aware | Legacy publisher |
| Keepsake 1.2.x | Legacy audit and outbox only | Atomic legacy plus pending Dovecote dual-write | Legacy publisher |
| Keepsake 2.0.x | Dovecote event and delivery only | Bridge is removed | Dovecote publisher |

The bridge-aware repository view exposes `apply`, `revoke`, and
`revoke_by_subject` as atomic dual-write commands. It deliberately does not
expose `append_audit_event`, timed-expiry helpers, or fulfillment-expiry
helpers: those operations remain legacy-only in processes using the ordinary
1.2 repository view and their rows are found by reconciliation. A deployment
must not treat an unbridgeable writer as cut over merely because its lifecycle
commands use the bridge.

During a rolling deployment, old 1.1.x writers may continue to write only the
legacy schema. Repeated high-water imports find those rows. Do not start
Dovecote publication until every writer is bridge-aware or 2.0-capable and the
final reconciliation has zero delta. A legacy transport may publish immediately
before cutover and fail before acknowledgement; consumers must deduplicate the
identical CloudEvents `(source, id)` pair.

The live bridge gate is part of `mise run test-db` (and therefore the CI
database job). It runs the ordinary and bridge PostgreSQL suites, then the
ordinary and bridge MySQL-family suites, serially; each ignored target resets
its disposable database before testing. Provide `DATABASE_URL` or
`MYSQL_DATABASE_URL`, and use `TEST_DB_UP=0` when the service is managed by
the caller.

Apply the Keepsake 1.2 migration and the matching Dovecote adapter migration
through the application's migration runner before enabling the bridge. The
bridge never applies Dovecote DDL itself. At cutover, stop legacy workers,
reconcile every identity through the moving high-water range, switch one
publisher owner, and remove the bridge only after the application's rollback
window and reconciliation evidence are complete. Removal is an
application-owned destructive migration: drop only the additive bridge ledger
and configuration tables (or use the application's migration endpoint), and
never drop or rewrite the historical audit/outbox tables.
