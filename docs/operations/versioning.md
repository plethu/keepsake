# Versioning

Keepsake uses crate versions for API and schema expectations. 2.0 is a breaking
boundary: the SQLx adapter no longer owns a Keepsake audit/outbox schema and
uses Dovecote for all SQL audit persistence and delivery.

## Semver

- **Major**: breaking changes to public API types, command semantics, storage
  record layout, or migration ordering. Keepsake 2.0 removes the maintained
  SQL audit repositories, audit history cursors, outbox paging, and
  claim/ack/release methods.
- **Minor**: additive API, new query helpers, new migrations that existing code
  can ignore until adopted.
- **Patch**: bug fixes and non-breaking schema corrections.

Pin `keepsake` and `keepsake-sqlx` to the same release. Select the matching
Dovecote adapter and apply both schemas before deploying code that depends on
the new audit contract.

## Upgrade checklist

- Read the changelog for API changes, new migration files, changed indexes, and
  required ordering.
- For new databases, apply the clean 2.0 domain baseline and Dovecote schema.
- For 1.x databases, select `upgrade_migrate()` explicitly and complete the
  documented history import before deploying 2.0 writers.
- Never edit or reorder published historical migrations.
- Test request paths and workers that use changed query helpers.

Embedded migrations define each track's required domain schema. Your service
decides when and how to apply it; Dovecote migrations are selected from the
matching Dovecote SQLx adapter. The adapter refuses a track mismatch rather
than guessing or dropping tables.
