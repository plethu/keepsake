# Versioning

Keepsake uses crate versions for API and schema expectations. 4.0 is the
current breaking boundary: public timestamps use `time::OffsetDateTime`,
persisted textual identifiers have an explicit portable contract, and the
SQLx adapter requires the additive v4 schema track alongside Dovecote.

## Semver

- **Major**: breaking changes to public API types, command semantics, storage
  record layout, or migration ordering. Keepsake 4.0 replaces public
  `chrono::DateTime<Utc>` with `time::OffsetDateTime` and adds the v4
  identifier/schema contract. The historical Keepsake 2.0 release removed the
  maintained SQL audit repositories, audit history cursors, outbox paging, and
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
- For new databases, apply the clean 4.0 domain baseline, v4 contract, and
  Dovecote schema.
- For existing v3 databases, run `repo.migrate()` to apply the additive v4
  track. Resolve the migration's incompatible-row preflight before deploying
  4.0 writers; do not edit historical v3 SQL.
- For 1.x databases, select `upgrade_migrate()` explicitly and complete the
  documented history import before deploying the historical 2.0 writers.
- Never edit or reorder published historical migrations.
- Test request paths and workers that use changed query helpers.

Embedded migrations define each track's required domain schema. Your service
decides when and how to apply it; Dovecote migrations are selected from the
matching Dovecote SQLx adapter. The adapter refuses a track mismatch rather
than guessing or dropping tables.
