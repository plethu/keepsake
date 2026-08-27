# Versioning

Keepsake uses crate versions for both API and schema expectations. From 1.0
onward, semver applies to the public Rust API and to schema expectations in
`keepsake-sqlx`.

## Semver

- **Major**: breaking changes to public API types, command semantics, storage
  record layout, or migration ordering.
- **Minor**: additive API, new query helpers, new migrations that existing code
  can ignore until adopted.
- **Patch**: bug fixes and non-breaking schema corrections.

Pin `keepsake` and `keepsake-sqlx` to the same release. Apply matching
migrations before deploying code that depends on new schema.

The 1.2 Dovecote bridge is an additive, opt-in migration feature. Its bridge
migrations and ledger can be removed only after cutover, reconciliation, and
the application's rollback window are complete; the legacy audit/outbox
migrations remain immutable.

## Upgrade checklist

- Read the changelog for API changes, new migration files, changed indexes, and
  required ordering.
- Apply matching migrations before deploying code that depends on new schema.
- Test request paths and workers that use changed query helpers.

Embedded migrations define the required schema. Your service decides when and
how to apply it.
