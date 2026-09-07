//! `PostgreSQL` identifier storage validation and bounded migration scans.

#[cfg(feature = "migrations")]
use crate::repository::schema::IDENTIFIER_SCAN_BATCH_SIZE;
#[cfg(feature = "migrations")]
use crate::repository::schema::PERSISTED_IDENTIFIERS;
#[cfg(feature = "migrations")]
use crate::repository::schema::PersistedIdentifier;
use crate::repository::schema::RepositoryResult;
use crate::repository::schema::mismatch;
#[cfg(feature = "migrations")]
use crate::repository::schema::persisted_identifier_type_mismatch;
#[cfg(feature = "migrations")]
use crate::repository::schema::validate_persisted_identifier_bytes;

pub(super) async fn postgres_v4_identifier_columns_check(
    pool: &sqlx::PgPool,
) -> RepositoryResult<()> {
    let expected = [
        ("keepsake_relation_definitions", "tenant_id"),
        ("keepsake_relation_definitions", "kind"),
        ("keepsake_relation_definitions", "key"),
        ("keepsakes", "tenant_id"),
        ("keepsakes", "subject_kind"),
        ("keepsakes", "subject_id"),
        ("keepsake_fulfillment_counters", "tenant_id"),
        ("keepsake_fulfillment_counters", "key"),
        ("keepsake_fulfillment_checklist", "tenant_id"),
        ("keepsake_fulfillment_checklist", "item"),
    ];
    for (table, column) in expected {
        let collation = sqlx::query_scalar::<_, Option<String>>(
            "select collation_name from information_schema.columns where table_schema = 'public' and table_name = $1 and column_name = $2",
        )
        .bind(table)
        .bind(column)
        .fetch_optional(pool)
        .await?
        .flatten();
        if collation.as_deref() != Some("C") {
            return Err(mismatch(format!(
                "column {table}.{column} lacks v4 C collation"
            )));
        }
    }
    Ok(())
}

#[cfg(feature = "migrations")]
pub(super) async fn postgres_v4_identifier_preflight(pool: &sqlx::PgPool) -> RepositoryResult<()> {
    // PostgreSQL text values are already validated as UTF-8 by the server.
    // Read every v3 value through the same Rust validator used by new writes so
    // Unicode edge whitespace, controls, and noncharacters cannot survive the
    // v4 constraint activation. The v3 catalog shape check runs first and
    // rejects a changed column type before this scan.
    for identifier in PERSISTED_IDENTIFIERS {
        postgres_scan_identifier(pool, *identifier).await?;
    }
    Ok(())
}

#[cfg(feature = "migrations")]
async fn postgres_scan_identifier(
    pool: &sqlx::PgPool,
    identifier: PersistedIdentifier,
) -> RepositoryResult<()> {
    use sqlx::Row;

    let query = format!(
        "select ctid::text as __keepsake_row, \"{}\" from \"{}\" order by ctid limit {IDENTIFIER_SCAN_BATCH_SIZE} offset $1",
        identifier.column, identifier.table
    );
    let mut offset: i64 = 0;

    loop {
        let rows = sqlx::query(sqlx::AssertSqlSafe(query.clone()))
            .bind(offset)
            .fetch_all(pool)
            .await?;
        if rows.is_empty() {
            break;
        }

        for row in rows {
            let row_locator: String = row.try_get("__keepsake_row")?;
            let value: Option<String> = row.try_get(identifier.column)?;
            let Some(value) = value else {
                return Err(persisted_identifier_type_mismatch(
                    identifier,
                    row_locator,
                    "NULL",
                ));
            };
            validate_persisted_identifier_bytes(identifier, row_locator, value.as_bytes())?;
        }
        offset = offset
            .checked_add(IDENTIFIER_SCAN_BATCH_SIZE)
            .ok_or_else(|| mismatch("identifier preflight row offset overflow"))?;
    }

    Ok(())
}
