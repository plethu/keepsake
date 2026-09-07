//! `MySQL` and `MariaDB` identifier storage validation and bounded migration scans.

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

pub(super) async fn mysql_v4_identifier_collation_check(
    pool: &sqlx::MySqlPool,
) -> RepositoryResult<()> {
    let expected = [
        ("keepsake_relation_definitions", "kind"),
        ("keepsake_relation_definitions", "key"),
        ("keepsake_relation_definitions", "tenant_id"),
        ("keepsakes", "tenant_id"),
        ("keepsakes", "subject_kind"),
        ("keepsakes", "subject_id"),
        ("keepsake_fulfillment_counters", "tenant_id"),
        ("keepsake_fulfillment_counters", "key"),
        ("keepsake_fulfillment_checklist", "tenant_id"),
        ("keepsake_fulfillment_checklist", "item"),
    ];
    for (table, column) in expected {
        let (character_set, collation) = sqlx::query_as::<_, (Option<String>, Option<String>)>(
            "select character_set_name, collation_name from information_schema.columns where table_schema = database() and table_name = ? and column_name = ?",
        )
        .bind(table)
        .bind(column)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| mismatch(format!("missing column {table}.{column}")))?;
        if character_set.as_deref() != Some("utf8mb4")
            || collation.as_deref() != Some("utf8mb4_bin")
        {
            return Err(mismatch(format!(
                "column {table}.{column} lacks explicit utf8mb4_bin collation"
            )));
        }
    }
    Ok(())
}

#[cfg(feature = "migrations")]
pub(super) async fn mysql_v4_identifier_preflight(pool: &sqlx::MySqlPool) -> RepositoryResult<()> {
    // The v4 ALTER changes the v3 tenant VARBINARY(255) columns to textual
    // utf8mb4 columns and adds <=191-byte checks. Read the original bytes
    // before that conversion and apply the exact Rust contract, including
    // invalid UTF-8 and Unicode edge cases that SQL TRIM cannot express.
    for identifier in PERSISTED_IDENTIFIERS {
        mysql_scan_identifier(pool, *identifier).await?;
    }
    Ok(())
}

#[cfg(feature = "migrations")]
async fn mysql_scan_identifier(
    pool: &sqlx::MySqlPool,
    identifier: PersistedIdentifier,
) -> RepositoryResult<()> {
    use sqlx::Row;

    let character_set = sqlx::query_scalar::<_, Option<String>>(
        "select character_set_name from information_schema.columns where table_schema = database() and table_name = ? and column_name = ?",
    )
    .bind(identifier.table)
    .bind(identifier.column)
    .fetch_optional(pool)
    .await?
    .flatten();
    // v3 tenant_id is VARBINARY and therefore carries the original UTF-8
    // bytes directly. The other identifier columns are VARCHAR; convert
    // them using their declared source charset before validating UTF-8 so
    // a latin1 v3 database is migrated according to MySQL's own ALTER
    // conversion rather than being mistaken for corrupt UTF-8.
    let value_expression = match character_set.as_deref() {
        None | Some("binary") => format!("cast(`{}` as binary)", identifier.column),
        Some(_) => format!(
            "cast(convert(`{}` using utf8mb4) as binary)",
            identifier.column
        ),
    };

    let query = format!(
        "select {value_expression} as __keepsake_value from `{}` order by {} limit {IDENTIFIER_SCAN_BATCH_SIZE} offset ?",
        identifier.table,
        mysql_identifier_order(identifier.table)?,
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

        for (row_number, row) in rows.into_iter().enumerate() {
            let row_label = i64::try_from(row_number)
                .ok()
                .and_then(|row| offset.checked_add(row))
                .and_then(|row| row.checked_add(1))
                .ok_or_else(|| mismatch("identifier preflight row offset overflow"))?;
            let value: Option<Vec<u8>> = row.try_get("__keepsake_value")?;
            let Some(value) = value else {
                return Err(persisted_identifier_type_mismatch(
                    identifier, row_label, "NULL",
                ));
            };

            validate_persisted_identifier_bytes(identifier, row_label, &value)?;
        }
        offset = offset
            .checked_add(IDENTIFIER_SCAN_BATCH_SIZE)
            .ok_or_else(|| mismatch("identifier preflight row offset overflow"))?;
    }

    Ok(())
}

#[cfg(feature = "migrations")]
fn mysql_identifier_order(table: &str) -> RepositoryResult<&'static str> {
    let order = match table {
        "keepsake_relation_definitions" | "keepsakes" => "`tenant_id`, `id`",
        "keepsake_fulfillment_counters" => "`tenant_id`, `keepsake_id`, `key`",
        "keepsake_fulfillment_checklist" => "`tenant_id`, `keepsake_id`, `item`",
        _ => return Err(mismatch(format!("unknown identifier table {table}"))),
    };
    Ok(order)
}
