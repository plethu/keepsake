//! `PostgreSQL` column type, default, and generation checks.

use super::catalog::PG_CLEAN_COLUMNS;
use crate::repository::schema::RepositoryResult;
use crate::repository::schema::default_sql;
use crate::repository::schema::mismatch;
use crate::repository::schema::normalize_sql;

pub(super) fn pg_default_matches(
    actual: Option<&str>,
    expected: Option<&str>,
    sequence: bool,
) -> bool {
    match (actual, expected, sequence) {
        (Some(actual), Some("nextval") | None, true) => {
            normalize_sql(actual).starts_with("nextval(")
        }
        (Some(actual), Some(expected), false) => {
            let actual = normalize_sql(actual);
            let expected = normalize_sql(expected);
            let actual = actual.strip_suffix("::text").unwrap_or(&actual);
            default_sql(actual) == default_sql(&expected)
        }
        (None, None, false) => true,
        _ => false,
    }
}

pub(super) async fn postgres_v3_columns_check(pool: &sqlx::PgPool) -> RepositoryResult<()> {
    use sqlx::Row;

    // The v3 tables retain the v2 column types and add one tenant_id column to
    // each relation-owned table. Reuse the v2 catalog contract for the stable
    // columns, then verify the tenant columns separately because their C
    // collation is part of the identity-isolation contract.
    for expected in PG_CLEAN_COLUMNS {
        let row = sqlx::query(
            "select data_type, udt_name, is_nullable, column_default, is_identity, is_generated, generation_expression from information_schema.columns where table_schema = 'public' and table_name = $1 and column_name = $2",
        )
        .bind(expected.table)
        .bind(expected.name)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| mismatch(format!("missing column {}.{}", expected.table, expected.name)))?;
        let data_type: String = row.try_get("data_type")?;
        let udt_name: String = row.try_get("udt_name")?;
        let nullable: String = row.try_get("is_nullable")?;
        let default: Option<String> = row.try_get("column_default")?;
        let identity: String = row.try_get("is_identity")?;
        let generated: String = row.try_get("is_generated")?;
        let generation_expression: Option<String> = row.try_get("generation_expression")?;
        if data_type != expected.data_type
            || udt_name != expected.udt_name
            || (nullable == "YES") != expected.nullable
            || identity != "NO"
            || generated != "NEVER"
            || generation_expression.is_some()
            || !pg_default_matches(default.as_deref(), expected.default, expected.sequence)
        {
            return Err(mismatch(format!(
                "column {}.{} has unexpected v3 catalog semantics",
                expected.table, expected.name
            )));
        }
    }

    for table in [
        "keepsake_relation_definitions",
        "keepsakes",
        "keepsake_fulfillment_counters",
        "keepsake_fulfillment_checklist",
    ] {
        let row = sqlx::query(
            "select data_type, udt_name, is_nullable, column_default, collation_name, is_identity, is_generated, generation_expression from information_schema.columns where table_schema = 'public' and table_name = $1 and column_name = 'tenant_id'",
        )
        .bind(table)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| mismatch(format!("missing column {table}.tenant_id")))?;
        let data_type: String = row.try_get("data_type")?;
        let udt_name: String = row.try_get("udt_name")?;
        let nullable: String = row.try_get("is_nullable")?;
        let default: Option<String> = row.try_get("column_default")?;
        let collation: Option<String> = row.try_get("collation_name")?;
        let identity: String = row.try_get("is_identity")?;
        let generated: String = row.try_get("is_generated")?;
        let generation_expression: Option<String> = row.try_get("generation_expression")?;
        if data_type != "text"
            || udt_name != "text"
            || nullable != "NO"
            || default.is_some()
            || collation.as_deref() != Some("C")
            || identity != "NO"
            || generated != "NEVER"
            || generation_expression.is_some()
        {
            return Err(mismatch(format!(
                "column {table}.tenant_id has unexpected v3 catalog semantics"
            )));
        }
    }

    let column_count = sqlx::query_scalar::<_, i64>(
        "select count(*) from information_schema.columns where table_schema = 'public' and table_name = any($1)",
    )
    .bind([
        "keepsake_schema_metadata",
        "keepsake_relation_definitions",
        "keepsakes",
        "keepsake_fulfillment_counters",
        "keepsake_fulfillment_checklist",
    ])
    .fetch_one(pool)
    .await?;
    let expected_count = PG_CLEAN_COLUMNS
        .len()
        .checked_add(4)
        .and_then(|count| i64::try_from(count).ok())
        .ok_or_else(|| mismatch("expected column count exceeds catalog limits"))?;
    if column_count != expected_count {
        return Err(mismatch("v3 domain tables contain unexpected columns"));
    }
    Ok(())
}
