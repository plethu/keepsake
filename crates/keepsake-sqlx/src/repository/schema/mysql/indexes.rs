//! `MySQL` and `MariaDB` index columns, uniqueness, and predicate checks.

#[cfg(feature = "migrations")]
use super::indexes_catalog::LEGACY_AUDIT_INDEXES;
#[cfg(feature = "migrations")]
use super::indexes_catalog::LEGACY_INDEXES;
use super::indexes_catalog::MYSQL_V3_INDEXES_CHECK_EXPECTED;
use crate::repository::schema::RepositoryResult;
use crate::repository::schema::mismatch;

#[cfg(feature = "migrations")]
pub(super) async fn mysql_indexes_check(
    pool: &sqlx::MySqlPool,
    activated_upgrade: bool,
) -> RepositoryResult<()> {
    for &(name, table, unique, columns) in LEGACY_INDEXES {
        let name = if activated_upgrade && name == "keepsake_relation_definitions_kind_key_unique" {
            "kind"
        } else {
            name
        };
        mysql_legacy_index_check(pool, table, name, unique, columns).await?;
    }

    if activated_upgrade {
        for &(name, table, unique, columns) in LEGACY_AUDIT_INDEXES {
            mysql_legacy_index_check(pool, table, name, unique, columns).await?;
        }
    }

    Ok(())
}

#[cfg(feature = "migrations")]
async fn mysql_legacy_index_check(
    pool: &sqlx::MySqlPool,
    table: &str,
    name: &str,
    unique: bool,
    columns: &[&str],
) -> RepositoryResult<()> {
    use sqlx::Row;
    let rows = sqlx::query("select non_unique as non_unique, column_name as column_name, sub_part as sub_part from information_schema.statistics where table_schema = database() and table_name = ? and index_name = ? order by seq_in_index")
        .bind(table).bind(name).fetch_all(pool).await?;
    if rows.len() != columns.len() {
        return Err(mismatch(format!("index {name} column count differs")));
    }

    for (row, &column) in rows.iter().zip(columns) {
        let non_unique: i64 = row.try_get("non_unique")?;
        let actual: Option<String> = row.try_get("column_name")?;
        let sub_part: Option<i64> = row.try_get("sub_part")?;
        let expected_sub_part =
            (name == "keepsake_audit_context_attribute_lookup" && column == "value").then_some(191);
        if (non_unique == 0) != unique
            || actual.as_deref() != Some(column)
            || sub_part != expected_sub_part
        {
            return Err(mismatch(format!(
                "index {name} columns or uniqueness differ"
            )));
        }
    }

    Ok(())
}

pub(super) async fn mysql_v3_indexes_check(pool: &sqlx::MySqlPool) -> RepositoryResult<()> {
    use sqlx::Row;

    let expected = MYSQL_V3_INDEXES_CHECK_EXPECTED;

    let names = sqlx::query_as::<_, (String, String)>(
        "select distinct table_name, index_name from information_schema.statistics where table_schema = database() and table_name in (?,?,?,?)",
    )
    .bind("keepsake_relation_definitions")
    .bind("keepsakes")
    .bind("keepsake_fulfillment_counters")
    .bind("keepsake_fulfillment_checklist")
    .fetch_all(pool)
    .await?;
    if names.len() != expected.len()
        || names.iter().any(|(table, name)| {
            !expected
                .iter()
                .any(|(expected_table, expected_name, _, _)| {
                    *expected_table == table && *expected_name == name
                })
        })
    {
        return Err(mismatch(format!("v3 domain index set differs: {names:?}")));
    }

    for (table, name, unique, columns) in expected {
        let rows = sqlx::query(
            "select non_unique as non_unique, cast(seq_in_index as signed) as seq_in_index, column_name as column_name, sub_part as sub_part from information_schema.statistics where table_schema = database() and table_name = ? and index_name = ? order by seq_in_index",
        )
        .bind(table)
        .bind(name)
        .fetch_all(pool)
        .await?;
        if rows.len() != columns.len() {
            return Err(mismatch(format!(
                "index {table}.{name} column count differs"
            )));
        }

        for ((row, expected_column), position) in rows.iter().zip(columns.iter()).zip(1_i64..) {
            let non_unique: i64 = row.try_get("non_unique")?;
            let seq_in_index: i64 = row.try_get("seq_in_index")?;
            let column: String = row.try_get("column_name")?;
            let sub_part: Option<i64> = row.try_get("sub_part")?;
            if seq_in_index != position
                || (non_unique == 0) != *unique
                || column != *expected_column
                || sub_part.is_some()
            {
                return Err(mismatch(format!(
                    "index {table}.{name} columns or uniqueness differ"
                )));
            }
        }
    }

    Ok(())
}
