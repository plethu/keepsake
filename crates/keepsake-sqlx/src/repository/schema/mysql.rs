//! `MySQL` and `MariaDB` schema verification.

use super::{RepositoryError, RepositoryResult, mismatch};
use crate::MySqlBackend;
use crate::repository::backend::KeepsakeSqlxBackend;

mod catalog;
mod columns;
mod constraints;
mod constraints_catalog;
mod identifiers;
mod indexes;
mod indexes_catalog;
#[cfg(feature = "migrations")]
mod legacy_constraints;
#[cfg(feature = "migrations")]
mod preflight;

#[cfg(feature = "migrations")]
use catalog::MYSQL_CLEAN_COLUMNS;
#[cfg(feature = "migrations")]
use catalog::MYSQL_LEGACY_COLUMNS;
use catalog::MYSQL_V3_TENANT_COLUMNS;
use catalog::MYSQL_V4_TENANT_COLUMNS;
use catalog::MySqlColumn;
use catalog::mysql_v3_clean_columns;
use columns::mysql_columns_check;
use constraints::mysql_v3_constraints_check;
use constraints::mysql_v3_foreign_keys_check;
use identifiers::mysql_v4_identifier_collation_check;
#[cfg(feature = "migrations")]
use indexes::mysql_indexes_check;
use indexes::mysql_v3_indexes_check;
#[cfg(feature = "migrations")]
use legacy_constraints::mysql_constraints_check;

#[cfg(feature = "migrations")]
async fn mysql_catalog_shape_check(
    pool: &sqlx::MySqlPool,
    activated_upgrade: bool,
) -> RepositoryResult<()> {
    let tables: &[&str] = if activated_upgrade {
        MYSQL_CATALOG_SHAPE_CHECK_TABLES_UPGRADE
    } else {
        MYSQL_CATALOG_SHAPE_CHECK_TABLES_CLEAN
    };

    let expected: Vec<MySqlColumn<'_>> = MYSQL_CLEAN_COLUMNS
        .iter()
        .chain(
            activated_upgrade
                .then_some(MYSQL_LEGACY_COLUMNS)
                .into_iter()
                .flatten(),
        )
        .copied()
        .collect();
    let table_count = sqlx::query_scalar::<_, i64>("select count(*) from information_schema.tables where table_schema = database() and table_name in (?,?,?,?,?,?,?,?)").bind(tables.first().unwrap_or(&"")).bind(tables.get(1).unwrap_or(&"")).bind(tables.get(2).unwrap_or(&"")).bind(tables.get(3).unwrap_or(&"")).bind(tables.get(4).unwrap_or(&"")).bind(tables.get(5).unwrap_or(&"")).bind(tables.get(6).unwrap_or(&"")).bind(tables.get(7).unwrap_or(&"")).fetch_one(pool).await?;
    if table_count != i64::try_from(tables.len()).unwrap_or(i64::MAX) {
        return Err(mismatch(format!(
            "expected {} domain tables, found {table_count}",
            tables.len()
        )));
    }

    if !activated_upgrade {
        let legacy = sqlx::query_scalar::<_, i64>("select count(*) from information_schema.tables where table_schema = database() and table_name in ('keepsake_audit_events','keepsake_audit_context_attributes','keepsake_audit_outbox')").fetch_one(pool).await?;
        if legacy != 0 {
            return Err(mismatch("clean track contains legacy audit tables"));
        }
    }

    let json_longtext_columns = mysql_columns_check(pool, &expected).await?;

    let column_count = sqlx::query_scalar::<_, i64>("select count(*) from information_schema.columns where table_schema = database() and table_name in (?,?,?,?,?,?,?,?)").bind(tables.first().unwrap_or(&"")).bind(tables.get(1).unwrap_or(&"")).bind(tables.get(2).unwrap_or(&"")).bind(tables.get(3).unwrap_or(&"")).bind(tables.get(4).unwrap_or(&"")).bind(tables.get(5).unwrap_or(&"")).bind(tables.get(6).unwrap_or(&"")).bind(tables.get(7).unwrap_or(&"")).fetch_one(pool).await?;
    if column_count != i64::try_from(expected.len()).unwrap_or(i64::MAX) {
        return Err(mismatch("domain tables contain unexpected columns"));
    }

    let server_version = sqlx::query_scalar::<_, String>("select version()")
        .fetch_one(pool)
        .await?;
    let maria_db = server_version.to_ascii_lowercase().contains("mariadb");
    mysql_constraints_check(pool, activated_upgrade, &json_longtext_columns, maria_db).await?;
    mysql_indexes_check(pool, activated_upgrade).await?;
    let trigger_count = sqlx::query_scalar::<_, i64>("select count(*) from information_schema.triggers where trigger_schema = database() and event_object_table in (?,?,?,?,?)").bind("keepsake_schema_metadata").bind("keepsake_relation_definitions").bind("keepsakes").bind("keepsake_fulfillment_counters").bind("keepsake_fulfillment_checklist").fetch_one(pool).await?;
    if trigger_count != 0 {
        return Err(mismatch(
            "unexpected trigger mutates a Keepsake invariant table",
        ));
    }
    Ok(())
}

#[cfg(feature = "migrations")]
pub(in crate::repository) async fn mysql_upgrade_schema_check(
    pool: &sqlx::MySqlPool,
) -> RepositoryResult<()> {
    mysql_catalog_shape_check(pool, true).await
}

async fn mysql_v3_domain_shape_check(
    pool: &sqlx::MySqlPool,
    identifier_contract: bool,
) -> RepositoryResult<()> {
    let expected_tables = MYSQL_V3_DOMAIN_SHAPE_CHECK_EXPECTED_TABLES;
    let v3_clean_columns = mysql_v3_clean_columns();
    let tenant_columns = if identifier_contract {
        MYSQL_V4_TENANT_COLUMNS
    } else {
        MYSQL_V3_TENANT_COLUMNS
    };
    let expected: Vec<MySqlColumn<'_>> = v3_clean_columns
        .iter()
        .chain(tenant_columns.iter())
        .copied()
        .collect();
    let json_longtext_columns = mysql_columns_check(pool, &expected).await?;
    let column_count = sqlx::query_scalar::<_, i64>("select count(*) from information_schema.columns where table_schema = database() and table_name in (?,?,?,?,?,?,?,?)")
        .bind(expected_tables.first().unwrap_or(&""))
        .bind(expected_tables.get(1).unwrap_or(&""))
        .bind(expected_tables.get(2).unwrap_or(&""))
        .bind(expected_tables.get(3).unwrap_or(&""))
        .bind(expected_tables.get(4).unwrap_or(&""))
        .bind(expected_tables.get(5).unwrap_or(&""))
        .bind(expected_tables.get(6).unwrap_or(&""))
        .bind(expected_tables.get(7).unwrap_or(&""))
        .fetch_one(pool)
        .await?;
    if column_count != i64::try_from(expected.len()).unwrap_or(i64::MAX) {
        return Err(mismatch("domain tables contain unexpected columns"));
    }

    let server_version = sqlx::query_scalar::<_, String>("select version()")
        .fetch_one(pool)
        .await?;
    // Validate key topology before CHECK expressions so a partially damaged
    // catalog reports the actionable PK/index/FK drift directly.
    mysql_v3_indexes_check(pool).await?;
    mysql_v3_foreign_keys_check(pool).await?;
    mysql_v3_constraints_check(
        pool,
        &json_longtext_columns,
        server_version.to_ascii_lowercase().contains("mariadb"),
        identifier_contract,
    )
    .await?;

    if identifier_contract {
        mysql_v4_identifier_collation_check(pool).await?;
    }

    let tenant_columns = sqlx::query_scalar::<_, i64>(
        "select count(*) from information_schema.columns where table_schema = database() and column_name = 'tenant_id' and is_nullable = 'NO' and table_name in (?,?,?,?)",
    )
    .bind("keepsake_relation_definitions")
    .bind("keepsakes")
    .bind("keepsake_fulfillment_counters")
    .bind("keepsake_fulfillment_checklist")
    .fetch_one(pool)
    .await?;
    if tenant_columns != 4 {
        return Err(mismatch(format!(
            "v3 schema has {tenant_columns} of 4 tenant_id columns"
        )));
    }

    let tenant_checks = sqlx::query_scalar::<_, i64>(
        "select count(*) from information_schema.table_constraints where constraint_schema = database() and constraint_type = 'CHECK' and constraint_name in (?,?,?,?,?,?,?,?)",
    )
    .bind("keepsake_relation_definitions_tenant_size")
    .bind("keepsake_relation_definitions_tenant_nonempty")
    .bind("keepsakes_tenant_size")
    .bind("keepsakes_tenant_nonempty")
    .bind("keepsake_fulfillment_counter_tenant_size")
    .bind("keepsake_fulfillment_counter_tenant_nonempty")
    .bind("keepsake_fulfillment_checklist_tenant_size")
    .bind("keepsake_fulfillment_checklist_tenant_nonempty")
    .fetch_one(pool)
    .await?;
    if tenant_checks != 8 {
        return Err(mismatch("v3 tenant size/non-empty checks are incomplete"));
    }

    Ok(())
}

pub(in crate::repository) async fn mysql_runtime_schema_check(
    pool: &sqlx::MySqlPool,
) -> RepositoryResult<()> {
    let metadata_exists = sqlx::query_scalar::<_, i64>(
        "select count(*) from information_schema.tables where table_schema = database() and table_name = 'keepsake_schema_metadata'",
    )
    .fetch_one(pool)
    .await?;
    if metadata_exists == 0 {
        return Err(mismatch("missing Keepsake schema metadata table"));
    }

    let backend = sqlx::query_scalar::<_, Option<String>>(
        "select value from keepsake_schema_metadata where `key` = 'backend'",
    )
    .fetch_optional(pool)
    .await?
    .flatten();
    if backend.as_deref() != Some(MySqlBackend::NAME) {
        return Err(mismatch(format!(
            "missing or incorrect MySQL backend marker: {backend:?}"
        )));
    }

    let track = sqlx::query_scalar::<_, Option<String>>(
        "select value from keepsake_schema_metadata where `key` = 'api_track'",
    )
    .fetch_optional(pool)
    .await?
    .flatten();
    if track.as_deref() == Some("4") {
        mysql_v3_domain_shape_check(pool, true).await?;
        return Ok(());
    }

    if track.as_deref() == Some("3") {
        return Err(RepositoryError::BackendMismatch {
            expected: "4.0 active schema",
            actual: "schema is still on the 3.0 API track; run migrate to activate the 4.0 schema"
                .to_owned(),
        });
    }

    if track.as_deref() == Some("2") {
        return Err(RepositoryError::BackendMismatch {
            expected: "4.0 active schema",
            actual: "schema is still on the 2.0 API track; run the explicit tenant upgrade route"
                .to_owned(),
        });
    }
    Err(RepositoryError::BackendMismatch {
        expected: "4.0 active schema",
        actual: "schema is not activated for the 4.0 API".to_owned(),
    })
}

#[cfg(test)]
pub(super) use columns::mysql_default_matches;
#[cfg(test)]
pub(super) use columns::mysql_is_generated_extra;
#[cfg(test)]
pub(super) use constraints::mysql_v3_referential_action_matches;
#[cfg(feature = "migrations")]
pub(in crate::repository) use preflight::mysql_clean_schema_preflight;
#[cfg(feature = "migrations")]
pub(in crate::repository) use preflight::mysql_upgrade_schema_preflight;

#[cfg(feature = "migrations")]
const MYSQL_CATALOG_SHAPE_CHECK_TABLES_CLEAN: &[&str] = &[
    "keepsake_schema_metadata",
    "keepsake_relation_definitions",
    "keepsakes",
    "keepsake_fulfillment_counters",
    "keepsake_fulfillment_checklist",
];

#[cfg(feature = "migrations")]
const MYSQL_CATALOG_SHAPE_CHECK_TABLES_UPGRADE: &[&str] = &[
    "keepsake_schema_metadata",
    "keepsake_relation_definitions",
    "keepsakes",
    "keepsake_fulfillment_counters",
    "keepsake_fulfillment_checklist",
    "keepsake_audit_events",
    "keepsake_audit_context_attributes",
    "keepsake_audit_outbox",
];

const MYSQL_V3_DOMAIN_SHAPE_CHECK_EXPECTED_TABLES: &[&str] = &[
    "keepsake_schema_metadata",
    "keepsake_relation_definitions",
    "keepsakes",
    "keepsake_fulfillment_counters",
    "keepsake_fulfillment_checklist",
];
