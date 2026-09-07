//! `PostgreSQL` schema verification.

#[cfg(feature = "postgres")]
use super::{RepositoryError, RepositoryResult, mismatch};
#[cfg(feature = "postgres")]
use crate::PostgresBackend;
#[cfg(feature = "postgres")]
use crate::repository::backend::KeepsakeSqlxBackend;

#[cfg(feature = "postgres")]
mod catalog;
#[cfg(feature = "postgres")]
mod columns;
mod constraints;
#[cfg(feature = "postgres")]
mod constraints_catalog;
#[cfg(feature = "postgres")]
mod identifiers;
#[cfg(feature = "postgres")]
mod indexes;
#[cfg(feature = "postgres")]
mod indexes_catalog;
#[cfg(all(feature = "postgres", feature = "migrations"))]
mod preflight;

#[cfg(all(feature = "postgres", feature = "migrations"))]
use catalog::PG_CLEAN_COLUMNS;
#[cfg(all(feature = "postgres", feature = "migrations"))]
use catalog::PG_LEGACY_COLUMNS;
#[cfg(all(feature = "postgres", feature = "migrations"))]
use catalog::PgColumn;
#[cfg(all(feature = "postgres", feature = "migrations"))]
use columns::pg_default_matches;
#[cfg(feature = "postgres")]
use columns::postgres_v3_columns_check;
#[cfg(all(feature = "postgres", feature = "migrations"))]
use constraints::pg_constraints_check;
#[cfg(feature = "postgres")]
use constraints::postgres_v3_constraints_check;
#[cfg(feature = "postgres")]
use identifiers::postgres_v4_identifier_columns_check;
#[cfg(all(feature = "postgres", feature = "migrations"))]
use indexes::pg_indexes_check;
#[cfg(feature = "postgres")]
use indexes::postgres_v3_indexes_check;

#[cfg(all(feature = "postgres", feature = "migrations"))]
async fn postgres_catalog_shape_check(
    pool: &sqlx::PgPool,
    activated_upgrade: bool,
) -> RepositoryResult<()> {
    let expected_tables: &[&str] = if activated_upgrade {
        POSTGRES_CATALOG_SHAPE_CHECK_EXPECTED_TABLES_UPGRADE
    } else {
        POSTGRES_CATALOG_SHAPE_CHECK_EXPECTED_TABLES_CLEAN
    };
    let expected_columns: Vec<PgColumn<'_>> = PG_CLEAN_COLUMNS
        .iter()
        .chain(
            activated_upgrade
                .then_some(PG_LEGACY_COLUMNS)
                .into_iter()
                .flatten(),
        )
        .copied()
        .collect();

    let table_count = sqlx::query_scalar::<_, i64>(
        "select count(*) from information_schema.tables where table_schema = 'public' and table_type = 'BASE TABLE' and table_name = any($1)",
    )
    .bind(expected_tables)
    .fetch_one(pool)
    .await?;
    if table_count != i64::try_from(expected_tables.len()).unwrap_or(i64::MAX) {
        return Err(mismatch(format!(
            "expected {} domain tables, found {table_count}",
            expected_tables.len()
        )));
    }

    if !activated_upgrade {
        let legacy_count = sqlx::query_scalar::<_, i64>(
            "select count(*) from information_schema.tables where table_schema = 'public' and table_name in ('keepsake_audit_events','keepsake_audit_context_attributes','keepsake_audit_outbox')",
        )
        .fetch_one(pool)
        .await?;
        if legacy_count != 0 {
            return Err(mismatch("clean track contains legacy audit tables"));
        }
    }

    postgres_legacy_columns_check(pool, &expected_columns).await?;

    let column_count = sqlx::query_scalar::<_, i64>(
        "select count(*) from information_schema.columns where table_schema = 'public' and table_name = any($1)",
    )
    .bind(expected_tables)
    .fetch_one(pool)
    .await?;
    if column_count != i64::try_from(expected_columns.len()).unwrap_or(i64::MAX) {
        return Err(mismatch("domain tables contain unexpected columns"));
    }

    pg_constraints_check(pool, activated_upgrade).await?;
    pg_indexes_check(pool, activated_upgrade).await?;
    let trigger_count = sqlx::query_scalar::<_, i64>(
        "select count(*) from pg_trigger t join pg_class c on c.oid = t.tgrelid join pg_namespace n on n.oid = c.relnamespace where not t.tgisinternal and n.nspname = 'public' and c.relname = any($1)",
    )
    .bind(expected_tables)
    .fetch_one(pool)
    .await?;
    if trigger_count != 0 {
        return Err(mismatch(
            "unexpected trigger mutates a Keepsake invariant table",
        ));
    }
    Ok(())
}

#[cfg(all(feature = "postgres", feature = "migrations"))]
pub(in crate::repository) async fn postgres_upgrade_schema_check(
    pool: &sqlx::PgPool,
) -> RepositoryResult<()> {
    postgres_catalog_shape_check(pool, true).await
}

#[cfg(feature = "postgres")]
pub(in crate::repository) async fn postgres_runtime_schema_check(
    pool: &sqlx::PgPool,
) -> RepositoryResult<()> {
    let metadata_exists = sqlx::query_scalar::<_, bool>(
        "select to_regclass('public.keepsake_schema_metadata') is not null",
    )
    .fetch_one(pool)
    .await?;
    if !metadata_exists {
        return Err(mismatch("missing Keepsake schema metadata table"));
    }

    let backend = sqlx::query_scalar::<_, Option<String>>(
        "select value from keepsake_schema_metadata where key = 'backend'",
    )
    .fetch_optional(pool)
    .await?
    .flatten();
    if backend.as_deref() != Some(PostgresBackend::NAME) {
        return Err(mismatch(format!(
            "missing or incorrect PostgreSQL backend marker: {backend:?}"
        )));
    }

    let track = sqlx::query_scalar::<_, Option<String>>(
        "select value from keepsake_schema_metadata where key = 'api_track'",
    )
    .fetch_optional(pool)
    .await?
    .flatten();
    match track.as_deref() {
        Some("4") => postgres_v4_runtime_schema_check(pool).await,
        Some("3") => Err(RepositoryError::BackendMismatch {
            expected: "4.0 active schema",
            actual: "schema is still on the 3.0 API track; run migrate to activate the 4.0 schema"
                .to_owned(),
        }),
        Some("2") => Err(RepositoryError::BackendMismatch {
            expected: "4.0 active schema",
            actual: "schema is still on the 2.0 API track; run the explicit tenant upgrade route"
                .to_owned(),
        }),
        Some(actual) => Err(RepositoryError::BackendMismatch {
            expected: "4.0 active schema",
            actual: format!("unsupported Keepsake API track {actual}"),
        }),
        None => Err(RepositoryError::BackendMismatch {
            expected: "4.0 active schema",
            actual: "schema is not activated for the 4.0 API".to_owned(),
        }),
    }
}

#[cfg(all(feature = "postgres", feature = "migrations"))]
async fn postgres_v3_runtime_schema_check(pool: &sqlx::PgPool) -> RepositoryResult<()> {
    postgres_runtime_schema_check_for_track(pool, false).await
}

#[cfg(feature = "postgres")]
async fn postgres_v4_runtime_schema_check(pool: &sqlx::PgPool) -> RepositoryResult<()> {
    postgres_runtime_schema_check_for_track(pool, true).await
}

#[cfg(feature = "postgres")]
async fn postgres_runtime_schema_check_for_track(
    pool: &sqlx::PgPool,
    identifier_contract: bool,
) -> RepositoryResult<()> {
    let expected_tables = POSTGRES_RUNTIME_SCHEMA_CHECK_FOR_TRACK_EXPECTED_TABLES;
    let table_count = sqlx::query_scalar::<_, i64>(
        "select count(*) from information_schema.tables where table_schema = 'public' and table_name = any($1)",
    )
    .bind(expected_tables)
    .fetch_one(pool)
    .await?;
    let tenant_column_count = sqlx::query_scalar::<_, i64>(
        "select count(*) from information_schema.columns where table_schema = 'public' and table_name = any($1) and column_name = 'tenant_id'",
    )
    .bind(expected_tables)
    .fetch_one(pool)
    .await?;
    let tenant_collation_count = sqlx::query_scalar::<_, i64>(
        "select count(*) from information_schema.columns where table_schema = 'public' and table_name = any($1) and column_name = 'tenant_id' and collation_name = 'C'",
    )
    .bind(expected_tables)
    .fetch_one(pool)
    .await?;
    if table_count != 4 || tenant_column_count != 4 || tenant_collation_count != 4 {
        return Err(RepositoryError::BackendMismatch {
            expected: "complete Keepsake 3.0 tenant-aware PostgreSQL schema",
            actual: "missing tenant-owned Keepsake table, column, or C collation".to_owned(),
        });
    }

    postgres_v3_columns_check(pool).await?;
    if identifier_contract {
        postgres_v4_identifier_columns_check(pool).await?;
    }

    let indexes = sqlx::query_scalar::<_, i64>(
        "select count(*) from pg_indexes where schemaname = 'public' and indexname = any($1)",
    )
    .bind([
        "keepsakes_one_active_relation_per_subject",
        "keepsakes_active_subject_lookup",
        "keepsakes_active_relation_membership",
        "keepsakes_due_timed_expiry",
        "keepsakes_due_fulfilled_expiry",
        "keepsake_fulfillment_counter_scan",
        "keepsake_fulfillment_checklist_scan",
    ])
    .fetch_one(pool)
    .await?;
    if indexes != 7 {
        return Err(RepositoryError::BackendMismatch {
            expected: "tenant-leading Keepsake 3.0 PostgreSQL indexes",
            actual: "one or more tenant-aware indexes are missing".to_owned(),
        });
    }
    postgres_v3_constraints_check(pool, identifier_contract).await?;
    postgres_v3_indexes_check(pool).await?;
    Ok(())
}

#[cfg(feature = "mysql")]
pub(super) use constraints::mysql_catalog_check_matches;
#[cfg(feature = "postgres")]
#[cfg(feature = "migrations")]
pub(in crate::repository) use preflight::postgres_clean_schema_preflight;
#[cfg(feature = "postgres")]
#[cfg(feature = "migrations")]
pub(in crate::repository) use preflight::postgres_upgrade_schema_preflight;

#[cfg(all(feature = "postgres", feature = "migrations"))]
const POSTGRES_CATALOG_SHAPE_CHECK_EXPECTED_TABLES_CLEAN: &[&str] = &[
    "keepsake_schema_metadata",
    "keepsake_relation_definitions",
    "keepsakes",
    "keepsake_fulfillment_counters",
    "keepsake_fulfillment_checklist",
];

#[cfg(all(feature = "postgres", feature = "migrations"))]
const POSTGRES_CATALOG_SHAPE_CHECK_EXPECTED_TABLES_UPGRADE: &[&str] = &[
    "keepsake_schema_metadata",
    "keepsake_relation_definitions",
    "keepsakes",
    "keepsake_fulfillment_counters",
    "keepsake_fulfillment_checklist",
    "keepsake_audit_events",
    "keepsake_audit_context_attributes",
    "keepsake_audit_outbox",
];

#[cfg(feature = "postgres")]
const POSTGRES_RUNTIME_SCHEMA_CHECK_FOR_TRACK_EXPECTED_TABLES: &[&str] = &[
    "keepsake_relation_definitions",
    "keepsakes",
    "keepsake_fulfillment_counters",
    "keepsake_fulfillment_checklist",
];

#[cfg(all(feature = "postgres", feature = "migrations"))]
async fn postgres_legacy_columns_check(
    pool: &sqlx::PgPool,
    expected_columns: &[PgColumn<'_>],
) -> RepositoryResult<()> {
    use sqlx::Row;
    for expected in expected_columns {
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
        let default_matches =
            pg_default_matches(default.as_deref(), expected.default, expected.sequence);
        if data_type != expected.data_type
            || udt_name != expected.udt_name
            || (nullable == "YES") != expected.nullable
            || identity != "NO"
            || generated != "NEVER"
            || generation_expression.is_some()
            || !default_matches
        {
            return Err(mismatch(format!(
                "column {}.{} has unexpected catalog semantics: type actual={data_type:?}/{udt_name:?} expected={:?}/{:?}; nullable actual={nullable:?} expected={}; identity={identity:?}; generated={generated:?}; generation_present={}; default actual={default:?} expected={:?} sequence={} match={default_matches}",
                expected.table,
                expected.name,
                expected.data_type,
                expected.udt_name,
                expected.nullable,
                generation_expression.is_some(),
                expected.default,
                expected.sequence,
            )));
        }
    }

    Ok(())
}
