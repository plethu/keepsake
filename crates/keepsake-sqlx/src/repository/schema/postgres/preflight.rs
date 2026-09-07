//! `PostgreSQL` migration preflight and historical track eligibility.

#[cfg(feature = "migrations")]
use super::identifiers::postgres_v4_identifier_preflight;
#[cfg(feature = "migrations")]
use super::postgres_catalog_shape_check;
#[cfg(feature = "migrations")]
use super::postgres_v3_runtime_schema_check;
use super::postgres_v4_runtime_schema_check;
#[cfg(feature = "migrations")]
use crate::PostgresBackend;
#[cfg(feature = "migrations")]
use crate::repository::backend::KeepsakeSqlxBackend;
use crate::repository::schema::RepositoryError;
use crate::repository::schema::RepositoryResult;

#[cfg(feature = "migrations")]
pub(in crate::repository) async fn postgres_clean_schema_preflight(
    pool: &sqlx::PgPool,
) -> RepositoryResult<()> {
    let has_domain = sqlx::query_scalar::<_, bool>(
        "select to_regclass('public.keepsake_relation_definitions') is not null",
    )
    .fetch_one(pool)
    .await?;
    if !has_domain {
        return postgres_schema_preflight(pool).await;
    }

    let track = sqlx::query_scalar::<_, Option<String>>(
        "select value from keepsake_schema_metadata where key = 'api_track'",
    )
    .fetch_optional(pool)
    .await?
    .flatten();
    match track.as_deref() {
        Some("4") => postgres_v4_runtime_schema_check(pool).await,
        Some("3") => {
            postgres_v3_runtime_schema_check(pool).await?;
            postgres_v4_identifier_preflight(pool).await
        }
        Some("2") => {
            let legacy = sqlx::query_scalar::<_, bool>(
                "select to_regclass('public.keepsake_audit_events') is not null",
            )
            .fetch_one(pool)
            .await?;
            if legacy {
                return Err(RepositoryError::BackendMismatch {
                    expected: "2.0 clean track",
                    actual: "activated upgrade track".to_owned(),
                });
            }
            postgres_catalog_shape_check(pool, false).await
        }
        Some(actual) => Err(RepositoryError::BackendMismatch {
            expected: "4.0 clean track",
            actual: actual.to_owned(),
        }),
        None => Err(RepositoryError::BackendMismatch {
            expected: "4.0 clean track",
            actual: "legacy schema; call upgrade_migrate".to_owned(),
        }),
    }
}

#[cfg(feature = "migrations")]
pub(in crate::repository) async fn postgres_upgrade_schema_preflight(
    pool: &sqlx::PgPool,
) -> RepositoryResult<()> {
    let metadata = sqlx::query_scalar::<_, Option<String>>(
        "select to_regclass('public.keepsake_schema_metadata')::text",
    )
    .fetch_one(pool)
    .await?;
    let has_v2 = if metadata.is_some() {
        sqlx::query_scalar::<_, bool>(
            "select exists (select 1 from keepsake_schema_metadata where key = 'api_track' and value = '2')",
        )
        .fetch_one(pool)
        .await?
    } else {
        false
    };

    if has_v2 {
        return Err(RepositoryError::BackendMismatch {
            expected: "legacy upgrade track",
            actual: "2.0 clean track".to_owned(),
        });
    }

    let has_v3 = if metadata.is_some() {
        sqlx::query_scalar::<_, bool>(
            "select exists (select 1 from keepsake_schema_metadata where key = 'api_track' and value = '3')",
        )
        .fetch_one(pool)
        .await?
    } else {
        false
    };

    if has_v3 {
        return Err(RepositoryError::BackendMismatch {
            expected: "legacy upgrade track",
            actual: "3.0 clean track".to_owned(),
        });
    }
    postgres_schema_preflight(pool).await
}

#[cfg(feature = "migrations")]
async fn postgres_schema_preflight(pool: &sqlx::PgPool) -> RepositoryResult<()> {
    let metadata = sqlx::query_scalar::<_, Option<String>>(
        "select to_regclass('public.keepsake_schema_metadata')::text",
    )
    .fetch_one(pool)
    .await?;
    if metadata.is_none() {
        return postgres_unmarked_schema_preflight(pool).await;
    }

    let backend = sqlx::query_scalar::<_, Option<String>>(
        "select value from keepsake_schema_metadata where key = 'backend'",
    )
    .fetch_one(pool)
    .await?;
    match backend.as_deref() {
        Some(PostgresBackend::NAME) | None => Ok(()),
        Some(actual) => Err(RepositoryError::BackendMismatch {
            expected: PostgresBackend::NAME,
            actual: actual.to_owned(),
        }),
    }
}

#[cfg(feature = "migrations")]
async fn postgres_unmarked_schema_preflight(pool: &sqlx::PgPool) -> RepositoryResult<()> {
    let count = sqlx::query_scalar::<_, i64>("select count(*) from information_schema.tables where table_schema = 'public' and table_type = 'BASE TABLE'").fetch_one(pool).await?;
    if count == 0 {
        return Ok(());
    }

    let known = sqlx::query_scalar::<_, bool>("select to_regclass('public.keepsake_relation_definitions') is not null and to_regclass('public.keepsakes') is not null and to_regclass('public._sqlx_migrations') is not null").fetch_one(pool).await?;
    if !known {
        return Err(RepositoryError::BackendMismatch {
            expected: PostgresBackend::NAME,
            actual: "unmarked non-empty schema".to_owned(),
        });
    }

    let migrations = sqlx::query_scalar::<_, i64>(
        "select count(*) from _sqlx_migrations where version in (1,2)",
    )
    .fetch_one(pool)
    .await?;
    if migrations == 2 {
        Ok(())
    } else {
        Err(RepositoryError::BackendMismatch {
            expected: PostgresBackend::NAME,
            actual: "unmarked unknown migration history".to_owned(),
        })
    }
}
