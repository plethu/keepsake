//! `MySQL` and `MariaDB` migration preflight and historical track eligibility.

#[cfg(feature = "migrations")]
use super::identifiers::mysql_v4_identifier_preflight;
use super::mysql_v3_domain_shape_check;
#[cfg(feature = "migrations")]
use crate::MySqlBackend;
#[cfg(feature = "migrations")]
use crate::repository::backend::KeepsakeSqlxBackend;
use crate::repository::schema::RepositoryError;
use crate::repository::schema::RepositoryResult;

#[cfg(feature = "migrations")]
pub(in crate::repository) async fn mysql_clean_schema_preflight(
    pool: &sqlx::MySqlPool,
) -> RepositoryResult<()> {
    let has_domain = sqlx::query_scalar::<_, Option<String>>("select table_name from information_schema.tables where table_schema = database() and table_name = 'keepsake_relation_definitions'").fetch_optional(pool).await?.flatten().is_some();
    if !has_domain {
        return mysql_schema_preflight(pool).await;
    }

    let track = sqlx::query_scalar::<_, Option<String>>(
        "select value from keepsake_schema_metadata where `key` = 'api_track'",
    )
    .fetch_optional(pool)
    .await?
    .flatten();
    match track.as_deref() {
        Some("4") => mysql_v3_domain_shape_check(pool, true).await,
        Some("3") => {
            mysql_v3_domain_shape_check(pool, false).await?;
            mysql_v4_identifier_preflight(pool).await
        }
        Some("2") => {
            let legacy = sqlx::query_scalar::<_, i64>("select count(*) from information_schema.tables where table_schema = database() and table_name = 'keepsake_audit_events'").fetch_one(pool).await? != 0;
            if legacy {
                return Err(RepositoryError::BackendMismatch {
                    expected: "2.0 clean track",
                    actual: "activated upgrade track".to_owned(),
                });
            }
            Err(RepositoryError::BackendMismatch {
                expected: "3.0 clean track",
                actual: "2.0 clean track; run the explicit tenant upgrade route".to_owned(),
            })
        }
        Some(actual) => Err(RepositoryError::BackendMismatch {
            expected: "3.0 clean track",
            actual: actual.to_owned(),
        }),
        None => Err(RepositoryError::BackendMismatch {
            expected: "3.0 clean track",
            actual: "legacy schema; call upgrade_migrate".to_owned(),
        }),
    }
}

#[cfg(feature = "migrations")]
pub(in crate::repository) async fn mysql_upgrade_schema_preflight(
    pool: &sqlx::MySqlPool,
) -> RepositoryResult<()> {
    let has_metadata = sqlx::query_scalar::<_, i64>("select count(*) from information_schema.tables where table_schema = database() and table_name = 'keepsake_schema_metadata'").fetch_one(pool).await? > 0;
    if !has_metadata {
        return mysql_schema_preflight(pool).await;
    }

    let has_v2 = sqlx::query_scalar::<_, Option<String>>(
        "select value from keepsake_schema_metadata where `key` = 'api_track'",
    )
    .fetch_optional(pool)
    .await?
    .flatten()
    .is_some_and(|value| value == "2");
    if has_v2 {
        return Err(RepositoryError::BackendMismatch {
            expected: "legacy upgrade track",
            actual: "2.0 clean track".to_owned(),
        });
    }

    let has_v3 = sqlx::query_scalar::<_, Option<String>>(
        "select value from keepsake_schema_metadata where `key` = 'api_track'",
    )
    .fetch_optional(pool)
    .await?
    .flatten()
    .is_some_and(|value| value == "3");
    if has_v3 {
        return Err(RepositoryError::BackendMismatch {
            expected: "legacy upgrade track",
            actual: "3.0 clean track".to_owned(),
        });
    }
    mysql_schema_preflight(pool).await
}

#[cfg(feature = "migrations")]
async fn mysql_schema_preflight(pool: &sqlx::MySqlPool) -> RepositoryResult<()> {
    let metadata = sqlx::query_scalar::<_, Option<String>>("select table_name from information_schema.tables where table_schema = database() and table_name = 'keepsake_schema_metadata'").fetch_optional(pool).await?.flatten();
    if metadata.is_some() {
        let backend = sqlx::query_scalar::<_, Option<String>>(
            "select value from keepsake_schema_metadata where `key` = 'backend'",
        )
        .fetch_optional(pool)
        .await?
        .flatten();
        return match backend.as_deref() {
            Some(MySqlBackend::NAME) | None => Ok(()),
            Some(actual) => Err(RepositoryError::BackendMismatch {
                expected: MySqlBackend::NAME,
                actual: actual.to_owned(),
            }),
        };
    }

    let existing = sqlx::query_scalar::<_, i64>(
        "select count(*) from information_schema.tables where table_schema = database()",
    )
    .fetch_one(pool)
    .await?;
    if existing == 0 {
        Ok(())
    } else {
        Err(RepositoryError::BackendMismatch {
            expected: MySqlBackend::NAME,
            actual: "unmarked non-empty schema".to_owned(),
        })
    }
}
