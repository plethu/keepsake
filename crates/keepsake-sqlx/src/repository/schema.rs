use super::{
    KeepsakeSqlxBackend, MySqlBackend, PostgresBackend, RepositoryError, RepositoryResult,
    SqliteBackend,
};

#[cfg(all(feature = "postgres", feature = "migrations"))]
pub(super) async fn postgres_schema_preflight(pool: &sqlx::PgPool) -> RepositoryResult<()> {
    let metadata_table = sqlx::query_scalar::<_, Option<String>>(
        r"
        select to_regclass('public.keepsake_schema_metadata')::text
        ",
    )
    .fetch_one(pool)
    .await?;

    if metadata_table.is_none() {
        return postgres_unmarked_schema_preflight(pool).await;
    }

    let backend = sqlx::query_scalar::<_, Option<String>>(
        r"
        select value
        from keepsake_schema_metadata
        where key = 'backend'
        ",
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

#[cfg(all(feature = "postgres", feature = "migrations"))]
async fn postgres_unmarked_schema_preflight(pool: &sqlx::PgPool) -> RepositoryResult<()> {
    let user_table_count = sqlx::query_scalar::<_, i64>(
        r"
        select count(*)
        from information_schema.tables
        where table_schema = 'public'
          and table_type = 'BASE TABLE'
        ",
    )
    .fetch_one(pool)
    .await?;

    if user_table_count == 0 {
        return Ok(());
    }

    let has_keepsake_tables = sqlx::query_scalar::<_, bool>(
        r"
        select to_regclass('public.keepsake_relation_definitions') is not null
           and to_regclass('public.keepsakes') is not null
           and to_regclass('public._sqlx_migrations') is not null
        ",
    )
    .fetch_one(pool)
    .await?;
    if !has_keepsake_tables {
        return Err(RepositoryError::BackendMismatch {
            expected: PostgresBackend::NAME,
            actual: "unmarked non-empty schema".to_owned(),
        });
    }

    let known_migrations = sqlx::query_scalar::<_, i64>(
        r"
        select count(*)
        from _sqlx_migrations
        where version in (1, 2)
        ",
    )
    .fetch_one(pool)
    .await?;

    if known_migrations == 2 {
        Ok(())
    } else {
        Err(RepositoryError::BackendMismatch {
            expected: PostgresBackend::NAME,
            actual: "unmarked unknown migration history".to_owned(),
        })
    }
}

#[cfg(all(feature = "sqlite", feature = "migrations"))]
pub(super) async fn sqlite_schema_preflight(pool: &sqlx::SqlitePool) -> RepositoryResult<()> {
    let metadata_table = sqlx::query_scalar::<_, Option<String>>(
        r"
        select name
        from sqlite_master
        where type = 'table' and name = 'keepsake_schema_metadata'
        ",
    )
    .fetch_optional(pool)
    .await?
    .flatten();

    if metadata_table.is_some() {
        let backend = sqlx::query_scalar::<_, Option<String>>(
            r"
            select value
            from keepsake_schema_metadata
            where key = 'backend'
            ",
        )
        .fetch_one(pool)
        .await?;
        return match backend.as_deref() {
            Some(SqliteBackend::NAME) | None => Ok(()),
            Some(actual) => Err(RepositoryError::BackendMismatch {
                expected: SqliteBackend::NAME,
                actual: actual.to_owned(),
            }),
        };
    }

    let existing_tables = sqlx::query_scalar::<_, i64>(
        r"
        select count(*)
        from sqlite_master
        where type = 'table'
          and name not like 'sqlite_%'
        ",
    )
    .fetch_one(pool)
    .await?;

    if existing_tables == 0 {
        Ok(())
    } else {
        Err(RepositoryError::BackendMismatch {
            expected: SqliteBackend::NAME,
            actual: "unmarked non-empty schema".to_owned(),
        })
    }
}

#[cfg(all(feature = "mysql", feature = "migrations"))]
pub(super) async fn mysql_schema_preflight(pool: &sqlx::MySqlPool) -> RepositoryResult<()> {
    let metadata_table = sqlx::query_scalar::<_, Option<String>>(
        r"
        select table_name
        from information_schema.tables
        where table_schema = database()
          and table_name = 'keepsake_schema_metadata'
        ",
    )
    .fetch_optional(pool)
    .await?
    .flatten();

    if metadata_table.is_some() {
        let backend = sqlx::query_scalar::<_, Option<String>>(
            r"
            select value
            from keepsake_schema_metadata
            where `key` = 'backend'
            ",
        )
        .fetch_one(pool)
        .await?;
        return match backend.as_deref() {
            Some(MySqlBackend::NAME) | None => Ok(()),
            Some(actual) => Err(RepositoryError::BackendMismatch {
                expected: MySqlBackend::NAME,
                actual: actual.to_owned(),
            }),
        };
    }

    let existing_tables = sqlx::query_scalar::<_, i64>(
        r"
        select count(*)
        from information_schema.tables
        where table_schema = database()
        ",
    )
    .fetch_one(pool)
    .await?;

    if existing_tables == 0 {
        Ok(())
    } else {
        Err(RepositoryError::BackendMismatch {
            expected: MySqlBackend::NAME,
            actual: "unmarked non-empty schema".to_owned(),
        })
    }
}
