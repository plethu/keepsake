use std::fmt::Debug;
use std::marker::PhantomData;

/// `SQLx` backend supported by Keepsake.
pub trait KeepsakeSqlxBackend: Debug + Clone + Copy + Send + Sync + 'static {
    /// `SQLx` database driver for this backend.
    type Database: sqlx::Database;

    /// Stable backend name stored in schema metadata.
    const NAME: &'static str;
}

/// Postgres backend marker.
#[cfg(feature = "postgres")]
#[derive(Debug, Clone, Copy)]
pub struct PostgresBackend;

#[cfg(feature = "postgres")]
impl KeepsakeSqlxBackend for PostgresBackend {
    type Database = sqlx::Postgres;

    const NAME: &'static str = "postgres";
}

/// `SQLite` backend marker.
#[cfg(feature = "sqlite")]
#[derive(Debug, Clone, Copy)]
pub struct SqliteBackend;

#[cfg(feature = "sqlite")]
impl KeepsakeSqlxBackend for SqliteBackend {
    type Database = sqlx::Sqlite;

    const NAME: &'static str = "sqlite";
}

/// `MySQL` backend marker.
#[cfg(feature = "mysql")]
#[derive(Debug, Clone, Copy)]
pub struct MySqlBackend;

#[cfg(feature = "mysql")]
impl KeepsakeSqlxBackend for MySqlBackend {
    type Database = sqlx::MySql;

    const NAME: &'static str = "mysql";
}

#[derive(Debug, Clone, Copy)]
pub(super) struct BackendMarker<B>(PhantomData<B>);

impl<B> BackendMarker<B> {
    pub(super) const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<B> Default for BackendMarker<B> {
    fn default() -> Self {
        Self::new()
    }
}
