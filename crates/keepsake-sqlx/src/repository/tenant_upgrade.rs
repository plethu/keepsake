//! Explicit v2-to-v3 tenant upgrade artifacts.

/// Adds nullable `PostgreSQL` tenant columns without assigning values.
#[cfg(feature = "postgres")]
pub const POSTGRES_PREPARE_SQL: &str =
    include_str!("../../migrations/upgrade/v2_to_v3/postgres/prepare.sql");

/// Verifies `PostgreSQL` operator backfill and activates tenant constraints.
#[cfg(feature = "postgres")]
pub const POSTGRES_ACTIVATE_SQL: &str =
    include_str!("../../migrations/upgrade/v2_to_v3/postgres/activate.sql");

/// Adds nullable `MySQL` tenant columns without assigning values.
#[cfg(feature = "mysql")]
pub const MYSQL_PREPARE_SQL: &str =
    include_str!("../../migrations/upgrade/v2_to_v3/mysql/prepare.sql");

/// Verifies `MySQL` operator backfill and activates tenant constraints.
#[cfg(feature = "mysql")]
pub const MYSQL_ACTIVATE_SQL: &str =
    include_str!("../../migrations/upgrade/v2_to_v3/mysql/activate.sql");

/// Adds nullable `SQLite` tenant columns without assigning values.
#[cfg(feature = "sqlite")]
pub const SQLITE_PREPARE_SQL: &str =
    include_str!("../../migrations/upgrade/v2_to_v3/sqlite/prepare.sql");

/// Verifies `SQLite` operator backfill and activates tenant constraints.
#[cfg(feature = "sqlite")]
pub const SQLITE_ACTIVATE_SQL: &str =
    include_str!("../../migrations/upgrade/v2_to_v3/sqlite/activate.sql");
