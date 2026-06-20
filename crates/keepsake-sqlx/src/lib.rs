//! `SQLx` adapter for Keepsake.

mod repository;

pub mod prelude {
    //! Common imports for application modules using the `SQLx` adapter.

    #[cfg(feature = "postgres")]
    pub use crate::{KeepsakeRepository, PostgresKeepsakeRepository, TimedKeepsakeRepository};
    #[cfg(feature = "mysql")]
    pub use crate::{MySqlKeepsakeRepository, TimedMySqlKeepsakeRepository};
    pub use crate::{RepositoryError, RepositoryResult};
    #[cfg(feature = "sqlite")]
    pub use crate::{SqliteKeepsakeRepository, TimedSqliteKeepsakeRepository};
}

pub use repository::{
    ActiveRelation, AppliedKeepsake, FulfilledExpiryCandidate, KeepsakeSqlxBackend,
    MembershipCursor, NoopRelationCache, RelationCache, RepositoryError, RepositoryResult,
    SqlxKeepsakeRepository, TimedExpiryCandidate, TimedSqlxKeepsakeRepository,
};
#[cfg(feature = "postgres")]
pub use repository::{
    KeepsakeRepository, PostgresBackend, PostgresKeepsakeRepository, TimedKeepsakeRepository,
};
#[cfg(feature = "cache")]
pub use repository::{LocalRelationCache, LocalRelationCacheConfig};
#[cfg(feature = "mysql")]
pub use repository::{MySqlBackend, MySqlKeepsakeRepository, TimedMySqlKeepsakeRepository};
#[cfg(feature = "sqlite")]
pub use repository::{SqliteBackend, SqliteKeepsakeRepository, TimedSqliteKeepsakeRepository};
