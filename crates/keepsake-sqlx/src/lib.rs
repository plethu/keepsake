//! `SQLx` adapter for Keepsake.
//!
//! Human guides and reference material are in the `docs/` directory of the
//! repository. API reference: <https://docs.rs/keepsake-sqlx>.
//!
//! This crate provides Postgres, `SQLite`, and `MySQL` repositories for durable
//! keepsake lifecycle state, relation reads, and expiry workers.
//!
//! Keepsake 2.0 stores each lifecycle audit occurrence as one validated
//! Dovecote event in the same caller transaction as the domain mutation. The
//! repository owns relation state; Dovecote owns the immutable event and
//! delivery records. Publication workers and transport clients stay outside
//! both crates. Consumers should use Dovecote's live or snapshot paging and
//! deduplicate at the `CloudEvents` `(source, id)` boundary.
//!
//! The constructor requires an application-owned absolute `CloudEvents` source.
//! Install the matching Keepsake clean baseline and Dovecote schema, then call
//! [`SqlxKeepsakeRepository::check_schema`] before serving requests. The
//! explicit [`SqlxKeepsakeRepository::upgrade_migrate`] path is only for
//! installations carrying the historical 1.x audit tables. Call
//! `activate_upgrade` only after complete-history reconciliation; until then
//! the 2.0 schema check remains blocked. Legacy tables stay inert for rollback.

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
    ActiveRelation, AppliedKeepsake, AuditEventDecodeError, DovecoteAuditConfig,
    DovecoteEnqueueError, DovecoteSchemaError, FulfilledExpiryCandidate, KeepsakeSqlxBackend,
    MembershipCursor, NoopRelationCache, RelationCache, RepositoryError, RepositoryResult,
    SqlxKeepsakeRepository, TimedExpiryCandidate, TimedSqlxKeepsakeRepository, decode_audit_event,
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
