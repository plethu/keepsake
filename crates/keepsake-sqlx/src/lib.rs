//! `SQLx` adapter for Keepsake.
//!
//! This crate provides Postgres, `SQLite`, and `MySQL` repositories for durable
//! keepsake lifecycle state, relation reads, expiry workers, audit history, and
//! audit outbox export.
//!
//! SQL audit writes performed by repository commands are transactional:
//! `apply`, `revoke`, expiry helpers, and `append_audit_event` write the audit
//! event and the corresponding outbox row in the same database transaction.
//! External systems such as Kafka, Restate, S3, or warehouse loaders should
//! consume the database outbox through `audit_outbox`,
//! `claim_audit_outbox`, `ack_audit_outbox`, and `release_audit_outbox`; broker
//! and storage clients intentionally stay outside this crate.

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
    ActiveRelation, AppliedKeepsake, AuditCursor, AuditEventRecord, AuditOutboxCursor,
    AuditOutboxRecord, FulfilledExpiryCandidate, KeepsakeSqlxBackend, MembershipCursor,
    NoopRelationCache, RelationCache, RepositoryError, RepositoryResult, SqlxKeepsakeRepository,
    TimedExpiryCandidate, TimedSqlxKeepsakeRepository,
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
