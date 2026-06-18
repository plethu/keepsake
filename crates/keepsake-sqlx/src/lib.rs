//! SQLx/Postgres adapter for Keepsake.

mod repository;

pub mod prelude {
    //! Common imports for application modules using the `SQLx` adapter.

    pub use crate::{KeepsakeRepository, RepositoryError, RepositoryResult};
}

pub use repository::{
    ActiveRelation, AppliedKeepsake, KeepsakeRepository, MembershipCursor, NoopRelationCache,
    RelationCache, RepositoryError, RepositoryResult, TimedExpiryCandidate,
    TimedKeepsakeRepository,
};
#[cfg(feature = "cache")]
pub use repository::{LocalRelationCache, LocalRelationCacheConfig};
