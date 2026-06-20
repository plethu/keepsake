//! Deterministic relation lifecycle primitives.

pub mod audit;
pub mod command;
pub mod error;
pub mod evaluation;
pub mod model;
pub mod observe;
pub mod policy;
pub mod prelude {
    //! Common imports for application modules using Keepsake.

    pub use crate::{
        ActiveRelation, ActiveRelationSource, ActorRef, ApplyKeepsake, AuditContext, AuditDecision,
        AuditEvent, AuditEventType, AuditSink, CommandContext, DynActiveRelationSource,
        ExpiryCause, ExpiryPolicy, FulfillmentPolicy, FulfillmentProvider, FulfillmentSnapshot,
        Keepsake, KeepsakeError, KeepsakeId, KeepsakeLifecycle, KeepsakeRecord, KeepsakeStore,
        LifecycleState, RelationDefinition, RelationId, RelationKey, RelationKind, RelationName,
        RelationSpec, StaticRelationKey, SubjectRef,
    };

    #[cfg(any(test, feature = "test"))]
    pub use crate::{
        ActiveRelationSeed, InMemoryActiveRelations, InMemoryActiveRelationsError,
        InMemoryFulfillmentProvider, InMemoryFulfillmentProviderError, InMemoryKeepsakeStore,
        InMemoryKeepsakeStoreError,
    };
}
pub mod provider;

#[doc(hidden)]
pub mod __private {
    pub use chrono::{DateTime, Utc};
    pub use uuid::Uuid;
}

/// Defines a zero-sized typed relation catalogue entry.
///
/// This is a convenience wrapper around [`RelationSpec`]. The trait remains the
/// stable contract; use the macro when application code owns a static relation
/// catalogue and the repeated marker-type boilerplate gets noisy.
#[macro_export]
macro_rules! relation_spec {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            id: $id:expr;
            key: ($kind:literal, $relation_name:literal);
            $(enabled: $enabled:expr;)?
            expiry($at:pat) => $expiry:expr $(;)?
        }
    ) => {
        $(#[$meta])*
        $vis struct $name;

        impl $crate::RelationSpec for $name {
            const ID: $crate::RelationId = $crate::__private::Uuid::from_u128($id);
            const KEY: $crate::StaticRelationKey =
                $crate::StaticRelationKey::new($kind, $relation_name);
            const ENABLED: bool = $crate::relation_spec!(@enabled $($enabled)?);

            fn expiry(
                $at: $crate::__private::DateTime<$crate::__private::Utc>,
            ) -> $crate::ExpiryPolicy {
                $expiry
            }
        }
    };
    (@enabled $enabled:expr) => {
        $enabled
    };
    (@enabled) => {
        true
    };
}

pub use audit::{
    AuditContext, AuditDecision, AuditEvent, AuditEventType, AuditSink, NoopAuditSink,
};
#[cfg(any(test, feature = "test"))]
pub use audit::{InMemoryAuditError, InMemoryAuditSink};
pub use command::{ApplyKeepsake, CommandContext, RevokeKeepsake};
pub use error::{KeepsakeError, Result};
pub use evaluation::{DecisionKind, EvaluationDecision, NoopReason, TransitionReason, evaluate};
pub use model::{
    ActiveRelation, ActorRef, ExpiryCause, FulfillmentSnapshot, Keepsake, KeepsakeId,
    KeepsakeLifecycle, KeepsakeRecord, LifecycleState, RelationDefinition, RelationId, RelationKey,
    RelationKind, RelationName, RelationSpec, StaticRelationKey, SubjectRef,
};
pub use observe::{
    MetricsRecorder, NoopMetricsRecorder, NoopTransitionObserver, TransitionObserver,
};
pub use policy::{ExpiryPolicy, FulfillmentPolicy};
#[cfg(any(test, feature = "test"))]
pub use provider::{
    ActiveRelationSeed, InMemoryActiveRelations, InMemoryActiveRelationsError,
    InMemoryFulfillmentProvider, InMemoryFulfillmentProviderError, InMemoryKeepsakeStore,
    InMemoryKeepsakeStoreError,
};
pub use provider::{
    ActiveRelationSource, DynActiveRelationSource, FulfillmentProvider, KeepsakeStore,
};
