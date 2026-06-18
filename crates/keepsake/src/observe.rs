//! Lightweight observability contracts.

use serde::{Deserialize, Serialize};

use crate::evaluation::DecisionKind;
use crate::model::{RelationKey, SubjectRef};

/// Structured lifecycle outcome emitted after an operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitionObservation {
    /// Command or job name, such as `apply`, `revoke`, or `expire_due`.
    pub operation: &'static str,
    /// Relation key, safe to aggregate.
    pub relation: RelationKey,
    /// Subject kind without subject id.
    pub subject_kind: String,
    /// Decision result.
    pub decision: DecisionKind,
}

impl TransitionObservation {
    /// Builds an observation while excluding app-owned subject ids.
    #[must_use]
    pub fn new(
        operation: &'static str,
        relation: RelationKey,
        subject: &SubjectRef,
        decision: DecisionKind,
    ) -> Self {
        Self {
            operation,
            relation,
            subject_kind: subject.kind.clone(),
            decision,
        }
    }
}

/// Hook for structured lifecycle outcomes.
pub trait TransitionObserver: Send + Sync {
    /// Observes a lifecycle outcome.
    fn observe(&self, observation: &TransitionObservation);
}

/// No-op transition observer.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopTransitionObserver;

impl TransitionObserver for NoopTransitionObserver {
    fn observe(&self, _observation: &TransitionObservation) {}
}

/// Backend-neutral metrics hook.
pub trait MetricsRecorder: Send + Sync {
    /// Records an operation count.
    fn increment(&self, name: &'static str, labels: &[(&'static str, String)]);

    /// Records a timing in milliseconds.
    fn timing_ms(&self, name: &'static str, value: u64, labels: &[(&'static str, String)]);
}

/// No-op metrics recorder.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopMetricsRecorder;

impl MetricsRecorder for NoopMetricsRecorder {
    fn increment(&self, _name: &'static str, _labels: &[(&'static str, String)]) {}

    fn timing_ms(&self, _name: &'static str, _value: u64, _labels: &[(&'static str, String)]) {}
}
