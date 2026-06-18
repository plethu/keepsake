//! In-memory audit sink for tests.

use std::sync::{Arc, Mutex};

use crate::audit::{AuditEvent, AuditSink};

/// In-memory audit sink errors.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum InMemoryAuditError {
    /// The shared event buffer was poisoned by a previous panic.
    #[error("in-memory audit sink buffer is poisoned")]
    Poisoned,
}

/// In-memory audit sink for tests.
#[derive(Debug, Clone, Default)]
pub struct InMemoryAuditSink {
    // Test-only shared buffer for asserting emitted audit events. Production
    // audit sinks should write to a durable backend instead of sharing a mutex.
    events: Arc<Mutex<Vec<AuditEvent>>>,
}

impl InMemoryAuditSink {
    /// Returns a snapshot of recorded events.
    pub fn events(&self) -> Result<Vec<AuditEvent>, InMemoryAuditError> {
        self.events
            .lock()
            .map_err(|_| InMemoryAuditError::Poisoned)
            .map(|events| events.clone())
    }
}

impl AuditSink for InMemoryAuditSink {
    type Error = InMemoryAuditError;

    fn record(&self, event: AuditEvent) -> Result<(), Self::Error> {
        self.events
            .lock()
            .map_err(|_| InMemoryAuditError::Poisoned)?
            .push(event);
        Ok(())
    }
}
