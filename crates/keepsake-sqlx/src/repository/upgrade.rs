//! Typed evidence for the explicit historical upgrade track.

use super::{RepositoryError, RepositoryResult};

/// Version of the Keepsake 1.x schema that the 2.0 upgrade track imports.
///
/// This is deliberately separate from the evidence row schema version. The
/// former identifies the source tables; the latter identifies this row's
/// interpretation.
pub const UPGRADE_SOURCE_SCHEMA: &str = "keepsake-sqlx-1.1";

/// Version of the importer evidence row contract.
pub const UPGRADE_EVIDENCE_SCHEMA_VERSION: i64 = 1;

/// Fixed provenance marker for the complete-history importer.
pub const UPGRADE_EVIDENCE_PROVENANCE: &str = "keepsake-dovecote-importer";

/// The stream owned by Keepsake's Dovecote audit projection.
pub const UPGRADE_EVIDENCE_STREAM: &str = "keepsake-audit";

#[derive(Debug, sqlx::FromRow)]
pub(super) struct UpgradeEvidenceRow {
    pub(super) evidence_schema_version: i64,
    pub(super) provenance: String,
    pub(super) source: String,
    pub(super) source_schema: String,
    pub(super) stream: String,
    pub(super) audit_high_water: i64,
    pub(super) outbox_high_water: i64,
    pub(super) missing_count: i64,
    pub(super) extra_count: i64,
    pub(super) state_delta_count: i64,
    pub(super) digest_delta_count: i64,
    pub(super) active_claim_count: i64,
    pub(super) codec_version: String,
    pub(super) complete: bool,
}

pub(super) fn validate(row: &UpgradeEvidenceRow, source: &str) -> RepositoryResult<()> {
    if row.evidence_schema_version != UPGRADE_EVIDENCE_SCHEMA_VERSION {
        return Err(RepositoryError::InvalidUpgradeEvidence {
            field: "evidence_schema_version",
        });
    }

    if row.provenance != UPGRADE_EVIDENCE_PROVENANCE {
        return Err(RepositoryError::InvalidUpgradeEvidence {
            field: "provenance",
        });
    }

    if row.source != source {
        return Err(RepositoryError::InvalidUpgradeEvidence { field: "source" });
    }

    if row.source_schema != UPGRADE_SOURCE_SCHEMA {
        return Err(RepositoryError::InvalidUpgradeEvidence {
            field: "source_schema",
        });
    }

    if row.stream != UPGRADE_EVIDENCE_STREAM {
        return Err(RepositoryError::InvalidUpgradeEvidence { field: "stream" });
    }

    if row.audit_high_water < 0 {
        return Err(RepositoryError::InvalidUpgradeEvidence {
            field: "audit_high_water",
        });
    }

    if row.outbox_high_water < 0 {
        return Err(RepositoryError::InvalidUpgradeEvidence {
            field: "outbox_high_water",
        });
    }

    if row.missing_count != 0 {
        return Err(RepositoryError::InvalidUpgradeEvidence {
            field: "missing_count",
        });
    }

    if row.extra_count != 0 {
        return Err(RepositoryError::InvalidUpgradeEvidence {
            field: "extra_count",
        });
    }

    if row.state_delta_count != 0 {
        return Err(RepositoryError::InvalidUpgradeEvidence {
            field: "state_delta_count",
        });
    }

    if row.digest_delta_count != 0 {
        return Err(RepositoryError::InvalidUpgradeEvidence {
            field: "digest_delta_count",
        });
    }

    if row.active_claim_count != 0 {
        return Err(RepositoryError::InvalidUpgradeEvidence {
            field: "active_claim_count",
        });
    }

    if row.codec_version.trim().is_empty() {
        return Err(RepositoryError::InvalidUpgradeEvidence {
            field: "codec_version",
        });
    }

    if !row.complete {
        return Err(RepositoryError::InvalidUpgradeEvidence { field: "complete" });
    }
    Ok(())
}
