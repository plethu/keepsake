//! Private activation evidence written by the complete-history bridge.
//!
//! There is intentionally no public constructor for this representation.
//! The only producer is the bridge importer after its bounded source scans and
//! live Dovecote reconciliation have reached the declared high-waters.

pub const SOURCE_SCHEMA: &str = "keepsake-sqlx-1.1";
pub const EVIDENCE_SCHEMA_VERSION: i64 = 1;
pub const PROVENANCE: &str = "keepsake-dovecote-importer";
pub const STREAM: &str = "keepsake-audit";

#[derive(Debug, sqlx::FromRow)]
pub(super) struct EvidenceRow {
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
