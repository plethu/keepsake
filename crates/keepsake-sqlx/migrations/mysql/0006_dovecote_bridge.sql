-- Additive Keepsake-to-Dovecote migration bookkeeping.  Historical audit and
-- outbox tables are deliberately untouched so their bytes remain immutable.
create table keepsake_dovecote_bridge_config (
  id boolean primary key default true,
  source varchar(2048) not null,
  stream varchar(255) not null default 'keepsake-audit',
  payload_codec varchar(64) not null default 'keepsake.audit.json.v1',
  audit_high_water bigint,
  audit_cursor bigint not null default 0,
  outbox_high_water bigint,
  outbox_cursor bigint not null default 0,
  completed_at datetime(6),
  updated_at datetime(6) not null,
  constraint keepsake_dovecote_bridge_config_singleton check (id = true)
);

create table keepsake_dovecote_bridge_ledger (
  legacy_kind varchar(16) not null,
  legacy_id bigint not null,
  source varbinary(2048) not null,
  stream varchar(255) not null,
  event_id varbinary(1024) not null,
  event_type varchar(255) not null,
  occurred_at datetime(6) not null,
  payload_codec varchar(64) not null,
  payload_origin varchar(32) not null,
  payload longblob not null,
  payload_sha256 char(64) not null,
  dovecote_row_id bigint not null,
  imported_at datetime(6) not null,
  primary key (legacy_kind, legacy_id),
  unique key keepsake_dovecote_bridge_identity (source, event_id),
  constraint keepsake_dovecote_bridge_ledger_kind check (legacy_kind in ('outbox', 'audit')),
  constraint keepsake_dovecote_bridge_ledger_origin check (payload_origin in ('bridge_exact', 'legacy_outbox_reencoded', 'legacy_outbox_exact_text', 'reconstructed_v1')),
  constraint keepsake_dovecote_bridge_identity_length check (octet_length(source) + octet_length(event_id) <= 2048)
);

create index keepsake_dovecote_bridge_ledger_row
  on keepsake_dovecote_bridge_ledger (dovecote_row_id);

-- The legacy outbox schema has no claim token. This bridge-owned generation
-- fences a bridge-aware acknowledgement even when a worker is reclaimed with
-- the same owner and expiry timestamp.
create table keepsake_dovecote_bridge_claims (
  outbox_id bigint primary key,
  claim_token binary(16) not null,
  claimed_by varchar(255) not null,
  claimed_until datetime(6) not null,
  updated_at datetime(6) not null,
  constraint keepsake_dovecote_bridge_claims_outbox_fk foreign key (outbox_id)
    references keepsake_audit_outbox(id) on delete cascade,
  constraint keepsake_dovecote_bridge_claims_token check (octet_length(claim_token) = 16)
);

-- Importer-owned evidence for the explicit 1.x-to-2.0 activation gate.  It is
-- written only by a completed bridge reconciliation; application code cannot
-- manufacture an activation report.
create table keepsake_upgrade_evidence (
  evidence_id bigint primary key,
  evidence_schema_version bigint not null,
  provenance varchar(191) not null,
  source varchar(2048) not null,
  source_schema varchar(191) not null,
  stream varchar(191) not null,
  audit_high_water bigint not null,
  outbox_high_water bigint not null,
  missing_count bigint not null,
  extra_count bigint not null,
  state_delta_count bigint not null,
  digest_delta_count bigint not null,
  active_claim_count bigint not null,
  codec_version varchar(191) not null,
  complete boolean not null,
  constraint keepsake_upgrade_evidence_single_row check (evidence_id = 1),
  constraint keepsake_upgrade_evidence_nonnegative check (
    audit_high_water >= 0 and outbox_high_water >= 0 and missing_count >= 0 and extra_count >= 0
    and state_delta_count >= 0 and digest_delta_count >= 0
    and active_claim_count >= 0
  )
);
