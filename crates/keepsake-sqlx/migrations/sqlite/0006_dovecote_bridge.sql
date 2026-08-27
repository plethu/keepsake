-- Additive Keepsake-to-Dovecote migration bookkeeping.  Historical audit and
-- outbox tables are deliberately untouched so their bytes remain immutable.
create table keepsake_dovecote_bridge_config (
  id integer primary key check (id = 1),
  source text not null,
  stream text not null default 'keepsake-audit',
  payload_codec text not null default 'keepsake.audit.json.v1',
  audit_high_water integer,
  audit_cursor integer not null default 0,
  outbox_high_water integer,
  outbox_cursor integer not null default 0,
  completed_at text,
  updated_at text not null
);

create table keepsake_dovecote_bridge_ledger (
  legacy_kind text not null check (legacy_kind in ('outbox', 'audit')),
  legacy_id integer not null,
  source text not null,
  stream text not null,
  event_id text not null,
  event_type text not null,
  occurred_at text not null,
  payload_codec text not null,
  payload_origin text not null check (payload_origin in ('bridge_exact', 'legacy_outbox_reencoded', 'legacy_outbox_exact_text', 'reconstructed_v1')),
  payload blob not null,
  payload_sha256 text not null,
  dovecote_row_id integer not null,
  imported_at text not null,
  primary key (legacy_kind, legacy_id),
  unique (source, event_id)
);

create index keepsake_dovecote_bridge_ledger_row
  on keepsake_dovecote_bridge_ledger (dovecote_row_id);

-- The legacy outbox schema has no claim token. This bridge-owned generation
-- fences a bridge-aware acknowledgement even when a worker is reclaimed with
-- the same owner and expiry timestamp.
create table keepsake_dovecote_bridge_claims (
  outbox_id integer primary key references keepsake_audit_outbox(id) on delete cascade,
  claim_token blob not null check (length(claim_token) = 16),
  claimed_by text not null,
  claimed_until text not null,
  updated_at text not null
);

-- Importer-owned evidence for the explicit 1.x-to-2.0 activation gate.  It is
-- written only by a completed bridge reconciliation; application code cannot
-- manufacture an activation report.
create table keepsake_upgrade_evidence (
  evidence_id integer primary key,
  evidence_schema_version integer not null,
  provenance text not null,
  source text not null,
  source_schema text not null,
  stream text not null,
  audit_high_water integer not null,
  outbox_high_water integer not null,
  missing_count integer not null,
  extra_count integer not null,
  state_delta_count integer not null,
  digest_delta_count integer not null,
  active_claim_count integer not null,
  codec_version text not null,
  complete integer not null check (complete in (0, 1)),
  constraint keepsake_upgrade_evidence_single_row check (evidence_id = 1),
  constraint keepsake_upgrade_evidence_nonnegative check (
    audit_high_water >= 0 and outbox_high_water >= 0 and missing_count >= 0 and extra_count >= 0
    and state_delta_count >= 0 and digest_delta_count >= 0
    and active_claim_count >= 0
  )
);
