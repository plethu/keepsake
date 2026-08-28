-- Keepsake 3.0 clean tenant-aware baseline. Published v1 and v2 migration
-- bytes are immutable; an existing v2 installation must use the explicit
-- prepare/backfill/activate route instead.
create table keepsake_schema_metadata (
  key text primary key,
  value text not null
);

insert into keepsake_schema_metadata (key, value) values ('backend', 'postgres');
insert into keepsake_schema_metadata (key, value) values ('api_track', '3');

create table keepsake_relation_definitions (
  tenant_id text collate "C" not null,
  id uuid not null,
  kind text not null,
  key text not null,
  enabled boolean not null default true,
  expiry_policy jsonb not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint keepsake_relation_definitions_tenant_size check (octet_length(tenant_id) <= 255),
  constraint keepsake_relation_definitions_tenant_nonempty check (octet_length(tenant_id) > 0),
  primary key (tenant_id, id),
  unique (tenant_id, kind, key)
);

create index keepsake_relation_definitions_tenant_key
  on keepsake_relation_definitions (tenant_id, kind, key, id);

create table keepsakes (
  tenant_id text collate "C" not null,
  id uuid not null,
  subject_kind text not null,
  subject_id text not null,
  relation_id uuid not null,
  state text not null constraint keepsakes_state_check check (state in ('applied', 'revoked', 'expired')),
  expiry_policy jsonb not null,
  applied_at timestamptz not null,
  expires_at timestamptz,
  fulfilled_at timestamptz,
  revoked_at timestamptz,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint keepsakes_tenant_size check (octet_length(tenant_id) <= 255),
  constraint keepsakes_tenant_nonempty check (octet_length(tenant_id) > 0),
  primary key (tenant_id, id),
  constraint keepsakes_relation_fk foreign key (tenant_id, relation_id)
    references keepsake_relation_definitions (tenant_id, id),
  constraint keepsakes_expiry_policy_projection check (coalesce(
    expiry_policy->>'type' in ('manual_only', 'at', 'when_fulfilled') and (
      (expiry_policy->>'type' = 'at' and expires_at is not null
       and (expiry_policy->>'timestamp')::timestamptz = expires_at)
      or (expiry_policy->>'type' in ('manual_only', 'when_fulfilled') and expires_at is null)
    ), false
  )),
  constraint keepsakes_lifecycle_timestamps check (coalesce(
    (state = 'applied' and revoked_at is null and fulfilled_at is null)
    or (state = 'revoked' and revoked_at is not null and fulfilled_at is null)
    or (state = 'expired' and revoked_at is null and (
      (expiry_policy->>'type' = 'at' and expires_at is not null and fulfilled_at is null)
      or (expiry_policy->>'type' = 'when_fulfilled' and fulfilled_at is not null and expires_at is null)
    )), false
  ))
);

create unique index keepsakes_one_active_relation_per_subject
  on keepsakes (tenant_id, subject_kind, subject_id, relation_id) where state = 'applied';
create index keepsakes_active_subject_lookup
  on keepsakes (tenant_id, subject_kind, subject_id, relation_id, id) where state = 'applied';
create index keepsakes_active_relation_membership
  on keepsakes (tenant_id, relation_id, subject_kind, subject_id, id) where state = 'applied';
create index keepsakes_due_timed_expiry
  on keepsakes (tenant_id, expires_at, relation_id, subject_kind, subject_id, id)
  where state = 'applied' and expires_at is not null;

create table keepsake_fulfillment_counters (
  tenant_id text collate "C" not null,
  keepsake_id uuid not null,
  key text not null,
  value bigint not null,
  observed_at timestamptz not null,
  constraint keepsake_fulfillment_counter_tenant_size check (octet_length(tenant_id) <= 255),
  constraint keepsake_fulfillment_counter_tenant_nonempty check (octet_length(tenant_id) > 0),
  primary key (tenant_id, keepsake_id, key),
  constraint keepsake_fulfillment_counter_keepsake_fk
    foreign key (tenant_id, keepsake_id)
    references keepsakes (tenant_id, id) on delete cascade
);
create index keepsake_fulfillment_counter_scan
  on keepsake_fulfillment_counters (tenant_id, key, value, keepsake_id);

create index keepsakes_due_fulfilled_expiry
  on keepsakes (tenant_id, relation_id, subject_kind, subject_id, id)
  where state = 'applied' and expiry_policy->>'type' = 'when_fulfilled';

create table keepsake_fulfillment_checklist (
  tenant_id text collate "C" not null,
  keepsake_id uuid not null,
  item text not null,
  complete boolean not null,
  observed_at timestamptz not null,
  constraint keepsake_fulfillment_checklist_tenant_size check (octet_length(tenant_id) <= 255),
  constraint keepsake_fulfillment_checklist_tenant_nonempty check (octet_length(tenant_id) > 0),
  primary key (tenant_id, keepsake_id, item),
  constraint keepsake_fulfillment_checklist_keepsake_fk
    foreign key (tenant_id, keepsake_id)
    references keepsakes (tenant_id, id) on delete cascade
);
create index keepsake_fulfillment_checklist_scan
  on keepsake_fulfillment_checklist (tenant_id, item, complete, keepsake_id);
