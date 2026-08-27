-- Keepsake 2.0 clean baseline. Audit persistence belongs to Dovecote.
create table keepsake_schema_metadata (
  key text primary key,
  value text not null
);

insert into keepsake_schema_metadata (key, value) values ('backend', 'postgres');
insert into keepsake_schema_metadata (key, value) values ('api_track', '2');

create table keepsake_relation_definitions (
  id uuid primary key,
  kind text not null,
  key text not null,
  enabled boolean not null default true,
  expiry_policy jsonb not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (kind, key)
);

create table keepsakes (
  id uuid primary key,
  subject_kind text not null,
  subject_id text not null,
  relation_id uuid not null references keepsake_relation_definitions(id),
  state text not null constraint keepsakes_state_check check (state in ('applied', 'revoked', 'expired')),
  expiry_policy jsonb not null,
  applied_at timestamptz not null,
  expires_at timestamptz,
  fulfilled_at timestamptz,
  revoked_at timestamptz,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
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
  on keepsakes (subject_kind, subject_id, relation_id) where state = 'applied';
create index keepsakes_active_subject_lookup
  on keepsakes (subject_kind, subject_id, relation_id, id) where state = 'applied';
create index keepsakes_active_relation_membership
  on keepsakes (relation_id, subject_kind, subject_id, id) where state = 'applied';
create index keepsakes_due_timed_expiry
  on keepsakes (expires_at, relation_id, subject_kind, subject_id, id)
  where state = 'applied' and expires_at is not null;

create table keepsake_fulfillment_counters (
  keepsake_id uuid not null references keepsakes(id) on delete cascade,
  key text not null,
  value bigint not null,
  observed_at timestamptz not null,
  primary key (keepsake_id, key)
);
create index keepsake_fulfillment_counter_scan
  on keepsake_fulfillment_counters (key, value, keepsake_id);

create index keepsakes_due_fulfilled_expiry
  on keepsakes (relation_id, subject_kind, subject_id, id)
  where state = 'applied' and expiry_policy->>'type' = 'when_fulfilled';

create table keepsake_fulfillment_checklist (
  keepsake_id uuid not null references keepsakes(id) on delete cascade,
  item text not null,
  complete boolean not null,
  observed_at timestamptz not null,
  primary key (keepsake_id, item)
);
create index keepsake_fulfillment_checklist_scan
  on keepsake_fulfillment_checklist (item, complete, keepsake_id);
