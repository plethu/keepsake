-- Keepsake 3.0 clean tenant-aware baseline. Published v1 and v2 migration
-- bytes are immutable; an existing v2 installation must use the explicit
-- prepare/backfill/activate route instead.
create table keepsake_schema_metadata (key text primary key, value text not null);
insert into keepsake_schema_metadata (key, value) values ('backend', 'sqlite');
insert into keepsake_schema_metadata (key, value) values ('api_track', '3');

create table keepsake_relation_definitions (
  tenant_id text not null check (length(cast(tenant_id as blob)) > 0 and length(cast(tenant_id as blob)) <= 255),
  id text not null,
  kind text not null,
  key text not null,
  enabled integer not null default 1 check (enabled in (0, 1)),
  expiry_policy text not null check (json_valid(expiry_policy)),
  created_at text not null,
  updated_at text not null,
  primary key (tenant_id, id),
  unique (tenant_id, kind, key)
);
create index keepsake_relation_definitions_tenant_key
  on keepsake_relation_definitions (tenant_id, kind, key, id);

create table keepsakes (
  tenant_id text not null check (length(cast(tenant_id as blob)) > 0 and length(cast(tenant_id as blob)) <= 255),
  id text not null,
  subject_kind text not null,
  subject_id text not null,
  relation_id text not null,
  state text not null check (state in ('applied', 'revoked', 'expired')),
  expiry_policy text not null check (json_valid(expiry_policy)),
  applied_at text not null,
  expires_at text,
  fulfilled_at text,
  revoked_at text,
  metadata text not null default '{}' check (json_valid(metadata)),
  created_at text not null,
  updated_at text not null,
  primary key (tenant_id, id),
  foreign key (tenant_id, relation_id)
    references keepsake_relation_definitions(tenant_id, id)
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
create index keepsakes_due_fulfilled_expiry
  on keepsakes (tenant_id, relation_id, subject_kind, subject_id, id)
  where state = 'applied' and json_extract(expiry_policy, '$.type') = 'when_fulfilled';

create table keepsake_fulfillment_counters (
  tenant_id text not null check (length(cast(tenant_id as blob)) > 0 and length(cast(tenant_id as blob)) <= 255),
  keepsake_id text not null,
  key text not null,
  value integer not null,
  observed_at text not null,
  primary key (tenant_id, keepsake_id, key),
  foreign key (tenant_id, keepsake_id)
    references keepsakes(tenant_id, id) on delete cascade
);
create index keepsake_fulfillment_counter_scan
  on keepsake_fulfillment_counters (tenant_id, key, value, keepsake_id);

create table keepsake_fulfillment_checklist (
  tenant_id text not null check (length(cast(tenant_id as blob)) > 0 and length(cast(tenant_id as blob)) <= 255),
  keepsake_id text not null,
  item text not null,
  complete integer not null,
  observed_at text not null,
  primary key (tenant_id, keepsake_id, item),
  foreign key (tenant_id, keepsake_id)
    references keepsakes(tenant_id, id) on delete cascade
);
create index keepsake_fulfillment_checklist_scan
  on keepsake_fulfillment_checklist (tenant_id, item, complete, keepsake_id);

-- SQLite uses triggers for the cross-column lifecycle invariants that are
-- expressed as checks in the Postgres and MySQL baselines.
create trigger keepsakes_clean_invariants_insert
before insert on keepsakes
for each row
when coalesce(not (
  json_extract(new.expiry_policy, '$.type') in ('manual_only', 'at', 'when_fulfilled')
  and (
    (json_extract(new.expiry_policy, '$.type') = 'at' and new.expires_at is not null and
      (case when instr(json_extract(new.expiry_policy, '$.timestamp'), '.') = 0
        then replace(json_extract(new.expiry_policy, '$.timestamp'), 'Z', '.000000Z')
        else substr(json_extract(new.expiry_policy, '$.timestamp'), 1, instr(json_extract(new.expiry_policy, '$.timestamp'), '.'))
          || substr(substr(json_extract(new.expiry_policy, '$.timestamp'), instr(json_extract(new.expiry_policy, '$.timestamp'), '.') + 1,
            instr(json_extract(new.expiry_policy, '$.timestamp'), 'Z') - instr(json_extract(new.expiry_policy, '$.timestamp'), '.') - 1) || '000000', 1, 6) || 'Z'
      end) = new.expires_at)
    or (json_extract(new.expiry_policy, '$.type') in ('manual_only', 'when_fulfilled') and new.expires_at is null)
  )
  and (
    (new.state = 'applied' and new.revoked_at is null and new.fulfilled_at is null)
    or (new.state = 'revoked' and new.revoked_at is not null and new.fulfilled_at is null)
    or (new.state = 'expired' and new.revoked_at is null and (
      (json_extract(new.expiry_policy, '$.type') = 'at' and new.expires_at is not null and new.fulfilled_at is null)
      or (json_extract(new.expiry_policy, '$.type') = 'when_fulfilled' and new.fulfilled_at is not null and new.expires_at is null)
    ))
  )
), 1)
begin
  select raise(abort, 'keepsakes_clean_invariants');
end;

create trigger keepsakes_clean_invariants_update
before update on keepsakes
for each row
when coalesce(not (
  json_extract(new.expiry_policy, '$.type') in ('manual_only', 'at', 'when_fulfilled')
  and (
    (json_extract(new.expiry_policy, '$.type') = 'at' and new.expires_at is not null and
      (case when instr(json_extract(new.expiry_policy, '$.timestamp'), '.') = 0
        then replace(json_extract(new.expiry_policy, '$.timestamp'), 'Z', '.000000Z')
        else substr(json_extract(new.expiry_policy, '$.timestamp'), 1, instr(json_extract(new.expiry_policy, '$.timestamp'), '.'))
          || substr(substr(json_extract(new.expiry_policy, '$.timestamp'), instr(json_extract(new.expiry_policy, '$.timestamp'), '.') + 1,
            instr(json_extract(new.expiry_policy, '$.timestamp'), 'Z') - instr(json_extract(new.expiry_policy, '$.timestamp'), '.') - 1) || '000000', 1, 6) || 'Z'
      end) = new.expires_at)
    or (json_extract(new.expiry_policy, '$.type') in ('manual_only', 'when_fulfilled') and new.expires_at is null)
  )
  and (
    (new.state = 'applied' and new.revoked_at is null and new.fulfilled_at is null)
    or (new.state = 'revoked' and new.revoked_at is not null and new.fulfilled_at is null)
    or (new.state = 'expired' and new.revoked_at is null and (
      (json_extract(new.expiry_policy, '$.type') = 'at' and new.expires_at is not null and new.fulfilled_at is null)
      or (json_extract(new.expiry_policy, '$.type') = 'when_fulfilled' and new.fulfilled_at is not null and new.expires_at is null)
    ))
  )
), 1)
begin
  select raise(abort, 'keepsakes_clean_invariants');
end;
