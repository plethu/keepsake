-- Keepsake 3.0 clean tenant-aware baseline. Published v1 and v2 migration
-- bytes are immutable; an existing v2 installation must use the explicit
-- prepare/backfill/activate route instead.
create table keepsake_schema_metadata (
  `key` varchar(191) primary key,
  value varchar(191) not null
);
insert into keepsake_schema_metadata (`key`, value) values ('backend', 'mysql');
insert into keepsake_schema_metadata (`key`, value) values ('api_track', '3');

create table keepsake_relation_definitions (
  tenant_id varbinary(255) not null,
  id varchar(36) not null,
  kind varchar(191) not null,
  `key` varchar(191) not null,
  enabled boolean not null default true,
  expiry_policy json not null,
  created_at datetime(6) not null,
  updated_at datetime(6) not null,
  constraint keepsake_relation_definitions_tenant_size check (octet_length(tenant_id) <= 255),
  constraint keepsake_relation_definitions_tenant_nonempty check (octet_length(tenant_id) > 0),
  primary key (tenant_id, id),
  constraint keepsake_relation_definitions_tenant_key unique (tenant_id, kind, `key`)
);
create index keepsake_relation_definitions_tenant_key_idx
  on keepsake_relation_definitions (tenant_id, kind, `key`, id);

create table keepsakes (
  tenant_id varbinary(255) not null,
  id varchar(36) not null,
  subject_kind varchar(191) not null,
  subject_id varchar(191) not null,
  relation_id varchar(36) not null,
  state varchar(16) not null,
  expiry_policy json not null,
  applied_at datetime(6) not null,
  expires_at datetime(6),
  fulfilled_at datetime(6),
  revoked_at datetime(6),
  metadata json not null,
  created_at datetime(6) not null,
  updated_at datetime(6) not null,
  active_relation_key varchar(36) generated always as (case when state = 'applied' then relation_id end) stored,
  fulfillment_pending tinyint generated always as (
    case when state = 'applied' and json_unquote(json_extract(expiry_policy, '$.type')) = 'when_fulfilled' then 1 end
  ) stored,
  constraint keepsakes_state_check check (state in ('applied', 'revoked', 'expired')),
  constraint keepsakes_tenant_size check (octet_length(tenant_id) <= 255),
  constraint keepsakes_tenant_nonempty check (octet_length(tenant_id) > 0),
  constraint keepsakes_expiry_policy_projection check ((
    json_unquote(json_extract(expiry_policy, '$.type')) in ('manual_only', 'at', 'when_fulfilled')
    and ((json_unquote(json_extract(expiry_policy, '$.type')) = 'at' and expires_at is not null
      and cast(replace(replace(json_unquote(json_extract(expiry_policy, '$.timestamp')), 'T', ' '), 'Z', '') as datetime(6)) = expires_at)
      or (json_unquote(json_extract(expiry_policy, '$.type')) in ('manual_only', 'when_fulfilled') and expires_at is null))
  ) is true),
  constraint keepsakes_lifecycle_timestamps check ((
    (state = 'applied' and revoked_at is null and fulfilled_at is null)
    or (state = 'revoked' and revoked_at is not null and fulfilled_at is null)
    or (state = 'expired' and revoked_at is null and ((json_unquote(json_extract(expiry_policy, '$.type')) = 'at' and expires_at is not null and fulfilled_at is null)
      or (json_unquote(json_extract(expiry_policy, '$.type')) = 'when_fulfilled' and fulfilled_at is not null and expires_at is null)))
  ) is true),
  primary key (tenant_id, id),
  constraint keepsakes_relation_fk foreign key (tenant_id, relation_id)
    references keepsake_relation_definitions(tenant_id, id),
  constraint keepsakes_one_active_relation_per_subject unique (tenant_id, subject_kind, subject_id, active_relation_key)
);
create index keepsakes_active_subject_lookup
  on keepsakes (tenant_id, subject_kind, subject_id, relation_id, id);
create index keepsakes_active_relation_membership
  on keepsakes (tenant_id, relation_id, subject_kind, subject_id, id);
create index keepsakes_due_timed_expiry
  on keepsakes (tenant_id, expires_at, relation_id, subject_kind, subject_id, id);
create index keepsakes_due_fulfilled_expiry
  on keepsakes (tenant_id, fulfillment_pending, relation_id, subject_kind, subject_id, id);

create table keepsake_fulfillment_counters (
  tenant_id varbinary(255) not null,
  keepsake_id varchar(36) not null,
  `key` varchar(191) not null,
  value bigint not null,
  observed_at datetime(6) not null,
  constraint keepsake_fulfillment_counter_tenant_size check (octet_length(tenant_id) <= 255),
  constraint keepsake_fulfillment_counter_tenant_nonempty check (octet_length(tenant_id) > 0),
  primary key (tenant_id, keepsake_id, `key`),
  constraint keepsake_fulfillment_counter_keepsake_fk foreign key (tenant_id, keepsake_id)
    references keepsakes(tenant_id, id) on delete cascade
);
create index keepsake_fulfillment_counter_scan
  on keepsake_fulfillment_counters (tenant_id, `key`, value, keepsake_id);

create table keepsake_fulfillment_checklist (
  tenant_id varbinary(255) not null,
  keepsake_id varchar(36) not null,
  item varchar(191) not null,
  complete tinyint(1) not null,
  observed_at datetime(6) not null,
  constraint keepsake_fulfillment_checklist_tenant_size check (octet_length(tenant_id) <= 255),
  constraint keepsake_fulfillment_checklist_tenant_nonempty check (octet_length(tenant_id) > 0),
  primary key (tenant_id, keepsake_id, item),
  constraint keepsake_fulfillment_checklist_keepsake_fk foreign key (tenant_id, keepsake_id)
    references keepsakes(tenant_id, id) on delete cascade
);
create index keepsake_fulfillment_checklist_scan
  on keepsake_fulfillment_checklist (tenant_id, item, complete, keepsake_id);
