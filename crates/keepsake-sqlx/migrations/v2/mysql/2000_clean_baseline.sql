-- Keepsake 2.0 clean baseline. Audit persistence belongs to Dovecote.
create table keepsake_schema_metadata (
  `key` varchar(191) primary key,
  value varchar(191) not null
);
insert into keepsake_schema_metadata (`key`, value) values ('backend', 'mysql');
insert into keepsake_schema_metadata (`key`, value) values ('api_track', '2');

create table keepsake_relation_definitions (
  id char(36) primary key,
  kind varchar(191) not null,
  `key` varchar(191) not null,
  enabled boolean not null default true,
  expiry_policy json not null,
  created_at datetime(6) not null,
  updated_at datetime(6) not null,
  constraint keepsake_relation_definitions_kind_key_unique unique (kind, `key`)
);
create table keepsakes (
  id char(36) primary key,
  subject_kind varchar(191) not null,
  subject_id varchar(191) not null,
  relation_id char(36) not null,
  state varchar(16) not null constraint keepsakes_state_check check (state in ('applied', 'revoked', 'expired')),
  expiry_policy json not null,
  applied_at datetime(6) not null,
  expires_at datetime(6),
  fulfilled_at datetime(6),
  revoked_at datetime(6),
  metadata json not null,
  created_at datetime(6) not null,
  updated_at datetime(6) not null,
  active_relation_key char(36) generated always as (case when state = 'applied' then relation_id end) stored,
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
  constraint keepsakes_relation_fk foreign key (relation_id) references keepsake_relation_definitions(id),
  constraint keepsakes_one_active_relation_per_subject unique (subject_kind, subject_id, active_relation_key)
);
create index keepsakes_active_subject_lookup on keepsakes (subject_kind, subject_id, relation_id, id);
create index keepsakes_active_relation_membership on keepsakes (relation_id, subject_kind, subject_id, id);
create index keepsakes_due_timed_expiry on keepsakes (expires_at, relation_id, subject_kind, subject_id, id);

create table keepsake_fulfillment_counters (
  keepsake_id char(36) not null,
  `key` varchar(191) not null,
  value bigint not null,
  observed_at datetime(6) not null,
  primary key (keepsake_id, `key`),
  constraint keepsake_fulfillment_counters_keepsake_fk foreign key (keepsake_id)
    references keepsakes(id) on delete cascade
);
create index keepsake_fulfillment_counter_scan on keepsake_fulfillment_counters (`key`, value, keepsake_id);
alter table keepsakes add column fulfillment_pending tinyint generated always as (
  case when state = 'applied' and json_unquote(json_extract(expiry_policy, '$.type')) = 'when_fulfilled' then 1 end
) stored;
create index keepsakes_due_fulfilled_expiry on keepsakes (fulfillment_pending, relation_id, subject_kind, subject_id, id);
create table keepsake_fulfillment_checklist (
  keepsake_id char(36) not null,
  item varchar(191) not null,
  complete tinyint(1) not null,
  observed_at datetime(6) not null,
  primary key (keepsake_id, item),
  constraint keepsake_fulfillment_checklist_keepsake_fk foreign key (keepsake_id)
    references keepsakes(id) on delete cascade
);
create index keepsake_fulfillment_checklist_scan on keepsake_fulfillment_checklist (item, complete, keepsake_id);
