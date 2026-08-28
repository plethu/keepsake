-- Keepsake v2 -> v3 activation. Run only after an operator-owned mapping has
-- populated every tenant_id. This script deliberately refuses to infer one.
create temporary table if not exists keepsake_upgrade_guard (
  complete tinyint not null check (complete = 1)
);
delete from keepsake_upgrade_guard;
insert into keepsake_upgrade_guard (complete)
select case when exists (select 1 from keepsake_relation_definitions where tenant_id is null)
  or exists (select 1 from keepsakes where tenant_id is null)
  or exists (select 1 from keepsake_fulfillment_counters where tenant_id is null)
  or exists (select 1 from keepsake_fulfillment_checklist where tenant_id is null)
  then 0 else 1 end;
drop temporary table keepsake_upgrade_guard;

  alter table keepsakes drop foreign key keepsakes_relation_fk;
  alter table keepsake_fulfillment_counters drop foreign key keepsake_fulfillment_counters_keepsake_fk;
  alter table keepsake_fulfillment_checklist drop foreign key keepsake_fulfillment_checklist_keepsake_fk;
  alter table keepsake_relation_definitions drop primary key;
  alter table keepsakes drop primary key;
  alter table keepsake_fulfillment_counters drop primary key;
  alter table keepsake_fulfillment_checklist drop primary key;
  alter table keepsake_relation_definitions drop index keepsake_relation_definitions_kind_key_unique;
  alter table keepsakes drop index keepsakes_one_active_relation_per_subject;
  alter table keepsakes drop index keepsakes_active_subject_lookup;
  alter table keepsakes drop index keepsakes_active_relation_membership;
  alter table keepsakes drop index keepsakes_due_timed_expiry;
  alter table keepsakes drop index keepsakes_due_fulfilled_expiry;
  alter table keepsake_fulfillment_counters drop index keepsake_fulfillment_counter_scan;
  alter table keepsake_fulfillment_checklist drop index keepsake_fulfillment_checklist_scan;

  -- MariaDB rejects conditional generated expressions that read CHAR
  -- columns. Keep UUID text wire values unchanged, but normalize the v3
  -- identifier columns to VARCHAR before rebuilding tenant-composite keys.
  alter table keepsake_relation_definitions
    modify id varchar(36) not null;
  alter table keepsakes
    modify id varchar(36) not null,
    modify relation_id varchar(36) not null,
    modify active_relation_key varchar(36) generated always as (case when state = 'applied' then relation_id end) stored;
  alter table keepsake_fulfillment_counters
    modify keepsake_id varchar(36) not null;
  alter table keepsake_fulfillment_checklist
    modify keepsake_id varchar(36) not null;

  alter table keepsake_relation_definitions
    modify tenant_id varbinary(255) not null,
    add constraint keepsake_relation_definitions_tenant_size check (octet_length(tenant_id) <= 255),
    add constraint keepsake_relation_definitions_tenant_nonempty check (octet_length(tenant_id) > 0),
    add primary key (tenant_id, id),
    add constraint keepsake_relation_definitions_tenant_key unique (tenant_id, kind, `key`);
  alter table keepsakes
    modify tenant_id varbinary(255) not null,
    add constraint keepsakes_tenant_size check (octet_length(tenant_id) <= 255),
    add constraint keepsakes_tenant_nonempty check (octet_length(tenant_id) > 0),
    add primary key (tenant_id, id),
    add constraint keepsakes_relation_fk foreign key (tenant_id, relation_id)
      references keepsake_relation_definitions(tenant_id, id),
    add constraint keepsakes_one_active_relation_per_subject unique (tenant_id, subject_kind, subject_id, active_relation_key);
  alter table keepsake_fulfillment_counters
    modify tenant_id varbinary(255) not null,
    add constraint keepsake_fulfillment_counter_tenant_size check (octet_length(tenant_id) <= 255),
    add constraint keepsake_fulfillment_counter_tenant_nonempty check (octet_length(tenant_id) > 0),
    add primary key (tenant_id, keepsake_id, `key`),
    add constraint keepsake_fulfillment_counter_keepsake_fk foreign key (tenant_id, keepsake_id)
      references keepsakes(tenant_id, id) on delete cascade;
  alter table keepsake_fulfillment_checklist
    modify tenant_id varbinary(255) not null,
    add constraint keepsake_fulfillment_checklist_tenant_size check (octet_length(tenant_id) <= 255),
    add constraint keepsake_fulfillment_checklist_tenant_nonempty check (octet_length(tenant_id) > 0),
    add primary key (tenant_id, keepsake_id, item),
    add constraint keepsake_fulfillment_checklist_keepsake_fk foreign key (tenant_id, keepsake_id)
      references keepsakes(tenant_id, id) on delete cascade;

  create index keepsake_relation_definitions_tenant_key_idx on keepsake_relation_definitions (tenant_id, kind, `key`, id);
  create index keepsakes_active_subject_lookup on keepsakes (tenant_id, subject_kind, subject_id, relation_id, id);
  create index keepsakes_active_relation_membership on keepsakes (tenant_id, relation_id, subject_kind, subject_id, id);
  create index keepsakes_due_timed_expiry on keepsakes (tenant_id, expires_at, relation_id, subject_kind, subject_id, id);
  create index keepsakes_due_fulfilled_expiry on keepsakes (tenant_id, fulfillment_pending, relation_id, subject_kind, subject_id, id);
  create index keepsake_fulfillment_counter_scan on keepsake_fulfillment_counters (tenant_id, `key`, value, keepsake_id);
  create index keepsake_fulfillment_checklist_scan on keepsake_fulfillment_checklist (tenant_id, item, complete, keepsake_id);
update keepsake_schema_metadata set value = '3' where `key` = 'api_track';
