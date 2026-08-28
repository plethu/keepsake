-- Keepsake v2 -> v3 activation. Run only after an operator-owned mapping has
-- populated every tenant_id. This script deliberately refuses to infer one.
do $$
begin
  if exists (select 1 from keepsake_relation_definitions where tenant_id is null)
     or exists (select 1 from keepsakes where tenant_id is null)
     or exists (select 1 from keepsake_fulfillment_counters where tenant_id is null)
     or exists (select 1 from keepsake_fulfillment_checklist where tenant_id is null) then
    raise exception 'Keepsake tenant backfill is incomplete';
  end if;
end
$$;

alter table keepsakes drop constraint keepsakes_relation_id_fkey;
alter table keepsake_fulfillment_counters drop constraint keepsake_fulfillment_counters_keepsake_id_fkey;
alter table keepsake_fulfillment_checklist drop constraint keepsake_fulfillment_checklist_keepsake_id_fkey;
alter table keepsake_relation_definitions drop constraint keepsake_relation_definitions_pkey;
alter table keepsakes drop constraint keepsakes_pkey;
alter table keepsake_fulfillment_counters drop constraint keepsake_fulfillment_counters_pkey;
alter table keepsake_fulfillment_checklist drop constraint keepsake_fulfillment_checklist_pkey;

alter table keepsake_relation_definitions
  alter column tenant_id type text collate "C" using tenant_id::text,
  alter column tenant_id set not null,
  add constraint keepsake_relation_definitions_tenant_size check (octet_length(tenant_id) <= 255),
  add constraint keepsake_relation_definitions_tenant_nonempty check (octet_length(tenant_id) > 0),
  add primary key (tenant_id, id);
alter table keepsakes
  alter column tenant_id type text collate "C" using tenant_id::text,
  alter column tenant_id set not null,
  add constraint keepsakes_tenant_size check (octet_length(tenant_id) <= 255),
  add constraint keepsakes_tenant_nonempty check (octet_length(tenant_id) > 0),
  add primary key (tenant_id, id),
  add constraint keepsakes_relation_fk foreign key (tenant_id, relation_id)
    references keepsake_relation_definitions (tenant_id, id);
alter table keepsake_fulfillment_counters
  alter column tenant_id type text collate "C" using tenant_id::text,
  alter column tenant_id set not null,
  add constraint keepsake_fulfillment_counter_tenant_size check (octet_length(tenant_id) <= 255),
  add constraint keepsake_fulfillment_counter_tenant_nonempty check (octet_length(tenant_id) > 0),
  add primary key (tenant_id, keepsake_id, key),
  add constraint keepsake_fulfillment_counter_keepsake_fk
    foreign key (tenant_id, keepsake_id) references keepsakes (tenant_id, id) on delete cascade;
alter table keepsake_fulfillment_checklist
  alter column tenant_id type text collate "C" using tenant_id::text,
  alter column tenant_id set not null,
  add constraint keepsake_fulfillment_checklist_tenant_size check (octet_length(tenant_id) <= 255),
  add constraint keepsake_fulfillment_checklist_tenant_nonempty check (octet_length(tenant_id) > 0),
  add primary key (tenant_id, keepsake_id, item),
  add constraint keepsake_fulfillment_checklist_keepsake_fk
    foreign key (tenant_id, keepsake_id) references keepsakes (tenant_id, id) on delete cascade;

alter table keepsake_relation_definitions drop constraint keepsake_relation_definitions_kind_key_key;
alter table keepsake_relation_definitions add constraint keepsake_relation_definitions_tenant_key
  unique (tenant_id, kind, key);

drop index if exists keepsakes_one_active_relation_per_subject;
drop index if exists keepsakes_active_subject_lookup;
drop index if exists keepsakes_active_relation_membership;
drop index if exists keepsakes_due_timed_expiry;
drop index if exists keepsakes_due_fulfilled_expiry;
drop index if exists keepsake_fulfillment_counter_scan;
drop index if exists keepsake_fulfillment_checklist_scan;

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
  where state = 'applied' and expiry_policy->>'type' = 'when_fulfilled';
create index keepsake_fulfillment_counter_scan
  on keepsake_fulfillment_counters (tenant_id, key, value, keepsake_id);
create index keepsake_fulfillment_checklist_scan
  on keepsake_fulfillment_checklist (tenant_id, item, complete, keepsake_id);

update keepsake_schema_metadata set value = '3' where key = 'api_track';
