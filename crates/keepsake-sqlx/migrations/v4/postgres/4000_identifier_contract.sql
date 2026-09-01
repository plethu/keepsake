-- Keepsake 4.0 identifier contract. The published v3 baseline remains
-- immutable; this migration is the v3 -> v4 runtime track.
--
-- PostgreSQL's C collation is bytewise for these identity columns. The
-- octet-length checks provide the cross-backend 191 UTF-8-byte ceiling; the
-- adapter performs the stricter Unicode whitespace/control validation before
-- writes and decodes.
alter table keepsake_relation_definitions
  alter column tenant_id type text collate "C" using tenant_id,
  alter column kind type text collate "C" using kind,
  alter column key type text collate "C" using key;
alter table keepsakes
  alter column tenant_id type text collate "C" using tenant_id,
  alter column subject_kind type text collate "C" using subject_kind,
  alter column subject_id type text collate "C" using subject_id;
alter table keepsake_fulfillment_counters
  alter column tenant_id type text collate "C" using tenant_id,
  alter column key type text collate "C" using key;
alter table keepsake_fulfillment_checklist
  alter column tenant_id type text collate "C" using tenant_id,
  alter column item type text collate "C" using item;

alter table keepsake_relation_definitions
  add constraint keepsake_relation_definitions_identifier_contract check (
    octet_length(tenant_id) > 0 and octet_length(tenant_id) <= 191
    and tenant_id = btrim(tenant_id)
    and octet_length(kind) > 0 and octet_length(kind) <= 191
    and kind = btrim(kind)
    and octet_length(key) > 0 and octet_length(key) <= 191
    and key = btrim(key)
  );
alter table keepsakes
  add constraint keepsakes_identifier_contract check (
    octet_length(tenant_id) > 0 and octet_length(tenant_id) <= 191
    and tenant_id = btrim(tenant_id)
    and octet_length(subject_kind) > 0 and octet_length(subject_kind) <= 191
    and subject_kind = btrim(subject_kind)
    and octet_length(subject_id) > 0 and octet_length(subject_id) <= 191
    and subject_id = btrim(subject_id)
  );
alter table keepsake_fulfillment_counters
  add constraint keepsake_fulfillment_counter_identifier_contract check (
    octet_length(tenant_id) > 0 and octet_length(tenant_id) <= 191
    and tenant_id = btrim(tenant_id)
    and octet_length(key) > 0 and octet_length(key) <= 191
    and key = btrim(key)
  );
alter table keepsake_fulfillment_checklist
  add constraint keepsake_fulfillment_checklist_identifier_contract check (
    octet_length(tenant_id) > 0 and octet_length(tenant_id) <= 191
    and tenant_id = btrim(tenant_id)
    and octet_length(item) > 0 and octet_length(item) <= 191
    and item = btrim(item)
  );

update keepsake_schema_metadata set value = '4' where key = 'api_track';
