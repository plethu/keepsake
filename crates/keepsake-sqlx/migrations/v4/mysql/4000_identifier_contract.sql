-- Keepsake 4.0 identifier contract. The published v3 baseline remains
-- immutable; this migration is the v3 -> v4 runtime track.
--
-- utf8mb4_bin is deliberately explicit and is supported by both MySQL and
-- MariaDB. It preserves case and bytes for equality and unique-key checks.
alter table keepsakes drop foreign key keepsakes_relation_fk;
alter table keepsake_fulfillment_counters drop foreign key keepsake_fulfillment_counter_keepsake_fk;
alter table keepsake_fulfillment_checklist drop foreign key keepsake_fulfillment_checklist_keepsake_fk;

alter table keepsake_relation_definitions
  modify tenant_id varchar(191) character set utf8mb4 collate utf8mb4_bin not null,
  modify kind varchar(191) character set utf8mb4 collate utf8mb4_bin not null,
  modify `key` varchar(191) character set utf8mb4 collate utf8mb4_bin not null;
alter table keepsakes
  modify tenant_id varchar(191) character set utf8mb4 collate utf8mb4_bin not null,
  modify subject_kind varchar(191) character set utf8mb4 collate utf8mb4_bin not null,
  modify subject_id varchar(191) character set utf8mb4 collate utf8mb4_bin not null;
alter table keepsake_fulfillment_counters
  modify tenant_id varchar(191) character set utf8mb4 collate utf8mb4_bin not null,
  modify `key` varchar(191) character set utf8mb4 collate utf8mb4_bin not null;
alter table keepsake_fulfillment_checklist
  modify tenant_id varchar(191) character set utf8mb4 collate utf8mb4_bin not null,
  modify item varchar(191) character set utf8mb4 collate utf8mb4_bin not null;

alter table keepsake_relation_definitions
  add constraint keepsake_relation_definitions_identifier_contract check (
    octet_length(tenant_id) > 0 and octet_length(tenant_id) <= 191 and tenant_id = trim(tenant_id)
    and octet_length(kind) > 0 and octet_length(kind) <= 191 and kind = trim(kind)
    and octet_length(`key`) > 0 and octet_length(`key`) <= 191 and `key` = trim(`key`)
  );
alter table keepsakes
  add constraint keepsakes_identifier_contract check (
    octet_length(tenant_id) > 0 and octet_length(tenant_id) <= 191 and tenant_id = trim(tenant_id)
    and octet_length(subject_kind) > 0 and octet_length(subject_kind) <= 191
    and subject_kind = trim(subject_kind)
    and octet_length(subject_id) > 0 and octet_length(subject_id) <= 191
    and subject_id = trim(subject_id)
  );
alter table keepsake_fulfillment_counters
  add constraint keepsake_fulfillment_counter_identifier_contract check (
    octet_length(tenant_id) > 0 and octet_length(tenant_id) <= 191 and tenant_id = trim(tenant_id)
    and octet_length(`key`) > 0 and octet_length(`key`) <= 191 and `key` = trim(`key`)
  );
alter table keepsake_fulfillment_checklist
  add constraint keepsake_fulfillment_checklist_identifier_contract check (
    octet_length(tenant_id) > 0 and octet_length(tenant_id) <= 191 and tenant_id = trim(tenant_id)
    and octet_length(item) > 0 and octet_length(item) <= 191 and item = trim(item)
  );

alter table keepsakes
  add constraint keepsakes_relation_fk foreign key (tenant_id, relation_id)
    references keepsake_relation_definitions(tenant_id, id);
alter table keepsake_fulfillment_counters
  add constraint keepsake_fulfillment_counter_keepsake_fk foreign key (tenant_id, keepsake_id)
    references keepsakes(tenant_id, id) on delete cascade;
alter table keepsake_fulfillment_checklist
  add constraint keepsake_fulfillment_checklist_keepsake_fk foreign key (tenant_id, keepsake_id)
    references keepsakes(tenant_id, id) on delete cascade;

update keepsake_schema_metadata set value = '4' where `key` = 'api_track';
