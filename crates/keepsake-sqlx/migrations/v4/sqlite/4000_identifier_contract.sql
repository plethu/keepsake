-- Keepsake 4.0 identifier contract. The published v3 baseline remains
-- immutable; this migration is the v3 -> v4 runtime track.
--
-- SQLite has no ALTER CONSTRAINT operation. These triggers enforce the same
-- byte ceiling and edge-whitespace rule while retaining the v3 table layout.
create trigger keepsake_relation_definitions_identifier_contract_insert
before insert on keepsake_relation_definitions
for each row when not (
  length(cast(new.tenant_id as blob)) > 0 and length(cast(new.tenant_id as blob)) <= 191
  and trim(new.tenant_id) = new.tenant_id
  and length(cast(new.kind as blob)) > 0 and length(cast(new.kind as blob)) <= 191
  and trim(new.kind) = new.kind
  and length(cast(new.key as blob)) > 0 and length(cast(new.key as blob)) <= 191
  and trim(new.key) = new.key
) begin select raise(abort, 'keepsake_identifier_contract'); end;
create trigger keepsake_relation_definitions_identifier_contract_update
before update on keepsake_relation_definitions
for each row when not (
  length(cast(new.tenant_id as blob)) > 0 and length(cast(new.tenant_id as blob)) <= 191
  and trim(new.tenant_id) = new.tenant_id
  and length(cast(new.kind as blob)) > 0 and length(cast(new.kind as blob)) <= 191
  and trim(new.kind) = new.kind
  and length(cast(new.key as blob)) > 0 and length(cast(new.key as blob)) <= 191
  and trim(new.key) = new.key
) begin select raise(abort, 'keepsake_identifier_contract'); end;

create trigger keepsakes_identifier_contract_insert
before insert on keepsakes
for each row when not (
  length(cast(new.tenant_id as blob)) > 0 and length(cast(new.tenant_id as blob)) <= 191
  and trim(new.tenant_id) = new.tenant_id
  and length(cast(new.subject_kind as blob)) > 0 and length(cast(new.subject_kind as blob)) <= 191
  and trim(new.subject_kind) = new.subject_kind
  and length(cast(new.subject_id as blob)) > 0 and length(cast(new.subject_id as blob)) <= 191
  and trim(new.subject_id) = new.subject_id
) begin select raise(abort, 'keepsake_identifier_contract'); end;
create trigger keepsakes_identifier_contract_update
before update on keepsakes
for each row when not (
  length(cast(new.tenant_id as blob)) > 0 and length(cast(new.tenant_id as blob)) <= 191
  and trim(new.tenant_id) = new.tenant_id
  and length(cast(new.subject_kind as blob)) > 0 and length(cast(new.subject_kind as blob)) <= 191
  and trim(new.subject_kind) = new.subject_kind
  and length(cast(new.subject_id as blob)) > 0 and length(cast(new.subject_id as blob)) <= 191
  and trim(new.subject_id) = new.subject_id
) begin select raise(abort, 'keepsake_identifier_contract'); end;

create trigger keepsake_fulfillment_counters_identifier_contract_insert
before insert on keepsake_fulfillment_counters
for each row when not (
  length(cast(new.tenant_id as blob)) > 0 and length(cast(new.tenant_id as blob)) <= 191
  and trim(new.tenant_id) = new.tenant_id
  and length(cast(new.key as blob)) > 0 and length(cast(new.key as blob)) <= 191
  and trim(new.key) = new.key
) begin select raise(abort, 'keepsake_identifier_contract'); end;
create trigger keepsake_fulfillment_counters_identifier_contract_update
before update on keepsake_fulfillment_counters
for each row when not (
  length(cast(new.tenant_id as blob)) > 0 and length(cast(new.tenant_id as blob)) <= 191
  and trim(new.tenant_id) = new.tenant_id
  and length(cast(new.key as blob)) > 0 and length(cast(new.key as blob)) <= 191
  and trim(new.key) = new.key
) begin select raise(abort, 'keepsake_identifier_contract'); end;

create trigger keepsake_fulfillment_checklist_identifier_contract_insert
before insert on keepsake_fulfillment_checklist
for each row when not (
  length(cast(new.tenant_id as blob)) > 0 and length(cast(new.tenant_id as blob)) <= 191
  and trim(new.tenant_id) = new.tenant_id
  and length(cast(new.item as blob)) > 0 and length(cast(new.item as blob)) <= 191
  and trim(new.item) = new.item
) begin select raise(abort, 'keepsake_identifier_contract'); end;
create trigger keepsake_fulfillment_checklist_identifier_contract_update
before update on keepsake_fulfillment_checklist
for each row when not (
  length(cast(new.tenant_id as blob)) > 0 and length(cast(new.tenant_id as blob)) <= 191
  and trim(new.tenant_id) = new.tenant_id
  and length(cast(new.item as blob)) > 0 and length(cast(new.item as blob)) <= 191
  and trim(new.item) = new.item
) begin select raise(abort, 'keepsake_identifier_contract'); end;

update keepsake_schema_metadata set value = '4' where key = 'api_track';
