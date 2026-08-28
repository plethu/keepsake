-- Keepsake v2 -> v3 preparation. This is an operator-run artifact, not part
-- of the clean-install migrator. It adds nullable columns only; the operator
-- must backfill every row from a reviewed tenant mapping before activation.
alter table keepsake_relation_definitions add column tenant_id text collate "C";
alter table keepsakes add column tenant_id text collate "C";
alter table keepsake_fulfillment_counters add column tenant_id text collate "C";
alter table keepsake_fulfillment_checklist add column tenant_id text collate "C";
