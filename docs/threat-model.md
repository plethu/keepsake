# Threat model

This document describes the security assumptions and failure boundaries for
Keepsake. It is an engineering aid, not a security audit or compliance claim.

## Assets

- Relation state, including sanctions, entitlements, holds, and expiry state.
- Tenant ownership of each relation, command, audit event, and delivery.
- Audit provenance, occurrence identity, and delivery state.
- Database credentials, migration authority, and application data.

## Trust boundaries

The identity provider authenticates credentials and supplies a tenant claim.
The application verifies that claim, binds it to the request principal, and
constructs the Keepsake tenant scope. Keepsake validates the typed tenant
value and applies lifecycle rules. SQLx and Dovecote persist the scope and
must repeat it in predicates and event envelopes. Database RLS and connection
session settings are optional deployment controls, not assumptions made by
the core crate.

Keepsake does not verify JWT signatures, discover tenant membership, own
application tables, configure RLS, or execute policy obligations. Migration
tools and delivery workers are separate operational actors.

## Threats and controls

### Cross-tenant reads or writes

An application bug, hostile request, or reused background worker could supply
the same subject or UUID for another tenant. The core contract therefore
requires a validated `TenantId`, puts it on durable values and commands, and
requires provider queries to receive an explicit tenant. Tenant-aware SQLx and
Dovecote adapters must bind that value before every read or mutation and use
tenant-prefixed uniqueness and indexes.

The residual risk is an adapter or application query that bypasses the
tenant-aware API, uses raw SQL, or relies only on a pooled session setting.
Review such code separately and add database RLS as defense in depth.

### Forged tenant claims

Keepsake cannot distinguish an authenticated claim from an arbitrary string.
The application must validate the issuer, signature, audience, lifetime,
membership, and principal/tenant relationship before creating a scope. Keep
secrets and bearer tokens out of Keepsake values and audit events; record only
bounded provenance or evidence identifiers.

### Replay and stale authorization evidence

Audit occurrence IDs are retry identities, not authentication. Applications
must enforce binding freshness and decide whether a retry needs a newly
verified request binding. Dovecote consumers must preserve tenant routing,
deduplicate the documented `(tenant_id, source, event_id)` identity, and treat
delivery as at-least-once.

### Migration or export leakage

Tenant columns and indexes are schema invariants. A migration must not assign
a guessed default tenant. Existing rows require an explicit, reviewable
mapping before tenant-aware writers are enabled. Exports, CDC, admin scans,
metrics, and logs must preserve tenant scope and avoid putting raw subject or
tenant identifiers into broadly visible labels.

### Availability and integrity failures

Database outages, lock contention, malformed events, and failed audit writes
must remain visible to the application. Retry idempotently using the same
occurrence identity where the operation is logically the same. Do not treat a
successful enqueue or emitted obligation as proof that an external action was
executed.

## Operational checklist

Before production use, an application should verify identity-provider
configuration, tenant binding and freshness tests, explicit SQL predicates,
RLS/session hygiene, migration mappings, backup/restore scope, Dovecote
consumer deduplication, secret handling, and dependency-advisory results.
