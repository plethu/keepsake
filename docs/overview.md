# Overview

Keepsake stores relations that have lifecycle rules. A relation can be simple:
`account:acct_123` has the `tag:trusted` relation. It can also expire:
`account:acct_123` has the `sanction:mute_24h` relation until 24 hours after it
was applied.

Several services may need the same data:

- is this subject active in this relation right now?
- can a retry safely run the same apply command again?
- which rows are due for expiry?
- which subjects currently have this relation?
- where should audit history live?

Keepsake models these operations as one relation lifecycle. The core crate
provides the types, commands, expiry policies, evaluator, audit traits, and
errors. The SQLx adapter stores the state in Postgres and provides query
helpers for request paths and workers.

## Core Model

Keepsake stores opaque subjects and relation definitions. Your application
handles users, accounts, authorization, tenancy, display data, and product
workflows. Keepsake stores whether "this subject has this relation".

| Concept | Example | Source |
| --- | --- | --- |
| Subject | `account:acct_123` | Application |
| Relation | `tag:trusted` | Application catalogue, stored by Keepsake |
| Keepsake | active trusted tag for `acct_123` | Keepsake |
| Expiry policy | manual, timed, or fulfillment-based | Relation definition |
| Audit context | operator id, ticket id, reason code | Application values on Keepsake events |

Pass lifecycle inputs explicitly. Callers provide `now`, relation definitions,
current state, and any fulfillment snapshot. Retries, expiry workers, tests,
and audit records all use the same values.

## When To Use It

Use Keepsake when relation state needs retry-safe writes, queryable active
state, and deterministic expiry or revoke behavior. Good fits include tags,
sanctions, entitlements, holds, risk flags, feature gates, and similar
workflows.

Use application tables for display data, permissions, tenant scope, and product
context. Keepsake stores relation lifecycle state for opaque subjects.

Use the Rust crates directly when Rust and Postgres fit your service. Use the
docs as implementation guidance when another language, database, migration
framework, cache layer, or audit pipeline needs the same contracts.
