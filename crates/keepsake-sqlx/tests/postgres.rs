#![allow(missing_docs)]
#![cfg(feature = "postgres-tests")]
//! Docker-backed Postgres integration tests.

#[path = "postgres/apply.rs"]
mod apply;
#[path = "postgres/audit.rs"]
mod audit;
#[path = "postgres/expiry.rs"]
mod expiry;
#[path = "postgres/queries.rs"]
mod queries;
#[path = "postgres/relations.rs"]
mod relations;
#[path = "postgres/schema.rs"]
mod schema;
#[path = "postgres/support.rs"]
mod support;
#[path = "postgres/tenancy.rs"]
mod tenancy;
