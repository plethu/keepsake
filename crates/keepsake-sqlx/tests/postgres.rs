#![allow(missing_docs)]
#![cfg(feature = "postgres-tests")]
//! Docker-backed Postgres integration tests.

#[path = "postgres/apply.rs"]
mod apply;
#[path = "postgres/expiry.rs"]
mod expiry;
#[path = "postgres/queries.rs"]
mod queries;
#[path = "postgres/relations.rs"]
mod relations;
#[path = "postgres/support.rs"]
mod support;
