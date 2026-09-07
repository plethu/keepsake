//! mysql persistence contract tests.

#![cfg(feature = "mysql-tests")]

mod mysql {
    mod audit;
    mod fulfillment;
    mod lifecycle;
    mod migrations;
    mod queries;
    mod relations;
    mod schema;
    mod support;
    mod tenancy;
    mod transactions;
}
