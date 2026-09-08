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
    #[path = "../support/timed_relation_scope.rs"]
    mod timed_relation_scope;
    mod transactions;
}
