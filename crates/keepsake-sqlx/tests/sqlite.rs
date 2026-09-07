//! sqlite persistence contract tests.

#![cfg(feature = "sqlite-tests")]

mod sqlite {
    mod audit;
    mod expiry;
    mod fulfillment;
    mod lifecycle;
    mod migrations;
    mod queries;
    mod support;
    mod tenancy;
    mod transactions;
}
