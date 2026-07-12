SHELL := /usr/bin/env bash

DATABASE_URL ?= postgres://keepsake:keepsake@localhost:55432/keepsake
MYSQL_DATABASE_URL ?= mysql://keepsake:keepsake@localhost:53306/keepsake
DOCKER_COMPOSE ?= docker compose

.PHONY: fmt clippy test test-db db-up db-down check clean

fmt:
	cargo fmt --all --check

clippy:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

test:
	cargo test --workspace --all-features

db-up:
	$(DOCKER_COMPOSE) up -d --wait postgres mysql

db-down:
	$(DOCKER_COMPOSE) down --remove-orphans

test-db: db-up
	DATABASE_URL="$(DATABASE_URL)" MYSQL_DATABASE_URL="$(MYSQL_DATABASE_URL)" scripts/check-database-gates.sh

check:
	scripts/check-project-gates.sh

clean:
	cargo clean
