SHELL := /usr/bin/env bash

DATABASE_URL ?= postgres://keepsake:keepsake@localhost:55432/keepsake
DOCKER_COMPOSE ?= docker compose
PNPM_STORE_DIR ?= $(CURDIR)/.pnpm-store
PNPM_XDG_DIR ?= $(CURDIR)/.cache/xdg

.PHONY: fmt clippy test test-db db-up db-down docs docs-install check clean

fmt:
	cargo fmt --all --check

clippy:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

test:
	cargo test --workspace --all-features

db-up:
	$(DOCKER_COMPOSE) up -d --wait postgres

db-down:
	$(DOCKER_COMPOSE) down --remove-orphans

test-db: db-up
	DATABASE_URL="$(DATABASE_URL)" cargo test -p keepsake-sqlx --test postgres --features postgres-tests -- --ignored --test-threads=1

docs-install:
	XDG_DATA_HOME="$(PNPM_XDG_DIR)/data" XDG_STATE_HOME="$(PNPM_XDG_DIR)/state" NPM_CONFIG_STORE_DIR="$(PNPM_STORE_DIR)" pnpm --dir docs-site install --frozen-lockfile

docs:
	XDG_DATA_HOME="$(PNPM_XDG_DIR)/data" XDG_STATE_HOME="$(PNPM_XDG_DIR)/state" NPM_CONFIG_STORE_DIR="$(PNPM_STORE_DIR)" pnpm --dir docs-site build

check: fmt clippy test docs

clean:
	cargo clean
	rm -rf docs-site/dist docs-site/.astro
