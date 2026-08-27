#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
fixture_dir="$(mktemp -d "${TMPDIR:-/tmp}/keepsake-sqlite-migration.XXXXXX")"
database_path="$fixture_dir/keepsake.sqlite"
cleanup() {
  rm -rf "$fixture_dir"
}
trap cleanup EXIT

echo "== SQLite migration compatibility: bridge-enabled process =="
(
  cd "$repo_root"
  KEEPSAKE_SQLITE_MIGRATION_COMPAT_DB="$database_path" \
    cargo test -p keepsake-sqlx --test dovecote_bridge_sqlite_stage1 \
      --no-default-features --features sqlite,migrations,dovecote-sqlite \
      -- --exact bridge_enabled_migration_creates_the_file_backed_schema
)

echo
echo "== SQLite migration compatibility: bridge-disabled process =="
(
  cd "$repo_root"
  KEEPSAKE_SQLITE_MIGRATION_COMPAT_DB="$database_path" \
    cargo test -p keepsake-sqlx --test dovecote_bridge_sqlite_stage2 \
      --no-default-features --features sqlite,migrations \
      -- --exact bridge_disabled_migration_accepts_the_existing_file_backed_schema
)

echo
echo "SQLite migration compatibility passed in separately compiled processes."
