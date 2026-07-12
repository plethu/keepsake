#!/usr/bin/env bash
set -euo pipefail

if ! repo_root="$(git rev-parse --show-toplevel 2>/dev/null)"; then
  echo "unable to resolve git repo root from current directory" >&2
  exit 2
fi

: "${DATABASE_URL:?DATABASE_URL must point to a running Postgres database}"
: "${MYSQL_DATABASE_URL:?MYSQL_DATABASE_URL must point to a running MySQL database}"

cd "$repo_root"

echo "== Postgres integration tests =="
cargo test \
  -p keepsake-sqlx \
  --test postgres \
  --features postgres-tests \
  -- \
  --ignored \
  --test-threads=1

echo
echo "== MySQL integration tests =="
cargo test \
  -p keepsake-sqlx \
  --test mysql \
  --features mysql-tests \
  -- \
  --ignored \
  --test-threads=1

echo
echo "Keepsake database gates passed."
