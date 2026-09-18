#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  check-project-gates.sh [repo-root]

Runs Keepsake's canonical local project gates:
  1. cargo fmt --all --check
  2. cargo clippy --workspace --all-targets --all-features -- -D warnings
  3. keepsake-sqlx feature-matrix clippy checks
  4. cargo test -p keepsake-sqlx --no-default-features --offline
  5. cargo test -p keepsake-sqlx --features sqlite-tests --test sqlite
  6. cargo deny advisory, ban, license, and source checks
  7. cargo test --workspace --all-features
  8. cargo machete
  9. production panic-result contract, strict rustdoc, TOML and spelling checks
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || "${1:-}" == "help" ]]; then
  usage
  exit 0
fi

input_root="${1:-}"
if [[ -n "$input_root" ]]; then
  if ! repo_root="$(git -C "$input_root" rev-parse --show-toplevel 2>/dev/null)"; then
    echo "repo root is not a git checkout: $input_root" >&2
    exit 2
  fi
else
  if ! repo_root="$(git rev-parse --show-toplevel 2>/dev/null)"; then
    echo "unable to resolve git repo root from current directory" >&2
    exit 2
  fi
fi

echo "== cargo fmt --all --check =="
(
  cd "$repo_root"
  cargo fmt --all --check
)

echo
echo "== cargo clippy =="
(
  cd "$repo_root"
  cargo clippy --workspace --all-targets --all-features -- -D warnings
)

echo
echo "== production panic-result contract =="
(
  cd "$repo_root"
  cargo clippy --workspace --lib --bins --all-features -- -D warnings -D clippy::panic_in_result_fn -D unreachable_pub
)

echo
echo "== strict API documentation =="
(
  cd "$repo_root"
  RUSTDOCFLAGS="${RUSTDOCFLAGS:+$RUSTDOCFLAGS }-D warnings" cargo doc --workspace --all-features --no-deps
)

echo
echo "== TOML and spelling =="
(
  cd "$repo_root"
  for tool in taplo typos; do
    if ! command -v "$tool" >/dev/null 2>&1; then
      echo "$tool is unavailable; run 'mise install' and invoke the gate through mise" >&2
      exit 2
    fi
  done
  taplo fmt --check
  taplo lint
  typos
)

echo
echo "== keepsake-sqlx feature matrix =="
(
  cd "$repo_root"
  feature_sets=(
    ""
    "postgres"
    "sqlite"
    "mysql"
    "postgres,cache,migrations"
    "sqlite,cache,migrations"
    "mysql,cache,migrations"
  )
  for features in "${feature_sets[@]}"; do
    args=(cargo clippy -p keepsake-sqlx --no-default-features)
    label="no features"
    if [[ -n "$features" ]]; then
      args+=(--features "$features")
      label="$features"
    fi
    echo "-- $label"
    "${args[@]}" -- -D warnings
  done
)

echo
echo "== keepsake-sqlx no-feature test build =="
(
  cd "$repo_root"
  cargo test -p keepsake-sqlx --no-default-features --offline
)

echo
echo "== keepsake-sqlx SQLite integration contract tests =="
(
  cd "$repo_root"
  cargo test -p keepsake-sqlx --features sqlite-tests --test sqlite
)

echo
echo "== cargo deny supply-chain checks =="
(
  cd "$repo_root"
  if command -v cargo-deny >/dev/null 2>&1; then
    cargo deny --all-features check advisories bans licenses sources
  elif command -v mise >/dev/null 2>&1; then
    mise exec -- cargo-deny --all-features check advisories bans licenses sources
  else
    echo "cargo-deny is unavailable; run 'mise install'" >&2
    exit 2
  fi
)

echo
echo "== cargo test =="
(
  cd "$repo_root"
  cargo test --workspace --all-features
)

echo
echo "== cargo machete unused-dependency checks =="
(
  cd "$repo_root"
  if command -v cargo-machete >/dev/null 2>&1; then
    cargo machete
  elif command -v mise >/dev/null 2>&1; then
    mise exec -- cargo-machete
  else
    echo "cargo-machete is unavailable; run 'mise install'" >&2
    exit 2
  fi
)

echo
echo "Keepsake project gates passed."
