#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  check-project-gates.sh [repo-root]

Runs Keepsake's canonical local project gates:
  1. cargo fmt --all --check
  2. structural Rust checks
  3. cargo clippy --workspace --all-targets --all-features -- -D warnings
  4. keepsake-sqlx feature-matrix clippy checks
  5. cargo test --workspace --all-features
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
echo "== structural Rust checks =="
if command -v ast-grep >/dev/null 2>&1; then
  MISE_PROJECT_ROOT="$repo_root" "$repo_root/.config/mise/tasks/lint-structure"
elif command -v mise >/dev/null 2>&1; then
  (
    cd "$repo_root"
    MISE_PROJECT_ROOT="$repo_root" mise exec -- .config/mise/tasks/lint-structure
  )
else
  echo "ast-grep is unavailable; install the pinned tools with 'mise install'" >&2
  exit 2
fi

echo
echo "== cargo clippy =="
(
  cd "$repo_root"
  cargo clippy --workspace --all-targets --all-features -- -D warnings
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
echo "== cargo test =="
(
  cd "$repo_root"
  cargo test --workspace --all-features
)

echo
echo "Keepsake project gates passed."
