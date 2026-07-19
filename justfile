set shell := ["bash", "-euo", "pipefail", "-c"]

database_url := env_var_or_default("DATABASE_URL", "postgres://keepsake:keepsake@localhost:55432/keepsake")
mysql_database_url := env_var_or_default("MYSQL_DATABASE_URL", "mysql://keepsake:keepsake@localhost:53306/keepsake")
docker_compose := env_var_or_default("DOCKER_COMPOSE", "docker compose")
test_db_up := env_var_or_default("TEST_DB_UP", "1")

fmt:
    cargo fmt --all

clippy:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

test:
    cargo test --workspace --all-features

db-up:
    {{ docker_compose }} up -d --wait postgres mysql

db-down:
    {{ docker_compose }} down --remove-orphans

test-db:
    if [[ "{{ test_db_up }}" == "1" ]]; then just db-up; fi
    DATABASE_URL="{{ database_url }}" MYSQL_DATABASE_URL="{{ mysql_database_url }}" scripts/check-database-gates.sh

check:
    scripts/check-project-gates.sh

clean:
    cargo clean
