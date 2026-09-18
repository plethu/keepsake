# Contributing

Keepsake is stable infrastructure for relation lifecycles. The main consumers
are Rust services and policy adapters such as gatekeep that read active relation
state through `ActiveRelationSource`.

## Before you open a PR

Install the pinned project tools once:

```sh
mise install
```

Run the local gates:

```sh
mise run check
```

When a change touches SQLx, migrations, or database queries, also run:

```sh
mise run test-db
```

The canonical gate includes cargo-machete unused-dependency checks, strict API
documentation, TOML validation and spelling. Production-only restrictions allow
test harnesses to use normal fixture assertions and arithmetic. For a focused
test, run `mise exec -- just test <filter> -- --nocapture`.

Run `mise run fmt` to format the workspace and `mise tasks` to list the
available project commands.

CI runs `mise run check` on pull requests via GitHub Actions. The same command
is the local release gate.

## Finding your way around

- `crates/keepsake` owns relation definitions, lifecycle commands, and audit values.
- `crates/keepsake-sqlx` owns persistence and backend-specific transactions.
- `examples/postgres-tags` shows schema setup through the first audited write.
- `examples/guide-tests` compiles the guide examples.

## Stability

From 1.0 onward, public API and schema changes follow semver. Open an issue
before proposing breaking changes.

## Docs

Human guides live in [`docs/README.md`](docs/README.md). API detail belongs in rustdoc
and on docs.rs. Update both when you change public behaviour.

## Issues and pull requests

Contributions are welcome, including bug reports, documentation improvements,
and changes to the code. Open an issue before spending substantial time on a
pull request so we can agree on the shape of the work before you implement it.

Tools, including generative AI, may help you write code, tests, or
documentation. You remain responsible for understanding and checking everything
you submit. Interpersonal communication must be your own work: please write
issue reports, pull request descriptions, review replies, and other comments
yourself rather than generating them or pasting generated prose.

A contribution is a conversation, not a drop-off. Please be willing to respond
to questions, consider review feedback, and revise the work with the
maintainers. A pull request does not need to arrive perfect; it does need
someone present on the other side of it.

## API compatibility

Run `mise exec -- just check-public-api` before changing public APIs. CI runs
this separately from the local `check` gate because it builds published baselines
from crates.io. The pinned versions live in `scripts/check-public-api.sh`;
default, no-default, and all-feature surfaces are compared. Update those
baselines after publication. Use `just check-public-api major` only for an
intentional breaking release; major mode permits breaks and is not a compatibility
check. Database and wire compatibility remain covered by their own tests.
