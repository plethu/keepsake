# Contributing

Keepsake is stable infrastructure for relation lifecycles. The main consumers
are Rust services and policy adapters such as gatekeep that read active relation
state through `ActiveRelationSource`.

## Before you open a PR

Run the local gates:

```sh
make check
```

When a change touches SQLx, migrations, or database queries, also run:

```sh
make test-db
```

Tool versions are pinned in `.mise.toml`:

```sh
mise install
```

CI runs on pull requests via Codeberg-hosted Forgejo Actions. If Actions is
unavailable, `make check` is the release gate.

## Stability

From 1.0 onward, public API and schema changes follow semver. Open an issue
before proposing breaking changes.

## Docs

Human guides live in [`docs/README.md`](docs/README.md). API detail belongs in rustdoc
and on docs.rs. Update both when you change public behaviour.

## Issues and pull requests

Issues are welcome for bugs, doc gaps, and operational problems.

Open an issue before spending time on a pull request. That way we can agree on
scope before you implement it.
