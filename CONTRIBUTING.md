# Contributing

Run the local gates before opening a change:

```sh
make check
```

Run database tests with Docker:

```sh
make test-db
```

Tool versions are pinned in `.mise.toml`:

```sh
mise install
```

When a change touches the public surface, lean on what keeps the project healthy
over time: maintainability, reliability, interoperability, and a workflow other
contributors can follow.

Issues are welcome for bugs, gaps in the docs, missing features, and anything
that bites you in operation.

A heads-up on pull requests: I can't promise timely review on unsolicited
patches right now, so please open an issue before you put real work into one.
That way we can agree on the shape of a change before you spend an evening on
it.
