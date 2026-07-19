# Structural Rust checks

These rules cover a few structural patterns that rustfmt and Clippy do not
express well. Run them with:

```sh
mise run lint-structure
```

Error-severity rules fail the task. Warnings need review and may be intentional.

| Signal | Tool | Severity | Threshold |
| --- | --- | --- | --- |
| Deep block nesting | Clippy `excessive_nesting` | error | `4` |
| Long `if` / `else if` cascade | `rust-elseif-cascade` | error | 3 branches |
| Ordered `if let Some(...)` cascade | `rust-if-let-policy-cascade` | warning | 2 guards |
| Dense `if let Some(...)` cascade | `rust-if-let-policy-cascade-dense` | error | 3 guards |
| Empty match arm or bare return | `rust-empty-match-noop` | warning | — |
| Missing blank after control flow | `rust-block-spacing` | error | — |

`rust-block-spacing` runs after rustfmt. Rustfmt preserves intentional blank
lines after closing braces but has no stable option to insert them selectively.

Suppress a true positive next to the code and name the rule:

```rust
// ast-grep-ignore: rust-empty-match-noop
None => {}
```

For Clippy nesting, use `#[expect(clippy::excessive_nesting, reason = "...")]`.
Prefer a local, reasoned suppression over disabling a rule or excluding a broad
path.
