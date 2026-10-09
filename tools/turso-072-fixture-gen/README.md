# Turso 0.7.2 fixture generator

Writes the on-disk files that
`crates/cayenne/tests/turso_0_7_2_open_compat_test.rs` opens with Turso 0.8.1.

This crate is **not** a workspace member. The workspace pins `turso = 0.8.1`;
adding this package to the workspace would unify the dependency and the files
would no longer be 0.7.2.

## Regenerate

From this directory, with network access to crates.io:

```bash
cargo run --release
```

Output: `crates/cayenne/tests/fixtures/turso_0_7_2/*.fixture` plus `MANIFEST.txt`.
The `.fixture` suffix keeps the repo `.gitignore` (`*.db`, `*.db-log`) from
dropping the checked-in copies. The compat test strips the suffix into a
temp directory before opening.
