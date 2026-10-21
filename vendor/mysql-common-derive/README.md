# mysql-common-derive (spiceai vendored fork)

Implements the `FromValue` and `FromRow` derive macros for `mysql_common`.

## Why this is vendored

This is a local, minimally-modified fork of [`mysql-common-derive`] **v0.32.1**
(upstream: <https://github.com/blackbeam/rust_mysql_common>), pulled into the
spiceai tree via `[patch.crates-io]` in the workspace root `Cargo.toml`.

Its **only** purpose is to drop the dependency on [`proc-macro-error2`], which is
unmaintained ([RUSTSEC-2026-0173]). `mysql-common-derive` is the sole consumer of
`proc-macro-error2` in the dependency tree (it is pulled in transitively via
`mysql_async` → `mysql_common` → `mysql-common-derive`), so removing it here
removes `proc-macro-error2` (and `proc-macro-error-attr2`) from the build
entirely and lets `cargo deny check advisories` pass.

## What changed vs. upstream 0.32.1

The `proc-macro-error2` `abort!`/`Diagnostic` usage was migrated to
`syn::Error`-based compile errors, which is the standard modern approach:

- `error.rs`: `impl From<Error> for proc_macro_error2::Diagnostic` →
  `impl From<Error> for syn::Error` (spanned; secondary spans via `combine`).
- `lib.rs`: removed the `#[proc_macro_error]` attribute from both derive entry
  points; errors are rendered with `syn::Error::to_compile_error()`.
- In `Result`-returning functions, `abort!(e)` became `return Err(e)`.
- In `ToTokens::to_tokens` implementations (crate-name resolution), `abort!(e)`
  became `tokens.extend(syn::Error::from(e).to_compile_error()); return;`.

The generated `FromValue` / `FromRow` derive output is unchanged for valid input;
only the *error-reporting path* differs (still a spanned compiler error, no longer
routed through `proc-macro-error2`).

To re-sync with a newer upstream release, re-vendor the new version and re-apply
the same migration (or drop this fork entirely if upstream moves off
`proc-macro-error2`).

[`mysql-common-derive`]: https://crates.io/crates/mysql-common-derive
[`proc-macro-error2`]: https://crates.io/crates/proc-macro-error2
[RUSTSEC-2026-0173]: https://rustsec.org/advisories/RUSTSEC-2026-0173

## License

Licensed under either of
 * Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license (http://opensource.org/licenses/MIT)
at your option (unchanged from upstream).
