# sqlparser (spiceai vendored fork)

The SQL lexer and parser behind DataFusion.

## Why this is vendored

This is a local, minimally modified fork of [`sqlparser`] **v0.62.0**
(upstream: <https://github.com/apache/datafusion-sqlparser-rs>), pulled into the spiceai tree via
`[patch.crates-io]` in the workspace root `Cargo.toml`.

In 0.62.0 the right operand of `IS [NOT] DISTINCT FROM` is parsed with `parse_expr()`, the
lowest precedence, so it swallows every operator that follows it. `a IS DISTINCT FROM b OR c`
parses as `a IS DISTINCT FROM (b OR c)`. PostgreSQL parses it as `(a IS DISTINCT FROM b) OR c`.
In Spice this returned wrong rows with no error: `SELECT true IS DISTINCT FROM true OR true`
returned `false`, and a filter `(x > 1) IS DISTINCT FROM true OR y = 'a'` lost its `y = 'a'`
disjunct. With integer operands, the query failed to plan with
`Cannot infer common argument type for logical boolean operation Int64 OR Boolean`.

Upstream fixed it in [apache/datafusion-sqlparser-rs#2436], which shipped in 0.63.0. DataFusion
55 depends on 0.62, and the 0.63 AST changes need a DataFusion upgrade, so the fix is backported
here instead.

## What changed vs. upstream 0.62.0

The two source hunks of #2436, unchanged:

- `src/parser/mod.rs`, `Parser::parse_infix`: the right operand of `IS DISTINCT FROM` and
  `IS NOT DISTINCT FROM` is parsed with `parse_subexpr(precedence)` (the `IS` precedence) instead
  of `parse_expr()`. Operators that bind tighter than `IS` (`+`, `=`, `||`, `->`, ...) stay in the
  right operand. `AND` and `OR` end it.
- `src/dialect/mod.rs`, `Dialect::prec_value`: the default precedence of `Precedence::PgOther`
  (`->`, `->>`, `@>`, custom operators) moves from 16 (below `IS`) to 21 (above `BETWEEN`/`LIKE`,
  below `+`/`-`), as in PostgreSQL's grammar. Without it, `a IS DISTINCT FROM b -> 'k'` would stop
  the right operand short of `->` in the generic and MySQL dialects. `PostgreSqlDialect` has its
  own table and already ranked these operators above `IS`; Spice parses with that dialect.

The rest of the crate is byte-for-byte the published 0.62.0 (`src/`, `Cargo.toml` apart from a
header comment, `LICENSE.TXT`).

To re-sync with a newer upstream release, drop this directory and the `[patch.crates-io]` entry
once DataFusion depends on sqlparser 0.63.0 or later, which contains #2436.
`crates/runtime-datafusion/tests/is_distinct_from_precedence.rs` and the spicescript
`test/spicescript/sql-is-distinct-from-precedence.yaml` fail if the fix is lost.

[`sqlparser`]: https://crates.io/crates/sqlparser
[apache/datafusion-sqlparser-rs#2436]: https://github.com/apache/datafusion-sqlparser-rs/pull/2436
