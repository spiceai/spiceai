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

Three Spice changes that are not upstream:

- `src/ast/mod.rs`, `impl Display for Ident`: a quoted identifier writes every quote character in
  its name twice. Upstream (0.62.0 and 0.63.0) skips a quote character that follows a backslash or
  another quote character, so a name such as `a\"b` closes the identifier early, and the rest of
  the name is read as SQL by the engine that receives it. Federated queries send column aliases
  and column names to the source this way.

- `src/ast/value.rs`, `impl Display for EscapeQuotedString`: a string literal (and every other
  quoted value written through `escape_quoted_string`) writes every quote character in the value
  twice. Upstream (0.62.0 and 0.63.0) skips a quote character that follows a backslash or another
  quote character. An engine that reads a backslash as a plain character, such as PostgreSQL or
  SQLite, then ends a literal that holds `\'` early, and reads `a''b` back as `a'b`. Federated
  queries send client filter values to the source this way. An engine that reads a backslash as an
  escape needs its dialect to write the backslash as well.

- `src/ast/mod.rs`, `impl Display for Expr`, `Expr::UnaryOp`: a minus sign before an operand that
  starts with a minus sign, such as DataFusion's `-(-x)`, is written as `- -x`. Upstream (0.62.0
  and 0.63.0) writes `--x`, which most engines read as a comment to the end of the line, so the
  engine fails to parse the query or runs a different one. Federated queries send client filters
  to the source this way.

Two more Spice changes, not upstream, that keep deep trees from overflowing the stack:

- `src/parser/mod.rs`, `Parser::parse_subexpr`, `Parser::parse_query_body` and
  `Parser::parse_remaining_set_exprs`: the parser tracks the depth of each expression and
  set-operation tree it builds and returns `ParserError::RecursionLimitExceeded` once a tree would
  be deeper than `MAX_AST_DEPTH` (2048). Upstream (0.62.0 and 0.63.0) bounds only nested calls with
  `recursion_limit`; a flat chain of binary operators (`1+1+…`) or set operations
  (`SELECT 1 UNION SELECT 1 …`) is built in a loop with no nesting, so ~62 KiB of SQL makes a tree
  31,000 levels deep. Dropping such a tree recurses once per level, and it overflowed the 2 MiB
  worker stack and aborted the process even when Spice rejected the statement before planning it.
  2048 is four times Spice's own expression depth limit (`MAX_SQL_EXPR_DEPTH`, 512), so every
  statement that limit admits still parses and gets that limit's error instead.
- `src/ast/spans.rs`, `Spanned for SetExpr`: the span of a set-operation chain is computed by
  walking its left spine in a loop; upstream recurses once per operation. DataFusion takes that
  span at every level while planning the chain, inside a 256 KiB stack red zone, and a chain at
  the bound above needs 256–512 KiB to span recursively, so planning it overflowed the stack.
  `test_set_operation_chain_span` in that file fails if this is lost.

The rest of the crate is byte-for-byte the published 0.62.0 (`src/`, `Cargo.toml` apart from a
header comment, `LICENSE.TXT`).

To re-sync with a newer upstream release, drop this directory and the `[patch.crates-io]` entry
once DataFusion depends on a sqlparser release that contains #2436 (0.63.0 or later) and writes
every quote character of a quoted identifier and of a string literal twice and a space between
two minus signs, and that bounds the depth of the trees it builds. These tests fail if a fix is
lost:
`crates/runtime-datafusion/tests/is_distinct_from_precedence.rs`, the spicescript
`test/spicescript/sql-is-distinct-from-precedence.yaml`,
`crates/runtime-datafusion/tests/quoted_identifier_escaping.rs`,
`crates/runtime-datafusion/tests/string_literal_escaping.rs`,
`crates/runtime-datafusion/tests/unary_minus_spacing.rs`, the string values of
`test/spicescript/postgres-literals.yaml`, and the parser-bound tests in
`crates/runtime/src/datafusion/sql_depth_guard_tests.rs`.

[`sqlparser`]: https://crates.io/crates/sqlparser
[apache/datafusion-sqlparser-rs#2436]: https://github.com/apache/datafusion-sqlparser-rs/pull/2436
