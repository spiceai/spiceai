# tantivy-query-grammar (spiceai vendored fork)

The query grammar behind Tantivy's `QueryParser`.

## Why this is vendored

This is a local, minimally modified fork of [`tantivy-query-grammar`] **v0.26.0**
(upstream: <https://github.com/quickwit-oss/tantivy>, directory `query-grammar`), pulled into
the spiceai tree via `[patch.crates-io]` in the workspace root `Cargo.toml`.

It does two things. First, it stops the strict grammar (`parse_query`, which
`QueryParser::parse_query` uses) from panicking on a `*` it reads as an exists query with no
field. Upstream, the `exists` rule accepts a `*` followed by whitespace or a character that must
be escaped in a word, and `literal` relies on its caller having already parsed a lone `*` as
match-all. Two shapes get past the caller: a `*` followed by a character the match-all rule does
not accept (`a *(b)`, `*"x"`, `*` then a non-breaking space), and a `*` after `+`/`-` and
whitespace (`- *`). `UserInputLeaf::set_field(None)` then panics with "Exist query without a field
isn't allowed": [quickwit-oss/tantivy#3031]. In Spice, a full-text search whose query had one of
these shapes failed with a 500, and a `panic = "abort"` build would exit.

Second, it bounds query nesting. Upstream recurses once per group (`(..)`, `field:(..)`) and per
`NOT` prefix with no limit, and re-parses each group when its first alternative fails, so a deeply
nested query can exhaust the stack or take exponential time.

## What changed vs. upstream 0.26.0

`literal` in `src/query_grammar.rs`: the `exists` alternative is tried only when a field name
was parsed before it. Without a field, `literal` tries the remaining alternatives (`range`, `set`,
`regex`, `term_or_phrase`) in the same order. On every input 0.26.0 parses without panicking, the
fork returns the same result, because there `exists` never matched without a field. An input that
panicked now either parses through a later alternative or returns a parse error, which Spice's
full-text search answers with its bag-of-words fallback.

`leaf` and `leaf_infallible` in `src/query_grammar.rs` enter a thread-local `NestingGuard`, which
allows at most `MAX_NESTING_DEPTH` nested levels. Past the limit, the strict grammar returns a
parse error and the lenient grammar reports "query nesting too deep".

The rest of the crate is byte-for-byte 0.26.0; `LICENSE` is Tantivy's MIT license.

To re-sync with a newer upstream release, re-vendor it and re-apply both changes.
`fieldless_exists_is_an_error_not_a_panic` and `query_nesting_past_the_limit_is_an_error` in
`src/query_grammar.rs`, and `test_fieldless_star_does_not_panic` in
`crates/search/src/generation/text_search/mod.rs`, fail if a change is lost.

[`tantivy-query-grammar`]: https://crates.io/crates/tantivy-query-grammar
[quickwit-oss/tantivy#3031]: https://github.com/quickwit-oss/tantivy/issues/3031
