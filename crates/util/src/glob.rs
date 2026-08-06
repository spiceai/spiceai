/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Helpers for reasoning about `schema.table` glob patterns *without* compiling
//! them, so a catalog connector can skip interrogating a schema that cannot
//! contain a selected table.
//!
//! A catalog's `include` patterns are matched per table against a compiled
//! `GlobSet` (over the candidate `"{schema}.{table}"`). Listing every table of
//! every schema to find out which ones match is expensive, so a connector wants
//! to rule out whole schemas from the patterns alone — before any per-table
//! metadata call.
//!
//! Such a pre-filter is only safe if it is a *necessary* condition: it may keep
//! a schema that turns out to hold nothing, but it must never drop one whose
//! tables the `GlobSet` would select. Dropping is silent data loss — the catalog
//! loads successfully and simply contains less than the user asked for.

/// The literal prefix of a glob pattern: the leading run of characters that
/// every string the pattern matches must start with.
///
/// The prefix ends at the first metacharacter (`*`, `?`, `[`, `{`) or at a
/// backslash, whose escaping meaning is platform-dependent. Stopping early is
/// always sound: a shorter prefix is a weaker claim about what can match.
///
/// A pattern that opens with a metacharacter has an empty literal prefix, which
/// constrains nothing — that is correct, not a failure to analyze it.
#[must_use]
pub fn glob_literal_prefix(pattern: &str) -> &str {
    let end = pattern
        .find(|c| matches!(c, '*' | '?' | '[' | '{' | '\\'))
        .unwrap_or(pattern.len());
    &pattern[..end]
}

/// Whether any table in `schema` could be selected by `include_patterns`.
///
/// This is the necessary condition described in the module docs, so it is safe
/// to use as a pre-filter: a `false` answer proves no `"{schema}.{table}"` can
/// match any pattern, for any table name.
///
/// An empty pattern list means "no include filter", so every schema is kept.
///
/// Pass the **raw patterns**, exactly as configured. The literal prefix of each
/// is derived here; handing this function a list of already-extracted prefixes
/// would compare the wrong strings and can silently drop a schema.
///
/// # How it decides
///
/// Every string a pattern matches begins with that pattern's literal prefix
/// `L`. The candidate is `"{schema}.{table}"` with `table` unknown, so writing
/// `D` for `"{schema}."`, such a candidate can exist only when `L` is a prefix
/// of `D` (any table completes it) or `D` is a prefix of `L` (the rest of `L`
/// constrains the table name, which some table can satisfy). Because
/// `D + table` must start with `L`, one of those two always holds whenever a
/// match is possible — comparing lengths shows which.
pub fn schema_may_contain_selected_table<S: AsRef<str>>(
    schema: &str,
    include_patterns: &[S],
) -> bool {
    if include_patterns.is_empty() {
        return true;
    }

    let schema_prefix = format!("{schema}.");

    include_patterns.iter().any(|pattern| {
        let literal = glob_literal_prefix(pattern.as_ref());
        schema_prefix.starts_with(literal) || literal.starts_with(&schema_prefix)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use globset::{Glob, GlobSet, GlobSetBuilder};

    #[test]
    fn literal_prefix_of_a_pattern_without_metacharacters_is_the_whole_pattern() {
        assert_eq!(glob_literal_prefix("public.orders"), "public.orders");
        assert_eq!(glob_literal_prefix("mydb"), "mydb");
    }

    #[test]
    fn literal_prefix_stops_at_each_metacharacter() {
        assert_eq!(glob_literal_prefix("sales_*.orders"), "sales_");
        assert_eq!(glob_literal_prefix("?ublic.orders"), "");
        assert_eq!(glob_literal_prefix("[ps]ublic.*"), "");
        assert_eq!(glob_literal_prefix("{public,sales}.*"), "");
        assert_eq!(glob_literal_prefix("public.ord?rs"), "public.ord");
        assert_eq!(glob_literal_prefix("public.[a-z]*"), "public.");
    }

    #[test]
    fn literal_prefix_stops_at_a_backslash() {
        // Backslash escaping is platform-dependent, so the prefix stops there.
        // A shorter prefix only ever keeps a schema, never drops one.
        assert_eq!(glob_literal_prefix(r"public.ord\*ers"), "public.ord");
    }

    #[test]
    fn literal_prefix_of_a_bare_wildcard_is_empty() {
        assert_eq!(glob_literal_prefix("*"), "");
        assert_eq!(glob_literal_prefix("**"), "");
        assert_eq!(glob_literal_prefix("*.*"), "");
    }

    #[test]
    fn no_include_patterns_keeps_every_schema() {
        let patterns: [&str; 0] = [];
        assert!(schema_may_contain_selected_table("mydb", &patterns));
    }

    #[test]
    fn a_schema_named_by_a_pattern_is_kept() {
        assert!(schema_may_contain_selected_table(
            "mydb",
            &["mydb.orders".to_string()]
        ));
        assert!(schema_may_contain_selected_table("mydb", &["mydb"]));
    }

    #[test]
    fn a_schema_no_pattern_can_reach_is_dropped() {
        assert!(!schema_may_contain_selected_table(
            "mydb",
            &["otherdb", "otherdb.orders", "sales_east.*"]
        ));
    }

    #[test]
    fn a_partial_wildcard_schema_pattern_is_kept() {
        // The shape that regressed: the wildcard is inside the schema segment.
        assert!(schema_may_contain_selected_table(
            "sales_east",
            &["sales_*.orders"]
        ));
        assert!(schema_may_contain_selected_table(
            "sales_east",
            &["sales_*.*"]
        ));
        // ...but a literal prefix that cannot lead to this schema still prunes.
        assert!(!schema_may_contain_selected_table(
            "north_east",
            &["sales_*.orders"]
        ));
    }

    #[test]
    fn a_pattern_opening_with_a_metacharacter_never_prunes() {
        for pattern in [
            "*",
            "*.*",
            "*.orders",
            "{public,sales}.*",
            "[ps]ublic.*",
            "?ublic.orders",
        ] {
            assert!(
                schema_may_contain_selected_table("public", &[pattern]),
                "pattern {pattern} must not prune any schema"
            );
        }
    }

    #[test]
    fn one_unprunable_pattern_in_a_set_keeps_the_schema() {
        // `any` semantics: a schema is kept if ANY pattern could reach it.
        assert!(schema_may_contain_selected_table(
            "mydb",
            &["otherdb.orders", "*.orders"]
        ));
    }

    fn globset_of(patterns: &[&str]) -> GlobSet {
        let mut builder = GlobSetBuilder::new();
        for pattern in patterns {
            builder.add(Glob::new(pattern).expect("test pattern is a valid glob"));
        }
        builder.build().expect("test patterns build into a GlobSet")
    }

    /// The invariant the pre-filter exists to satisfy, asserted directly against
    /// the same `globset` matcher the per-table filter uses: if the compiled set
    /// selects `"{schema}.{table}"`, the pre-filter must keep `schema`.
    ///
    /// The reverse is deliberately not asserted — keeping a schema that holds no
    /// selected table costs a metadata call, which is not a correctness failure.
    #[test]
    fn prefilter_never_drops_a_schema_whose_table_the_globset_selects() {
        let patterns = [
            "public.orders",
            "public.*",
            "*.orders",
            "*",
            "*.*",
            "sales_*.orders",
            "sales_*.*",
            "{public,sales}.*",
            "[ps]ublic.*",
            "?ublic.orders",
            "public.ord*",
            "sales_east.ord?rs",
            "otherdb.*",
            r"pub\lic.*",
        ];
        let schemas = [
            "public",
            "sales",
            "sales_east",
            "salesx",
            "otherdb",
            "p",
            "",
            "public.nested",
        ];
        let tables = ["orders", "line_item", "o", "", "orders.v2"];

        for pattern in patterns {
            let set = globset_of(&[pattern]);
            for schema in schemas {
                let kept = schema_may_contain_selected_table(schema, &[pattern]);
                for table in tables {
                    let candidate = format!("{schema}.{table}");
                    assert!(
                        !(set.is_match(&candidate) && !kept),
                        "pattern {pattern:?} selects {candidate:?} but the pre-filter dropped schema {schema:?}"
                    );
                }
            }
        }
    }

    /// The same invariant over multi-pattern sets, where the compiled `GlobSet`
    /// matches if *any* pattern does and the pre-filter must keep the schema if
    /// any pattern could reach it.
    #[test]
    fn prefilter_never_drops_a_schema_selected_by_any_pattern_in_a_set() {
        let sets: &[&[&str]] = &[
            &["otherdb.orders", "sales_*.orders"],
            &["public.orders", "*"],
            &["[ps]ublic.*", "otherdb.x"],
            &["{public,sales}.*", "north.*"],
            &["otherdb.*", "?ublic.orders"],
        ];
        let schemas = ["public", "sales", "sales_east", "otherdb", "north", "zzz"];
        let tables = ["orders", "x", "line_item"];

        for patterns in sets {
            let set = globset_of(patterns);
            for schema in schemas {
                let kept = schema_may_contain_selected_table(schema, patterns);
                for table in tables {
                    let candidate = format!("{schema}.{table}");
                    assert!(
                        !(set.is_match(&candidate) && !kept),
                        "{patterns:?} selects {candidate:?} but the pre-filter dropped schema {schema:?}"
                    );
                }
            }
        }
    }

    /// The pre-filter has to actually prune, or it would be trivially correct
    /// and pointless.
    #[test]
    fn prefilter_still_prunes_an_unreachable_schema() {
        assert!(!schema_may_contain_selected_table(
            "warehouse",
            &["public.*", "sales_*.orders", "otherdb"]
        ));
    }
}
