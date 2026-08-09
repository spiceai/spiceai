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

//! The `include`/`exclude` glob patterns a catalog is configured with, resolved
//! into the one decision every catalog connector needs: does this catalog
//! select a given table?

use std::sync::Arc;

use globset::GlobSet;

/// Decides which of a catalog's discovered tables it registers.
///
/// Both patterns are carried in a single value on purpose. A connector that
/// holds them as two independent fields has to thread both of them through
/// `CatalogProvider::new` -> `SchemaProvider::new` -> the per-table filter, and
/// dropping one of them is silent: the catalog loads, reports success, and
/// registers tables the user asked to keep out. Passing one value leaves
/// nothing to forget.
///
/// The selector matches a name the caller builds, rather than building one
/// itself, because connectors disagree on what a table is called: the SQL
/// catalogs match `"{schema}.{table}"` (see [`TableSelector::selects_table`])
/// while Iceberg matches a fully qualified `TableIdent`. Whatever name a
/// connector matches `include` against, it matches `exclude` against too.
#[derive(Debug, Clone, Default)]
pub struct TableSelector {
    include: Option<Arc<GlobSet>>,
    exclude: Option<Arc<GlobSet>>,
    /// Literal prefix of each `include` pattern, for [`TableSelector::may_select_within`].
    /// Empty when the patterns were not supplied, which disables that prune.
    include_literal_prefixes: Arc<Vec<String>>,
}

impl TableSelector {
    /// A selector for a catalog configured with `include` and/or `exclude`.
    /// `None` means the catalog did not configure that half.
    #[must_use]
    pub fn new(include: Option<GlobSet>, exclude: Option<GlobSet>) -> Self {
        Self {
            include: include.map(Arc::new),
            exclude: exclude.map(Arc::new),
            include_literal_prefixes: Arc::default(),
        }
    }

    /// Records the raw `include` patterns the compiled set was built from,
    /// enabling [`TableSelector::may_select_within`].
    ///
    /// A [`GlobSet`] cannot be introspected, so answering "can this container
    /// hold anything I want?" needs the source patterns. They must describe the
    /// same set as the compiled `include`, or a container will be skipped that
    /// shouldn't be.
    ///
    /// Optional because omitting it is safe: without the patterns the prune
    /// simply never fires, costing queries rather than correctness. That is the
    /// opposite of `exclude`, where dropping half the configuration silently
    /// registers tables the user excluded (#12636) -- which is why `exclude` is
    /// a constructor argument and this is not.
    #[must_use]
    pub fn with_include_patterns(mut self, patterns: &[String]) -> Self {
        self.include_literal_prefixes = Arc::new(
            patterns
                .iter()
                .map(|pattern| glob_literal_prefix(pattern))
                .collect(),
        );
        self
    }

    /// Whether any `include` pattern could match a name beginning with
    /// `"{container}."` -- a schema, database, or namespace worth interrogating.
    ///
    /// Lets a connector skip a container's metadata queries entirely instead of
    /// discovering its tables and then rejecting each one. Returns `true` when
    /// the patterns were never supplied, so the prune is opt-in.
    ///
    /// Conservative by construction: it must never answer `false` for a
    /// container that can contribute a table, because the resulting skip is
    /// silent -- the tables simply never appear. Answering `true` unnecessarily
    /// only costs queries.
    ///
    /// A pattern matches only strings beginning with its literal prefix `L`. The
    /// candidate is `{container}.{table}` with `table` unknown, so such a string
    /// can exist only when `L` is a prefix of `"{container}."` (any table
    /// completes it), or `"{container}."` is a prefix of `L` (the rest of `L`
    /// constrains the table name, which some table name can satisfy).
    ///
    /// This is why a pattern beginning with a metacharacter never prunes:
    /// `*.orders` has an empty literal prefix, and `*` matches `.` in
    /// `globset`, so it can match a table in any container.
    #[must_use]
    pub fn may_select_within(&self, container: &str) -> bool {
        // No patterns recorded: either none were configured (an absent `include`
        // selects everything) or the caller did not supply them. Both mean the
        // prune cannot rule anything out.
        if self.include_literal_prefixes.is_empty() {
            return true;
        }

        let container_prefix = format!("{container}.");
        self.include_literal_prefixes.iter().any(|literal| {
            container_prefix.starts_with(literal.as_str())
                || literal.starts_with(&container_prefix)
        })
    }

    /// A selector that selects every table -- an unconfigured catalog.
    #[must_use]
    pub fn select_all() -> Self {
        Self::default()
    }

    /// Whether the catalog selects the table named `qualified_name`.
    ///
    /// An absent `include` selects every name, so an unconfigured catalog
    /// registers everything it discovers. `exclude` is a veto: a name matched
    /// by *both* is not selected, which is the semantic `postgres` and
    /// `postgres_accelerated` have always implemented.
    #[must_use]
    pub fn selects(&self, qualified_name: &str) -> bool {
        self.rejection_reason(qualified_name).is_none()
    }

    /// Which half of the configuration withheld `qualified_name`, phrased for a
    /// diagnostic, or `None` when the catalog selects it.
    ///
    /// A connector logging a skipped table wants to say *why*, and only the
    /// selector knows which pattern set decided it.
    #[must_use]
    pub fn rejection_reason(&self, qualified_name: &str) -> Option<&'static str> {
        if self
            .include
            .as_ref()
            .is_some_and(|globset| !globset.is_match(qualified_name))
        {
            return Some("does not match include patterns");
        }

        if self
            .exclude
            .as_ref()
            .is_some_and(|globset| globset.is_match(qualified_name))
        {
            return Some("matches exclude patterns");
        }

        None
    }

    /// [`TableSelector::selects`] for the `"{schema}.{table}"` naming every SQL
    /// catalog connector matches against.
    #[must_use]
    pub fn selects_table(&self, schema_name: &str, table_name: &str) -> bool {
        self.selects(&format!("{schema_name}.{table_name}"))
    }
}

/// The literal prefix of a glob pattern: everything before the first
/// metacharacter. Every string the pattern matches must begin with this.
///
/// A backslash ends the prefix rather than being interpreted, because its
/// meaning is platform-dependent (an escape on Unix, a separator on Windows).
/// Stopping early is always safe: a shorter prefix is a weaker necessary
/// condition, so it can only cause a container to be kept, never dropped.
fn glob_literal_prefix(pattern: &str) -> String {
    let mut prefix = String::new();
    for c in pattern.chars() {
        match c {
            '*' | '?' | '[' | '{' | '\\' => break,
            other => prefix.push(other),
        }
    }
    prefix
}

#[cfg(test)]
mod tests {
    use super::{TableSelector, glob_literal_prefix};
    use globset::{Glob, GlobSet, GlobSetBuilder};

    /// A selector carrying the raw include patterns, as `table_selector()` builds it.
    fn sel(patterns: &[&str]) -> TableSelector {
        let owned: Vec<String> = patterns.iter().map(|p| (*p).to_string()).collect();
        let mut builder = GlobSetBuilder::new();
        for p in patterns {
            builder.add(Glob::new(p).expect("glob pattern should parse"));
        }
        TableSelector::new(Some(builder.build().expect("glob set should build")), None)
            .with_include_patterns(&owned)
    }

    #[test]
    fn glob_literal_prefix_stops_at_the_first_metacharacter() {
        assert_eq!(glob_literal_prefix("public.orders"), "public.orders");
        assert_eq!(glob_literal_prefix("public.*"), "public.");
        assert_eq!(glob_literal_prefix("sales_*.orders"), "sales_");
        assert_eq!(glob_literal_prefix("*.orders"), "");
        assert_eq!(glob_literal_prefix("*"), "");
        assert_eq!(glob_literal_prefix("{public,sales}.*"), "");
        assert_eq!(glob_literal_prefix("[ps]ublic.*"), "");
        assert_eq!(glob_literal_prefix("?ublic.orders"), "");
        assert_eq!(glob_literal_prefix(r"pub\lic.orders"), "pub");
    }

    #[test]
    fn may_select_within_keeps_only_containers_a_literal_pattern_can_reach() {
        assert!(sel(&["public.orders"]).may_select_within("public"));
        assert!(!sel(&["public.orders"]).may_select_within("sales"));
        assert!(sel(&["public.*"]).may_select_within("public"));
        assert!(!sel(&["public.*"]).may_select_within("sales"));
    }

    #[test]
    fn may_select_within_never_prunes_a_non_literal_container_component() {
        // `*` matches `.` in globset, so these can reach a table in any container.
        for pattern in ["*.orders", "*", "*.*", "{public,sales}.*", "?ublic.orders"] {
            for container in ["public", "sales", "anything_at_all"] {
                assert!(
                    sel(&[pattern]).may_select_within(container),
                    "pattern {pattern} must not prune container {container}"
                );
            }
        }
    }

    #[test]
    fn may_select_within_handles_partial_container_wildcards() {
        let s = sel(&["sales_*.orders"]);
        assert!(s.may_select_within("sales_east"));
        assert!(s.may_select_within("sales_"));
        assert!(!s.may_select_within("public"));
        assert!(!s.may_select_within("sales"));
    }

    #[test]
    fn may_select_within_keeps_a_container_any_one_pattern_can_reach() {
        let s = sel(&["public.orders", "sales.*"]);
        assert!(s.may_select_within("public"));
        assert!(s.may_select_within("sales"));
        assert!(!s.may_select_within("audit"));
        assert!(sel(&["public.orders", "*.audit_log"]).may_select_within("anything"));
    }

    #[test]
    fn may_select_within_is_disabled_without_recorded_patterns() {
        // An unconfigured catalog, and a selector built without the raw patterns:
        // both must prune nothing. `exclude` never prunes either -- proving an
        // exclude set covers *every* table in a container is a far harder claim.
        assert!(TableSelector::select_all().may_select_within("public"));
        assert!(TableSelector::new(Some(globset(&["public.*"])), None).may_select_within("sales"));
        assert!(
            TableSelector::new(None, Some(globset(&["private.*"])))
                .with_include_patterns(&[])
                .may_select_within("private")
        );
    }

    /// The property the prune must never violate: if the selector selects
    /// `container.table`, it must keep `container`. A violation is silent -- the
    /// table simply never appears -- so it is checked over pattern shapes rather
    /// than by example.
    #[test]
    fn may_select_within_never_contradicts_selects_table() {
        let pattern_sets: &[&[&str]] = &[
            &["public.orders"], &["public.*"], &["*.orders"], &["*"], &["*.*"],
            &["sales_*.orders"], &["sales_*.*"], &["{public,sales}.*"], &["[ps]ublic.*"],
            &["?ublic.orders"], &["public.order?"], &["public.orders", "sales.*"],
            &["public.*", "*.audit_log"], &["pg_*.*"],
        ];
        let containers = ["public", "sales", "sales_east", "sales_", "audit", "pg_toast", "s", ""];
        let tables = ["orders", "order1", "audit_log", "lineitem", "x", ""];

        for patterns in pattern_sets {
            let selector = sel(patterns);
            for container in containers {
                let kept = selector.may_select_within(container);
                for table in tables {
                    if selector.selects_table(container, table) {
                        assert!(
                            kept,
                            "patterns {patterns:?} select {container}.{table}, but the prune \
                             dropped {container} -- the table would silently disappear"
                        );
                    }
                }
            }
        }
    }

    fn globset(patterns: &[&str]) -> GlobSet {
        let mut builder = GlobSetBuilder::new();
        for pattern in patterns {
            builder.add(globset::Glob::new(pattern).expect("valid glob"));
        }
        builder.build().expect("valid globset")
    }

    #[test]
    fn selects_everything_when_unconfigured() {
        let selector = TableSelector::select_all();
        assert!(selector.selects_table("public", "orders"));
        assert!(selector.selects_table("private", "secrets"));
    }

    #[test]
    fn honors_include() {
        let selector = TableSelector::new(Some(globset(&["public.*"])), None);
        assert!(selector.selects_table("public", "orders"));
        assert!(!selector.selects_table("reporting", "orders"));
    }

    /// The regression this type exists for: an `exclude` with no `include` must
    /// withhold the table, rather than being ignored because the catalog only
    /// ever looked at `include`.
    #[test]
    fn honors_exclude_without_include() {
        let selector = TableSelector::new(None, Some(globset(&["public.audit_log"])));
        assert!(selector.selects_table("public", "orders"));
        assert!(!selector.selects_table("public", "audit_log"));
    }

    #[test]
    fn exclude_wins_over_include() {
        let selector = TableSelector::new(
            Some(globset(&["public.*"])),
            Some(globset(&["public.audit_log"])),
        );
        assert!(selector.selects_table("public", "orders"));
        assert!(!selector.selects_table("public", "audit_log"));
    }

    /// A table matched by neither is not selected: `include` still decides
    /// membership, and `exclude` only ever removes.
    #[test]
    fn exclude_does_not_widen_include() {
        let selector = TableSelector::new(
            Some(globset(&["public.*"])),
            Some(globset(&["private.audit_log"])),
        );
        assert!(!selector.selects_table("reporting", "orders"));
    }

    /// Connectors that already hold a qualified name -- Iceberg matches a
    /// fully-qualified `TableIdent`, not `"{schema}.{table}"` -- match both
    /// halves against that same name.
    #[test]
    fn selects_matches_a_caller_built_name() {
        let selector = TableSelector::new(
            Some(globset(&["warehouse.db.*"])),
            Some(globset(&["warehouse.db.audit_log"])),
        );
        assert!(selector.selects("warehouse.db.orders"));
        assert!(!selector.selects("warehouse.db.audit_log"));
        assert!(!selector.selects("other.db.orders"));
    }

    /// A glob matching a whole schema removes every table in it.
    #[test]
    fn exclude_accepts_a_wildcard() {
        let selector = TableSelector::new(None, Some(globset(&["private.*"])));
        assert!(selector.selects_table("public", "orders"));
        assert!(!selector.selects_table("private", "orders"));
        assert!(!selector.selects_table("private", "secrets"));
    }

    /// `compile_globset` yields `None` for an empty pattern list, so an
    /// explicitly-empty `exclude:` must not withhold anything.
    #[test]
    fn an_absent_exclude_withholds_nothing() {
        let selector = TableSelector::new(Some(globset(&["public.*"])), None);
        assert!(selector.selects_table("public", "audit_log"));
    }

    #[test]
    fn rejection_reason_names_the_half_that_decided() {
        let selector = TableSelector::new(
            Some(globset(&["public.*"])),
            Some(globset(&["public.audit_log"])),
        );
        assert_eq!(selector.rejection_reason("public.orders"), None);
        assert_eq!(
            selector.rejection_reason("reporting.orders"),
            Some("does not match include patterns")
        );
        assert_eq!(
            selector.rejection_reason("public.audit_log"),
            Some("matches exclude patterns")
        );
    }

    /// A name that both fails `include` and matches `exclude` reports the
    /// include miss -- the two are checked in the order they are configured,
    /// and either answer is a correct explanation of the same rejection.
    #[test]
    fn rejection_reason_prefers_the_include_miss() {
        let selector = TableSelector::new(
            Some(globset(&["public.*"])),
            Some(globset(&["private.audit_log"])),
        );
        assert_eq!(
            selector.rejection_reason("private.audit_log"),
            Some("does not match include patterns")
        );
    }
}
