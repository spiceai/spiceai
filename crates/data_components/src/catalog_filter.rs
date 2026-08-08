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
}

impl TableSelector {
    /// A selector for a catalog configured with `include` and/or `exclude`.
    /// `None` means the catalog did not configure that half.
    #[must_use]
    pub fn new(include: Option<GlobSet>, exclude: Option<GlobSet>) -> Self {
        Self {
            include: include.map(Arc::new),
            exclude: exclude.map(Arc::new),
        }
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

#[cfg(test)]
mod tests {
    use super::TableSelector;
    use globset::{GlobSet, GlobSetBuilder};

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
