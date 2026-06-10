//! [`CayenneInListToRangeRewrite`]: a logical [`OptimizerRule`]
//! ([`ApplyOrder::BottomUp`]) that matches `Filter` nodes whose input is a
//! single Cayenne table scan and rewrites `col IN (...)` predicates whose
//! list holds at least 4 distinct integer literals forming a gap-free range
//! into `col BETWEEN min AND max`. Non-rewritable predicates (negated, short,
//! sparse, duplicated, or non-integer lists) are left untouched; see
//! `crate::provider::table::rewrite_consecutive_inlist_to_range`.

use super::{
    ApplyOrder, Arc, CayenneTableProvider, DataFusionError, DefaultTableSource, Filter,
    LogicalPlan, OptimizerConfig, OptimizerRule, Result, TableProvider, TableProviderPredicate,
    TableSource, TableSourcePredicate, Transformed, TreeNode, is_single_cayenne_table_scan_input,
};

/// Logical optimizer rule that rewrites `column IN (k, k+1, …, k+N-1)` to
/// `column BETWEEN k AND k+N-1` for single-table Cayenne-backed filter inputs
/// when the list holds at least 4 distinct integer literals forming a
/// gap-free range (in any order — the rewrite sorts them itself).
/// BETWEEN is ~50 % faster than IN-list at per-row predicate evaluation.
/// Running this as a logical-plan rule (rather than in `TableProvider::scan`)
/// lets `DataFusion`'s downstream simplification passes treat the result
/// identically to a SQL-parsed `BETWEEN`. See bench
/// `pk_in_list_vs_range_rewrite`.
pub struct CayenneInListToRangeRewrite {
    is_cayenne_table_source: TableSourcePredicate,
}

impl Default for CayenneInListToRangeRewrite {
    fn default() -> Self {
        Self::new()
    }
}

impl CayenneInListToRangeRewrite {
    /// Create a new instance of the rule.
    #[must_use]
    pub fn new() -> Self {
        Self::new_with_table_provider_predicate(|provider| {
            provider.as_any().is::<CayenneTableProvider>()
        })
    }

    /// Create a new instance with a caller-provided table-provider predicate.
    #[must_use]
    pub fn new_with_table_provider_predicate(
        is_cayenne_table_provider: impl Fn(&dyn TableProvider) -> bool + Send + Sync + 'static,
    ) -> Self {
        let is_cayenne_table_provider: TableProviderPredicate = Arc::new(is_cayenne_table_provider);
        Self::new_with_table_source_predicate(move |source| {
            source
                .as_any()
                .downcast_ref::<DefaultTableSource>()
                .is_some_and(|source| is_cayenne_table_provider(source.table_provider.as_ref()))
        })
    }

    /// Create a new instance with a caller-provided table-source predicate.
    #[must_use]
    pub fn new_with_table_source_predicate(
        is_cayenne_table_source: impl Fn(&dyn TableSource) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            is_cayenne_table_source: Arc::new(is_cayenne_table_source),
        }
    }
}

impl std::fmt::Debug for CayenneInListToRangeRewrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneInListToRangeRewrite").finish()
    }
}

impl OptimizerRule for CayenneInListToRangeRewrite {
    fn name(&self) -> &'static str {
        "cayenne_inlist_to_range_rewrite"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        let LogicalPlan::Filter(filter) = plan else {
            return Ok(Transformed::no(plan));
        };
        if !is_single_cayenne_table_scan_input(&filter.input, &self.is_cayenne_table_source) {
            return Ok(Transformed::no(LogicalPlan::Filter(filter)));
        }

        let original = filter.predicate.clone();
        let rewritten = original
            .clone()
            .transform_up(|expr| {
                let after =
                    crate::provider::table::rewrite_consecutive_inlist_to_range(expr.clone());
                if after == expr {
                    Ok(Transformed::no(expr))
                } else {
                    Ok(Transformed::yes(after))
                }
            })?
            .data;
        if rewritten == original {
            return Ok(Transformed::no(LogicalPlan::Filter(filter)));
        }
        let new_filter = Filter::try_new(rewritten, filter.input)?;
        Ok(Transformed::yes(LogicalPlan::Filter(new_filter)))
    }
}
