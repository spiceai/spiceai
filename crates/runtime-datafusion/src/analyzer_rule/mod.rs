/*
Copyright 2025 The Spice.ai OSS Authors

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
pub mod duplicate_plan_node;
pub use duplicate_plan_node::*;

pub mod partitioned_table_scan_rewrite;
pub use partitioned_table_scan_rewrite::*;

pub mod regexp_match_null_check;
pub use regexp_match_null_check::*;

use std::sync::Arc;

use datafusion::optimizer::AnalyzerRule;
use datafusion::optimizer::analyzer::{
    resolve_grouping_function::ResolveGroupingFunction, type_coercion::TypeCoercion,
};
use datafusion_federation::sql::federation_analyzer_rule;

/// Builds the analyzer-rule list Spice runs, in Spice's order.
pub struct AnalyzerRulesBuilder {
    include_federation: bool,
    extra_rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
}

impl AnalyzerRulesBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn include_federation(mut self, include: bool) -> Self {
        self.include_federation = include;
        self
    }

    #[must_use]
    pub fn with_extra_rules(
        mut self,
        extra_rules: impl IntoIterator<Item = Arc<dyn AnalyzerRule + Send + Sync>>,
    ) -> Self {
        self.extra_rules.extend(extra_rules);
        self
    }

    /// Spice customizes the order of the analyzer rules, since some of them are only relevant when `DataFusion` is executing the query,
    /// as opposed to when underlying federated query engines will execute the query.
    ///
    /// This list should be kept in sync with the default rules in `Analyzer::new()`, but with the federation analyzer rule added first.
    #[must_use]
    pub fn build(self) -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
        // Before federation, which decides pushdown against the plan it is
        // handed: the NULL-check idiom federates only in its rewritten
        // `regexp_like` form (`regexp_match` itself is deny-listed for
        // BigQuery). See `RegexpMatchNullCheckRewrite`.
        let mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> =
            vec![Arc::new(RegexpMatchNullCheckRewrite::new())];
        if self.include_federation {
            rules.push(Arc::new(federation_analyzer_rule()));
        }
        // The rest of these rules are run after the federation analyzer since they only affect internal DataFusion execution.
        rules.extend([
            Arc::new(ResolveGroupingFunction::new()) as Arc<dyn AnalyzerRule + Send + Sync>,
            Arc::new(TypeCoercion::new()) as Arc<dyn AnalyzerRule + Send + Sync>,
        ]);
        rules.into_iter().chain(self.extra_rules).collect()
    }
}

impl Default for AnalyzerRulesBuilder {
    fn default() -> Self {
        Self {
            include_federation: true,
            extra_rules: vec![],
        }
    }
}
