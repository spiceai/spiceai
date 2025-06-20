/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::sync::Arc;

use datafusion::{
    catalog::TableProvider,
    common::{
        Column, DFSchema, JoinConstraint, JoinType,
        tree_node::{Transformed, TreeNode},
    },
    config::ConfigOptions,
    datasource::DefaultTableSource,
    error::DataFusionError,
    logical_expr::{Join, LogicalPlan, Sort, SortExpr, SubqueryAlias, TableScan},
    optimizer::AnalyzerRule,
    prelude::Expr,
};
use search::SEARCH_SCORE_COLUMN_NAME;

use crate::search::full_text::udtf::TEXT_SEARCH_UDTF_NAME;
use crate::search::full_text::udtf::{TextSearchTableFuncArgs, TextSearchUDTFProvider};

/// Rewrites [`super::udtf::TextSearchTableFunc`] calls to:
///   - Join on the underlying table
///   - Order by the [`search::SEARCH_SCORE_COLUMN_NAME`] column
///
/// ### Example
/// ```sql
/// SELECT * from text_search(notes, 'search embed')
/// ```
/// Gets rewritten to
/// ```sql
// SELECT *
// FROM text_search(notes, 'search embed') t
// JOIN notes n ON t.primary_key = n.primary_key
// ORDER BY score desc
/// ```
#[derive(Debug, Clone)]
pub struct FullTextUDTFAnalyzerRule {}

impl AnalyzerRule for FullTextUDTFAnalyzerRule {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> Result<LogicalPlan, DataFusionError> {
        let transformed_plan = plan.transform_down(|plan| match &plan {
            LogicalPlan::TableScan(TableScan {
                table_name,
                fetch,
                source,
                filters,
                projection,
                ..
            }) => {
                if table_name.to_string() != format!("{TEXT_SEARCH_UDTF_NAME}()") {
                    return Ok(Transformed::no(plan));
                }
                tracing::warn!("Found the udtf");
                let Some(default_source) = source.as_any().downcast_ref::<DefaultTableSource>()
                else {
                    return Ok(Transformed::no(plan));
                };
                let underlying = Arc::clone(&default_source.table_provider);
                let Some(text_search_udtf) =
                    underlying.as_any().downcast_ref::<TextSearchUDTFProvider>()
                else {
                    return Ok(Transformed::no(plan));
                };
                tracing::warn!("and its on a TextSearchTableProvider");

                let TextSearchTableFuncArgs {
                    tbl: base_table,
                    primary_key,
                    ..
                } = &text_search_udtf.args;
                let base_schema = text_search_udtf.underlying.schema();
                let base_table_scan = TableScan::try_new(
                    base_table.clone(),
                    Arc::new(DefaultTableSource::new(Arc::clone(
                        &text_search_udtf.underlying,
                    ))),
                    projection.as_ref().map(|v| {
                        v.iter()
                            .filter(|idx| **idx <= base_schema.fields().len())
                            .copied()
                            .collect()
                    }),
                    filters.clone(),
                    None,
                )?;
                tracing::warn!("base table good");

                let index_table_name = format!("{}_udtf", base_table.clone());
                let index_scan = TableScan::try_new(
                    index_table_name.clone(),
                    Arc::new(DefaultTableSource::new(Arc::new(text_search_udtf.clone()))),
                    // Hardcoded to only one primary key for now.
                    Some(vec![0, text_search_udtf.schema().fields().len() - 1]),
                    vec![],
                    *fetch,
                )?;

                let Ok(df_schema) = DFSchema::try_from(text_search_udtf.schema()).map(Arc::new)
                else {
                    unreachable!("DFSchema::try_from is infallible as of DataFusion 38")
                };

                tracing::warn!("index scan table good");
                let join = Join {
                    left: Arc::new(LogicalPlan::TableScan(index_scan)),
                    right: Arc::new(LogicalPlan::TableScan(base_table_scan)),
                    join_type: JoinType::Left,
                    join_constraint: JoinConstraint::On,
                    on: vec![(
                        Column::new_unqualified(primary_key.clone()).into(),
                        Column::new_unqualified(primary_key.clone()).into(),
                    )],
                    filter: None,
                    schema: Arc::clone(&df_schema),
                    null_equals_null: false,
                };

                let sort = Sort {
                    fetch: *fetch,
                    input: Arc::new(LogicalPlan::Join(join)),
                    expr: vec![SortExpr {
                        expr: Expr::Column(Column::new_unqualified(SEARCH_SCORE_COLUMN_NAME)),
                        nulls_first: false,
                        asc: false,
                    }],
                };

                tracing::warn!("giving back a 'Transformed::yes'");
                Ok(Transformed::yes(LogicalPlan::SubqueryAlias(
                    SubqueryAlias::try_new(Arc::new(LogicalPlan::Sort(sort)), table_name.clone())?,
                )))
            }
            _ => Ok(Transformed::no(plan)),
        })?;

        tracing::error!("ftsrule: {:?}", transformed_plan.data);
        Ok(transformed_plan.data)
    }

    fn name(&self) -> &'static str {
        "full_text_udtf_analyzer_rule"
    }
}
