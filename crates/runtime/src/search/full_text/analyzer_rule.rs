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

//! [`AnalyzerRule`] to resolve '`text_search`' UDTFs properly by resolving them as a join between the full text search index and their underlying table. See [`crate::search::full_text::udtf::TextSearchTableFunc`] for details on the underlying [`TableProvider`].
//!
//! ### Example
//! ```sql
//! SELECT * from text_search(notes, 'search embed')
//! ```
//! Becomes
//! ```sql
//! SELECT *
//! FROM text_search(notes, 'search embed') t
//! JOIN notes n ON t.primary_key = n.primary_key
//! ORDER BY score desc
//! ```

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

                let TextSearchTableFuncArgs {
                    tbl: base_table, ..
                } = &text_search_udtf.args;

                let underlying_table = &text_search_udtf.underlying;

                let base_table_scan = TableScan::try_new(
                    base_table.clone(),
                    Arc::new(DefaultTableSource::new(Arc::clone(underlying_table))),
                    projection.as_ref().map(|v| {
                        let base_schema = underlying_table.schema();
                        v.iter()
                            .filter(|idx| **idx <= base_schema.fields().len())
                            .copied()
                            .collect()
                    }),
                    filters.clone(),
                    None,
                )?;

                let index_scan = TableScan::try_new(
                    format!("{}_udtf", base_table.clone()),
                    Arc::new(DefaultTableSource::new(Arc::new(text_search_udtf.clone()))),
                    Some(
                        [
                            text_search_udtf.primary_key_projection(), // Primary key
                            vec![text_search_udtf.schema().fields().len() - 1], // 'score' column
                        ]
                        .concat(),
                    ),
                    vec![],
                    *fetch,
                )?;

                let Ok(df_schema) = DFSchema::try_from(text_search_udtf.schema()).map(Arc::new)
                else {
                    unreachable!("DFSchema::try_from is infallible as of DataFusion 38")
                };

                let join = Join {
                    left: Arc::new(LogicalPlan::TableScan(index_scan)),
                    right: Arc::new(LogicalPlan::TableScan(base_table_scan)),
                    join_type: JoinType::Left,
                    join_constraint: JoinConstraint::On,
                    on: text_search_udtf
                        .index
                        .primary_key
                        .iter()
                        .map(|p| {
                            (
                                Column::new_unqualified(p.clone()).into(),
                                Column::new_unqualified(p.clone()).into(),
                            )
                        })
                        .collect::<Vec<_>>(),
                    filter: None,
                    schema: Arc::clone(&df_schema),
                    null_equals_null: false,
                };

                let sort = Sort {
                    input: Arc::new(LogicalPlan::Join(join)),
                    expr: vec![SortExpr {
                        expr: Expr::Column(Column::new_unqualified(SEARCH_SCORE_COLUMN_NAME)),
                        nulls_first: false,
                        asc: false,
                    }],
                    fetch: *fetch,
                };

                Ok(Transformed::yes(LogicalPlan::SubqueryAlias(
                    SubqueryAlias::try_new(Arc::new(LogicalPlan::Sort(sort)), table_name.clone())?,
                )))
            }
            _ => Ok(Transformed::no(plan)),
        })?;

        Ok(transformed_plan.data)
    }

    fn name(&self) -> &'static str {
        "full_text_udtf_analyzer_rule"
    }
}
