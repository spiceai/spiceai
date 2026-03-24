/*
Copyright 2026 The Spice.ai OSS Authors

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
use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, Weak},
};

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::{
        Result, ToDFSchema,
        tree_node::{Transformed, TransformedResult},
    },
    config::ConfigOptions,
    datasource::{DefaultTableSource, TableProvider},
    error::DataFusionError,
    execution::SessionState,
    logical_expr::{
        EmptyRelation, Expr, Limit, LogicalPlan, LogicalPlanBuilder, Sort, SubqueryAlias,
        TableScan, Union, lit,
    },
    optimizer::AnalyzerRule,
    prelude::SessionContext,
    sql::TableReference,
};
use parking_lot::RwLock;

/// A specific value for partitioning keys.
/// For example, if a table is partitioned by:
///  - "date"
///  - "region"
///
/// Unique `PartitionValue`s might be (i.e. `Vec<PartitionValue>`):
/// ```json
/// {"date": "2024-01-01", "region": "us-east"}
/// {"date": "2024-01-01", "region": "us-west"}
/// {"date": "2024-01-02", "region": "us-east"}
/// ```
pub type PartitionValue = HashMap<String, String>;

/// Define how to get partitions for a given table, and how they are partitioned.
pub trait TablePartitionProvider: Send + Sync + Debug {
    /// Get partitions for a given [`TableReference`].
    ///
    /// `schema`: The schema of the table locally. Expect all returned [`TableProvider`] to conform to this schema.
    /// Returns pairs of [`TableProvider`] and the partition values (as string key-value maps) that
    /// they are responsible for. The analyzer rule converts these into filter [`Expr`]s.
    fn get_partitions(
        &self,
        table: &TableReference,
        schema: &SchemaRef,
    ) -> Vec<(Arc<dyn TableProvider>, Vec<PartitionValue>)>;

    /// Whether partitioning should be applied to the given table.
    fn should_partition(&self, tbl: &TableScan) -> bool;
}

/// An [`AnalyzerRule`] that rewrites table scans on a single locally registered table as the
/// `UNION ALL` of one or more partitions of this table (possibly from a different source).
///
/// For example, suppose we want to do it on `sales`. Then we go from this
///
/// ```
/// Limit: skip=0, fetch=3
///  Projection: sales.order_number, sales.phone, sales.postal_code
///    TableScan: sales projection=[order_number, phone, postal_code], full_filters=[sales.status = Utf8("Disputed")]
/// ```
/// To something like this:
/// ```
/// Union
///  Limit: skip=0, fetch=3
///   Projection: sales.order_number, sales.phone, sales.postal_code
///     TableScan: sales
///       projection=[order_number, phone, postal_code]
///       full_filters=[ sales.status = Utf8("Disputed"), hash(sales.partition_key) == 0x143A6D32718BA52B18A7281 ]
///  Limit: skip=0, fetch=3
///   Projection: sales.order_number, sales.phone, sales.postal_code
///     TableScan: sales
///       projection=[order_number, phone, postal_code]
///       full_filters=[ sales.status = Utf8("Disputed"), hash(sales.partition_key) == 0x896981361692108D62195F ]
/// ```
pub struct PartitionedTableScanRewrite {
    partition_provider: Arc<dyn TablePartitionProvider>,
    // Avoid holding a strong reference to SessionState to prevent circular references (SessionState -> AnalyzerRule -> SessionState). We only need it to parse partition expressions.
    session_state: Weak<RwLock<SessionState>>,
}

impl PartitionedTableScanRewrite {
    pub fn new(
        partition_provider: Arc<dyn TablePartitionProvider>,
        session_ctx: &SessionContext,
    ) -> Self {
        Self {
            partition_provider,
            session_state: session_ctx.state_weak_ref(),
        }
    }
}

impl Debug for PartitionedTableScanRewrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionedTableScanRewrite")
            .field("partition_provider", &self.partition_provider)
            .finish_non_exhaustive()
    }
}

impl PartitionedTableScanRewrite {
    /// Build partitioned sub-scans for a [`TableScan`] that should be partitioned,
    /// returning the individual sub-scan plans (one per partition).
    fn build_sub_scans(&self, scan: &TableScan) -> Result<Vec<Arc<LogicalPlan>>, DataFusionError> {
        let schema = scan.source.schema();
        let providers = self
            .partition_provider
            .get_partitions(&scan.table_name, &schema);

        tracing::debug!(
            "PartitionedTableScanRewrite: {} partitions for '{}' table.",
            providers.len(),
            scan.table_name
        );

        let df_schema = schema.to_dfschema()?;

        let mut sub_scans = Vec::with_capacity(providers.len());
        for (provider, partition_values) in providers {
            let mut filters = scan.filters.clone();

            let partition_exprs: Vec<Expr> = partition_values
                .iter()
                .filter_map(|pv| {
                    partition_value_to_expr(pv, &df_schema, &self.session_state).transpose()
                })
                .collect::<Result<Vec<_>, _>>()?;

            if let Some(partition_filter) = partition_exprs.into_iter().reduce(Expr::or) {
                filters.push(partition_filter);
            }
            let plan = LogicalPlanBuilder::scan_with_filters(
                scan.table_name.clone(),
                Arc::new(DefaultTableSource::new(Arc::clone(&provider))),
                scan.projection.clone(),
                filters,
            )?
            .build()?;
            sub_scans.push(Arc::new(plan));
        }
        Ok(sub_scans)
    }

    /// Build a `Union` (or single plan) from sub-scans, wrapped in a [`SubqueryAlias`].
    /// Returns `None` if there are no partitions (caller should produce an [`EmptyRelation`]).
    fn build_union_from_sub_scans(
        sub_scans: Vec<Arc<LogicalPlan>>,
        table_name: &TableReference,
    ) -> Result<Option<LogicalPlan>, DataFusionError> {
        let Some(first_scan) = sub_scans.first() else {
            return Ok(None);
        };
        let first_scan = Arc::unwrap_or_clone(Arc::clone(first_scan));
        let rest = sub_scans.into_iter().skip(1).collect::<Vec<_>>();

        if rest.is_empty() {
            return Ok(Some(first_scan));
        }

        let mut builder = LogicalPlanBuilder::from(first_scan);
        for scan in rest {
            builder = builder.union(Arc::unwrap_or_clone(scan))?;
        }
        let result = builder.alias(table_name.clone())?.build()?;
        Ok(Some(result))
    }
}

impl AnalyzerRule for PartitionedTableScanRewrite {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> Result<LogicalPlan, DataFusionError> {
        plan.transform_up_with_subqueries(|plan| {
            match &plan {
                // Rewrite partitioned TableScan into Union of sub-scans.
                LogicalPlan::TableScan(scan) => {
                    if !self.partition_provider.should_partition(scan) {
                        return Ok(Transformed::no(plan));
                    }

                    let sub_scans = self.build_sub_scans(scan)?;
                    match Self::build_union_from_sub_scans(sub_scans, &scan.table_name)? {
                        Some(result) => Ok(Transformed::yes(result)),
                        None => Ok(Transformed::yes(LogicalPlan::EmptyRelation(
                            EmptyRelation {
                                produce_one_row: false,
                                schema: Arc::clone(plan.schema()),
                            },
                        ))),
                    }
                }

                // Push Sort into each Union leg for better distributed performance.
                // After the TableScan → Union rewrite above (bottom-up), Sort's input
                // is now SubqueryAlias(Union(...)). We push Sort into each leg so each
                // executor sorts locally, reducing the data sent to the coordinator.
                LogicalPlan::Sort(sort) => {
                    if let LogicalPlan::SubqueryAlias(alias) = sort.input.as_ref() {
                        if let LogicalPlan::Union(union) = alias.input.as_ref() {
                            let new_inputs: Vec<Arc<LogicalPlan>> = union
                                .inputs
                                .iter()
                                .map(|leg| {
                                    Arc::new(LogicalPlan::Sort(Sort {
                                        expr: sort.expr.clone(),
                                        input: Arc::clone(leg),
                                        fetch: sort.fetch,
                                    }))
                                })
                                .collect();
                            let new_union =
                                LogicalPlan::Union(Union::try_new(new_inputs)?);
                            let new_alias = LogicalPlan::SubqueryAlias(
                                SubqueryAlias::try_new(
                                    Arc::new(new_union),
                                    alias.alias.clone(),
                                )?,
                            );
                            // Keep the outer Sort for correctness (merges sorted streams).
                            let result = LogicalPlan::Sort(Sort {
                                expr: sort.expr.clone(),
                                input: Arc::new(new_alias),
                                fetch: sort.fetch,
                            });
                            return Ok(Transformed::yes(result));
                        }
                    }
                    Ok(Transformed::no(plan))
                }

                // Push Limit into each Union leg so executors return at most N rows each.
                // Handles two patterns:
                //   Limit → Sort → SubqueryAlias → Union  (ORDER BY ... LIMIT)
                //   Limit → SubqueryAlias → Union          (LIMIT without ORDER BY)
                LogicalPlan::Limit(limit) => {
                    // Only push down when we have a fetch value.
                    if limit.fetch.is_none() {
                        return Ok(Transformed::no(plan));
                    }

                    // Pattern: Limit → Sort → SubqueryAlias → Union
                    // At this point Sort was already pushed down, so each Union leg
                    // has a Sort. We wrap each leg with Limit.
                    if let LogicalPlan::Sort(sort) = limit.input.as_ref() {
                        if let LogicalPlan::SubqueryAlias(alias) = sort.input.as_ref() {
                            if let LogicalPlan::Union(union) = alias.input.as_ref() {
                                let pushed_fetch = pushed_down_fetch(
                                    limit.skip.as_deref(),
                                    limit.fetch.as_deref(),
                                );
                                let new_inputs: Vec<Arc<LogicalPlan>> = union
                                    .inputs
                                    .iter()
                                    .map(|leg| {
                                        Arc::new(LogicalPlan::Limit(Limit {
                                            skip: None,
                                            fetch: pushed_fetch.clone(),
                                            input: Arc::clone(leg),
                                        }))
                                    })
                                    .collect();
                                let new_union =
                                    LogicalPlan::Union(Union::try_new(new_inputs)?);
                                let new_alias = LogicalPlan::SubqueryAlias(
                                    SubqueryAlias::try_new(
                                        Arc::new(new_union),
                                        alias.alias.clone(),
                                    )?,
                                );
                                // Keep outer Sort + Limit for correctness.
                                let new_sort = LogicalPlan::Sort(Sort {
                                    expr: sort.expr.clone(),
                                    input: Arc::new(new_alias),
                                    fetch: sort.fetch,
                                });
                                let result = LogicalPlan::Limit(Limit {
                                    skip: limit.skip.clone(),
                                    fetch: limit.fetch.clone(),
                                    input: Arc::new(new_sort),
                                });
                                return Ok(Transformed::yes(result));
                            }
                        }
                    }

                    // Pattern: Limit → SubqueryAlias → Union (no Sort)
                    if let LogicalPlan::SubqueryAlias(alias) = limit.input.as_ref() {
                        if let LogicalPlan::Union(union) = alias.input.as_ref() {
                            let pushed_fetch =
                                pushed_down_fetch(limit.skip.as_deref(), limit.fetch.as_deref());
                            let new_inputs: Vec<Arc<LogicalPlan>> = union
                                .inputs
                                .iter()
                                .map(|leg| {
                                    Arc::new(LogicalPlan::Limit(Limit {
                                        skip: None,
                                        fetch: pushed_fetch.clone(),
                                        input: Arc::clone(leg),
                                    }))
                                })
                                .collect();
                            let new_union =
                                LogicalPlan::Union(Union::try_new(new_inputs)?);
                            let new_alias = LogicalPlan::SubqueryAlias(
                                SubqueryAlias::try_new(
                                    Arc::new(new_union),
                                    alias.alias.clone(),
                                )?,
                            );
                            let result = LogicalPlan::Limit(Limit {
                                skip: limit.skip.clone(),
                                fetch: limit.fetch.clone(),
                                input: Arc::new(new_alias),
                            });
                            return Ok(Transformed::yes(result));
                        }
                    }

                    Ok(Transformed::no(plan))
                }

                _ => Ok(Transformed::no(plan)),
            }
        })
        .data()
    }

    fn name(&self) -> &'static str {
        "PartitionedTableScanRewrite"
    }
}

/// Compute the fetch value to push into each Union leg.
///
/// When there is a `LIMIT n OFFSET m`, each partition must return at least `n + m` rows
/// so the outer Limit can correctly skip `m` and take `n` from the merged result.
/// When there is no OFFSET, push fetch as-is.
///
/// Returns `None` if the pushed-down fetch cannot be computed (e.g. non-literal expressions),
/// in which case the caller should still push the Limit but with `fetch = None` (unlimited).
fn pushed_down_fetch(skip: Option<&Expr>, fetch: Option<&Expr>) -> Option<Box<Expr>> {
    use datafusion::common::ScalarValue;

    let fetch_expr = fetch?;

    match skip {
        None => Some(Box::new(fetch_expr.clone())),
        Some(skip_expr) => {
            // Try to evaluate skip + fetch for literal values.
            if let (
                Expr::Literal(ScalarValue::Int64(Some(s)), _),
                Expr::Literal(ScalarValue::Int64(Some(f)), _),
            ) = (skip_expr, fetch_expr)
            {
                Some(Box::new(lit(s + f)))
            } else {
                // Conservative: push fetch without accounting for skip.
                // The outer Limit still applies the correct skip.
                Some(Box::new(fetch_expr.clone()))
            }
        }
    }
}

/// Converts a [`PartitionValue`] (e.g. `{"bucket(3, org_id)": "42"}`) into a filter [`Expr`]
/// (e.g. `bucket(3, org_id) = '42'`). Multiple keys are `AND`ed together.
fn partition_value_to_expr(
    pv: &PartitionValue,
    df_schema: &datafusion::common::DFSchema,
    state: &Weak<RwLock<SessionState>>,
) -> Result<Option<Expr>, DataFusionError> {
    let mut expr: Option<Expr> = None;
    for (partition_expr_str, val) in pv {
        let new_expr = state
            .upgrade()
            .ok_or_else(|| {
                DataFusionError::Plan(
                    "SessionState has been dropped, cannot parse partition expression".to_string(),
                )
            })?
            .read()
            .create_logical_expr(partition_expr_str, df_schema)?
            .eq(lit(val.clone()));
        expr = match expr {
            Some(existing) => Some(existing.and(new_expr)),
            None => Some(new_expr),
        };
    }
    Ok(expr)
}
