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

use std::{fmt::Debug, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::{
        Result,
        tree_node::{Transformed, TransformedResult},
    },
    config::ConfigOptions,
    datasource::{DefaultTableSource, TableProvider},
    error::DataFusionError,
    logical_expr::{
        EmptyRelation, Expr, FetchType, Limit, LogicalPlan, LogicalPlanBuilder, SkipType, Sort,
        TableScan, Union,
    },
    optimizer::AnalyzerRule,
    sql::TableReference,
};

/// Define how to get partitions for a given table, and how they are partitioned.
pub trait TablePartitionProvider: Send + Sync + Debug {
    /// Get partitions for a given [`TableReference`].
    ///
    /// `schema`: The schema of the table locally. Expect all returned [`TableProvider`] to conform to this schema.
    /// Return pairs of [`TableProvider`] and the partition [`Expr`] that they represent/contain.
    fn get_partitions(
        &self,
        table: &TableReference,
        schema: &SchemaRef,
    ) -> Vec<(Arc<dyn TableProvider>, Vec<Expr>)>;

    /// Whether partitioning should be applied to the given table.
    fn should_partition(&self, tbl: &TableScan) -> bool;
}

/// An [`AnalyzerRule`] that rewrites table scans on a single locally registered table as the
/// `UNION ALL` of one or more partitions of this table (possibly from a different source).
///
/// Additionally, when a `Limit -> Sort -> Union` pattern is created by this rewrite, it pushes
/// `Sort(TopK)` into each union leg so that each partition returns at most `skip + fetch` rows.
///
/// For example, suppose we want to partition `sales`. We go from this:
///
/// ```text
/// Limit: skip=0, fetch=3
///  Sort: sales.order_number ASC
///   Projection: sales.order_number, sales.phone, sales.postal_code
///    TableScan: sales projection=[order_number, phone, postal_code], full_filters=[sales.status = Utf8("Disputed")]
/// ```
/// To:
/// ```text
/// Limit: skip=0, fetch=3
///  Sort: sales.order_number ASC
///   Projection: sales.order_number, sales.phone, sales.postal_code
///    Union
///     Sort: sales.order_number ASC, fetch=3
///      TableScan: sales@executor-1
///        full_filters=[sales.status = Utf8("Disputed"), hash(sales.partition_key) == 0x143A...]
///     Sort: sales.order_number ASC, fetch=3
///      TableScan: sales@executor-2
///        full_filters=[sales.status = Utf8("Disputed"), hash(sales.partition_key) == 0x896...]
/// ```
pub struct PartitionedTableScanRewrite {
    partition_provider: Arc<dyn TablePartitionProvider>,
}

impl PartitionedTableScanRewrite {
    pub fn new(partition_provider: Arc<dyn TablePartitionProvider>) -> Self {
        Self { partition_provider }
    }
}

impl Debug for PartitionedTableScanRewrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionedTableScanRewrite")
            .field("partition_provider", &self.partition_provider)
            .finish()
    }
}

impl AnalyzerRule for PartitionedTableScanRewrite {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> Result<LogicalPlan, DataFusionError> {
        plan.transform_up_with_subqueries(|plan| {
            if let LogicalPlan::TableScan(scan) = &plan {
                if !self.partition_provider.should_partition(scan) {
                    return Ok(Transformed::no(plan));
                }

                let providers = self
                    .partition_provider
                    .get_partitions(&scan.table_name, &scan.source.schema());

                tracing::debug!(
                    "PartitionedTableScanRewrite: {} partitions for '{}' table.",
                    providers.len(),
                    scan.table_name
                );

                let mut sub_scans = Vec::with_capacity(providers.len());
                for (provider, partition_filters) in providers {
                    let source = DefaultTableSource::new(Arc::clone(&provider));
                    let mut filters = scan.filters.clone();

                    // Combine partitions with OR.
                    if let Some(partition_filter) = partition_filters.into_iter().reduce(Expr::or) {
                        filters.push(partition_filter);
                    }
                    let plan = LogicalPlanBuilder::scan_with_filters(
                        scan.table_name.clone(),
                        Arc::new(source),
                        scan.projection.clone(),
                        filters,
                    )?
                    .build()?;
                    sub_scans.push(Arc::new(plan));
                }

                // If no partitions, return empty relation. This can happen if no partitions match the table (even if we want to partition it).
                if sub_scans.is_empty() {
                    return Ok(Transformed::yes(LogicalPlan::EmptyRelation(
                        EmptyRelation {
                            produce_one_row: false,
                            schema: Arc::clone(plan.schema()),
                        },
                    )));
                }

                return Ok(Transformed::yes(LogicalPlan::Union(Union {
                    inputs: sub_scans,
                    schema: Arc::clone(plan.schema()),
                })));
            }

            // Push Sort(TopK) into each Union leg when Limit sits above Sort -> Union.
            // DataFusion's optimizer pushes Limit through Union, but does not push
            // Sort(TopK) through Union. Without this, each executor returns all rows
            // and the scheduler sorts the full merged result.
            if matches!(&plan, LogicalPlan::Limit(_)) {
                let LogicalPlan::Limit(limit) = plan else {
                    unreachable!();
                };
                return push_sort_topk_into_union(limit);
            }

            Ok(Transformed::no(plan))
        })
        .data()
    }

    fn name(&self) -> &'static str {
        "PartitionedTableScanRewrite"
    }
}

/// When `Limit -> Sort -> Union(sub_scans)`, push `Sort(fetch = skip + fetch)` into each union
/// leg. This enables per-executor TopK, reducing data transfer from executors to the scheduler.
///
/// The outer `Limit -> Sort` is preserved for correct final merge-sort and limiting.
fn push_sort_topk_into_union(limit: Limit) -> Result<Transformed<LogicalPlan>, DataFusionError> {
    let LogicalPlan::Sort(sort) = limit.input.as_ref() else {
        return Ok(Transformed::no(LogicalPlan::Limit(limit)));
    };
    let LogicalPlan::Union(union_plan) = sort.input.as_ref() else {
        return Ok(Transformed::no(LogicalPlan::Limit(limit)));
    };

    let fetch = match limit.get_fetch_type()? {
        FetchType::Literal(Some(f)) => f,
        _ => return Ok(Transformed::no(LogicalPlan::Limit(limit))),
    };
    let skip = match limit.get_skip_type()? {
        SkipType::Literal(s) => s,
        SkipType::UnsupportedExpr => return Ok(Transformed::no(LogicalPlan::Limit(limit))),
    };
    let Some(effective_fetch) = skip.checked_add(fetch) else {
        return Ok(Transformed::no(LogicalPlan::Limit(limit)));
    };

    let new_inputs: Vec<Arc<LogicalPlan>> = union_plan
        .inputs
        .iter()
        .map(|input| {
            Arc::new(LogicalPlan::Sort(Sort {
                expr: sort.expr.clone(),
                input: Arc::clone(input),
                fetch: Some(effective_fetch),
            }))
        })
        .collect();

    let new_union = LogicalPlan::Union(Union {
        inputs: new_inputs,
        schema: Arc::clone(&union_plan.schema),
    });

    Ok(Transformed::yes(LogicalPlan::Limit(Limit {
        skip: limit.skip.clone(),
        fetch: limit.fetch.clone(),
        input: Arc::new(LogicalPlan::Sort(Sort {
            expr: sort.expr.clone(),
            input: Arc::new(new_union),
            fetch: sort.fetch,
        })),
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        datasource::empty::EmptyTable,
        logical_expr::{LogicalPlanBuilder, SortExpr, col, lit},
    };

    /// A test partition provider that splits any table into two partitions.
    #[derive(Debug)]
    struct TwoPartitionProvider {
        schema: SchemaRef,
    }

    impl TablePartitionProvider for TwoPartitionProvider {
        fn get_partitions(
            &self,
            _table: &TableReference,
            _schema: &SchemaRef,
        ) -> Vec<(Arc<dyn TableProvider>, Vec<Expr>)> {
            let p1: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(Arc::clone(&self.schema)));
            let p2: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(Arc::clone(&self.schema)));
            vec![
                (p1, vec![col("partition_id").eq(lit(0))]),
                (p2, vec![col("partition_id").eq(lit(1))]),
            ]
        }

        fn should_partition(&self, _tbl: &TableScan) -> bool {
            true
        }
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("partition_id", DataType::Int32, false),
        ]))
    }

    fn make_rule(schema: &SchemaRef) -> PartitionedTableScanRewrite {
        PartitionedTableScanRewrite::new(Arc::new(TwoPartitionProvider {
            schema: Arc::clone(schema),
        }))
    }

    fn make_table_scan(schema: &SchemaRef) -> LogicalPlan {
        let source: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(Arc::clone(schema)));
        LogicalPlanBuilder::scan(
            "test_table",
            Arc::new(DefaultTableSource::new(source)),
            None,
        )
        .expect("failed to build scan")
        .build()
        .expect("failed to build plan")
    }

    #[test]
    fn test_table_scan_rewritten_to_union() {
        let schema = test_schema();
        let rule = make_rule(&schema);
        let plan = make_table_scan(&schema);

        let result = rule
            .analyze(plan, &ConfigOptions::default())
            .expect("analyze failed");

        assert!(
            matches!(result, LogicalPlan::Union(_)),
            "Expected Union, got: {result}"
        );
        if let LogicalPlan::Union(union_plan) = &result {
            assert_eq!(union_plan.inputs.len(), 2, "Expected 2 partition sub-scans");
        }
    }

    #[test]
    fn test_limit_sort_pushdown_through_union() {
        let schema = test_schema();
        let rule = make_rule(&schema);

        // Build: Limit(fetch=5) -> Sort(id ASC) -> TableScan
        let scan = make_table_scan(&schema);
        let plan = LogicalPlanBuilder::from(scan)
            .sort(vec![SortExpr::new(col("id"), true, false)])
            .expect("sort failed")
            .limit(0, Some(5))
            .expect("limit failed")
            .build()
            .expect("build failed");

        let result = rule
            .analyze(plan, &ConfigOptions::default())
            .expect("analyze failed");

        // Expect: Limit -> Sort -> Union(Sort(fetch=5) -> scan, Sort(fetch=5) -> scan)
        let LogicalPlan::Limit(limit) = &result else {
            panic!("Expected Limit, got: {result}");
        };
        let LogicalPlan::Sort(outer_sort) = limit.input.as_ref() else {
            panic!("Expected Sort under Limit, got: {}", limit.input);
        };
        let LogicalPlan::Union(union_plan) = outer_sort.input.as_ref() else {
            panic!("Expected Union under Sort, got: {}", outer_sort.input);
        };
        assert_eq!(union_plan.inputs.len(), 2, "Expected 2 union legs");

        for (i, input) in union_plan.inputs.iter().enumerate() {
            let LogicalPlan::Sort(inner_sort) = input.as_ref() else {
                panic!("Union leg {i}: expected Sort, got: {input}");
            };
            assert_eq!(
                inner_sort.fetch,
                Some(5),
                "Union leg {i}: expected Sort(fetch=5)"
            );
        }
    }

    #[test]
    fn test_limit_sort_with_offset_pushdown() {
        let schema = test_schema();
        let rule = make_rule(&schema);

        // Build: Limit(skip=10, fetch=5) -> Sort(id ASC) -> TableScan
        let scan = make_table_scan(&schema);
        let plan = LogicalPlanBuilder::from(scan)
            .sort(vec![SortExpr::new(col("id"), true, false)])
            .expect("sort failed")
            .limit(10, Some(5))
            .expect("limit failed")
            .build()
            .expect("build failed");

        let result = rule
            .analyze(plan, &ConfigOptions::default())
            .expect("analyze failed");

        // Each inner Sort should have fetch = skip + fetch = 15
        let LogicalPlan::Limit(limit) = &result else {
            panic!("Expected Limit, got: {result}");
        };
        let LogicalPlan::Sort(outer_sort) = limit.input.as_ref() else {
            panic!("Expected Sort under Limit");
        };
        let LogicalPlan::Union(union_plan) = outer_sort.input.as_ref() else {
            panic!("Expected Union under Sort");
        };

        for (i, input) in union_plan.inputs.iter().enumerate() {
            let LogicalPlan::Sort(inner_sort) = input.as_ref() else {
                panic!("Union leg {i}: expected Sort");
            };
            assert_eq!(
                inner_sort.fetch,
                Some(15),
                "Union leg {i}: expected Sort(fetch=15) for skip=10 + fetch=5"
            );
        }
    }

    #[test]
    fn test_limit_without_sort_no_pushdown() {
        let schema = test_schema();
        let rule = make_rule(&schema);

        // Build: Limit(fetch=5) -> TableScan (no Sort)
        let scan = make_table_scan(&schema);
        let plan = LogicalPlanBuilder::from(scan)
            .limit(0, Some(5))
            .expect("limit failed")
            .build()
            .expect("build failed");

        let result = rule
            .analyze(plan, &ConfigOptions::default())
            .expect("analyze failed");

        // Limit -> Union (no inner Sort pushed — DataFusion's optimizer handles Limit through Union)
        let LogicalPlan::Limit(limit) = &result else {
            panic!("Expected Limit, got: {result}");
        };
        let LogicalPlan::Union(union_plan) = limit.input.as_ref() else {
            panic!("Expected Union under Limit, got: {}", limit.input);
        };
        // Verify no Sort was injected into union legs
        for (i, input) in union_plan.inputs.iter().enumerate() {
            assert!(
                !matches!(input.as_ref(), LogicalPlan::Sort(_)),
                "Union leg {i}: should NOT have Sort pushed (no Sort in original plan)"
            );
        }
    }
}
