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
        tree_node::{Transformed, TransformedResult, TreeNode},
    },
    config::ConfigOptions,
    datasource::{DefaultTableSource, TableProvider},
    error::DataFusionError,
    logical_expr::{EmptyRelation, Expr, LogicalPlan, LogicalPlanBuilder, TableScan, Union},
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
        schema: SchemaRef,
    ) -> Vec<(Arc<dyn TableProvider>, Vec<Expr>)>;

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
        plan.transform_up(|plan| {
            let LogicalPlan::TableScan(scan) = &plan else {
                return Ok(Transformed::no(plan));
            };
            if !self.partition_provider.should_partition(scan) {
                return Ok(Transformed::no(plan));
            }

            let providers = self
                .partition_provider
                .get_partitions(&scan.table_name, scan.source.schema());

            tracing::debug!(
                "PartitionedTableScanRewrite: {} partitions for '{}' table.",
                providers.len(),
                scan.table_name
            );

            let mut sub_scans = Vec::with_capacity(providers.len());
            for (provider, partition_filters) in providers {
                let source = DefaultTableSource::new(Arc::clone(&provider));
                let mut filters = scan.filters.clone();
                filters.extend_from_slice(&partition_filters);
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
            Ok(Transformed::yes(LogicalPlan::Union(Union {
                inputs: sub_scans,
                schema: Arc::clone(plan.schema()),
            })))
        })
        .data()
    }

    fn name(&self) -> &'static str {
        "PartitionedTableScanRewrite"
    }
}
