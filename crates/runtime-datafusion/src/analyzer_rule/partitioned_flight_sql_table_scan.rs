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

use std::{collections::HashMap, fmt::Debug, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::{
        Result,
        tree_node::{Transformed, TransformedResult, TreeNode},
    },
    config::ConfigOptions,
    datasource::{DefaultTableSource, TableProvider},
    error::DataFusionError,
    logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, Union},
    optimizer::AnalyzerRule,
    sql::TableReference,
};
use itertools::concat;

pub trait FlightSQLPartitionProvider: Send + Sync + Debug {
    fn get_partitions(
        &self,
        table: &TableReference,
        schema: SchemaRef,
    ) -> Option<Vec<(Arc<dyn TableProvider>, Vec<Expr>)>>;
}

impl FlightSQLPartitionProvider
    for HashMap<TableReference, Vec<(Arc<dyn TableProvider>, Vec<Expr>)>>
{
    fn get_partitions(
        &self,
        table: &TableReference,
        _schema: SchemaRef,
    ) -> Option<Vec<(Arc<dyn TableProvider>, Vec<Expr>)>> {
        self.get(table).cloned()
    }
}

/// An [`AnalyzerRule`] that rewrites local table scans as the UNION ALL of one or more remote FlightSQL table scans.
/// For example, suppose we want to do it on `sales`. The we go from this
// ```
// |  Limit: skip=0, fetch=3                                                                                            |
// |   Projection: sales.order_number, sales.phone, sales.postal_code                                                   |
// |     TableScan: sales projection=[order_number, phone, postal_code], full_filters=[sales.status = Utf8("Disputed")] |
// ```
// To something like this:
// ```
// |  Union
// |   Limit: skip=0, fetch=3                                                                                            |
// |    Projection: sales.order_number, sales.phone, sales.postal_code                                                   |
// |      TableScan: sales
// |        projection=[order_number, phone, postal_code]
// |        full_filters=[
// |          sales.status = Utf8("Disputed")
// |          hash(sales.partition_key) == 0x143A6D32718BA52B18A7281
// |        ]                                                                                                            |
// |   Limit: skip=0, fetch=3                                                                                            |
// |    Projection: sales.order_number, sales.phone, sales.postal_code                                                   |
// |      TableScan: sales
// |        projection=[order_number, phone, postal_code]
// |        full_filters=[
// |          sales.status = Utf8("Disputed")
// |          hash(sales.partition_key) == 0x896981361692108D62195F
// |        ]                                                                                                            |
// ```
pub struct PartitionedFlightSQLTableScan {
    partition_provider: Arc<dyn FlightSQLPartitionProvider>,
}

impl PartitionedFlightSQLTableScan {
    pub fn new(partition_provider: Arc<dyn FlightSQLPartitionProvider>) -> Self {
        Self { partition_provider }
    }

    pub fn with_map(
        table_partitions: HashMap<TableReference, Vec<(Arc<dyn TableProvider>, Vec<Expr>)>>,
    ) -> Self {
        Self {
            partition_provider: Arc::new(table_partitions),
        }
    }
}

impl Debug for PartitionedFlightSQLTableScan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionedFlightSQLTableScan")
            .field("partition_provider", &self.partition_provider)
            .finish()
    }
}

impl AnalyzerRule for PartitionedFlightSQLTableScan {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> Result<LogicalPlan, DataFusionError> {
        plan.transform_up(|plan| {
            if let LogicalPlan::TableScan(scan) = &plan {
                if let Some(providers) = self
                    .partition_provider
                    .get_partitions(&scan.table_name, scan.source.schema())
                {
                    if providers.is_empty() {
                        return Ok(Transformed::no(plan));
                    }

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

                    return Ok(Transformed::yes(LogicalPlan::Union(Union {
                        inputs: sub_scans,
                        schema: Arc::clone(&plan.schema()),
                    })));
                }
            }
            Ok(Transformed::no(plan))
        })
        .data()
    }

    fn name(&self) -> &str {
        "partitioned_flight_sql_table_scan"
    }
}
