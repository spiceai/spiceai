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
//! Shared test fixtures for the [`PartitionedTableScanRewrite`](super::PartitionedTableScanRewrite)
//! submodules.

use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema, SchemaRef},
    catalog::Session,
    common::{Result, Statistics, stats::Precision},
    datasource::{DefaultTableSource, TableProvider, TableType, empty::EmptyTable},
    error::DataFusionError,
    logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, TableScan},
    physical_plan::{ExecutionPlan, empty::EmptyExec},
    prelude::SessionContext,
    sql::TableReference,
};

use super::{PartitionValue, PartitionedTableScanRewrite, TablePartitionProvider};

/// A test [`TableProvider`] that reports a fixed, exact `num_rows` statistic. Used to
/// exercise limit-based union-leg trimming. `scan` returns an empty plan (the trimming
/// rewrite operates purely on statistics, so no data is needed).
#[derive(Debug)]
pub(crate) struct StatsTable {
    schema: SchemaRef,
    num_rows: Precision<usize>,
}

#[async_trait]
impl TableProvider for StatsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let projected = datafusion::common::project_schema(&self.schema, projection)?;
        Ok(Arc::new(EmptyExec::new(projected)))
    }

    fn statistics(&self) -> Option<Statistics> {
        let mut stats = Statistics::new_unknown(&self.schema);
        stats.num_rows = self.num_rows;
        Some(stats)
    }
}

/// A test partition provider that splits any table into two partitions, each carrying a
/// partition-pruning filter (`partition_id = '0'` / `'1'`). The providers are `EmptyTable`s,
/// which report no statistics.
#[derive(Debug)]
pub(crate) struct TwoPartitionProvider {
    schema: SchemaRef,
}

impl TablePartitionProvider for TwoPartitionProvider {
    fn get_partitions(
        &self,
        _table: &TableReference,
        _schema: &SchemaRef,
    ) -> Vec<(Arc<dyn TableProvider>, Vec<PartitionValue>)> {
        let p1: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(Arc::clone(&self.schema)));
        let p2: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(Arc::clone(&self.schema)));
        vec![
            (
                p1,
                vec![HashMap::from([(
                    "partition_id".to_string(),
                    Some("0".to_string()),
                )])],
            ),
            (
                p2,
                vec![HashMap::from([(
                    "partition_id".to_string(),
                    Some("1".to_string()),
                )])],
            ),
        ]
    }

    fn should_partition(&self, _tbl: &TableScan) -> bool {
        true
    }
}

/// A test partition provider that produces one filter-free leg per supplied row count.
/// Each leg's provider reports the corresponding `num_rows` as `Precision::Exact`, except
/// where `None` is given (which produces a provider with absent statistics).
#[derive(Debug)]
pub(crate) struct StatsPartitionProvider {
    schema: SchemaRef,
    leg_rows: Vec<Option<usize>>,
}

impl TablePartitionProvider for StatsPartitionProvider {
    fn get_partitions(
        &self,
        _table: &TableReference,
        _schema: &SchemaRef,
    ) -> Vec<(Arc<dyn TableProvider>, Vec<PartitionValue>)> {
        self.leg_rows
            .iter()
            .map(|rows| {
                let num_rows = match rows {
                    Some(n) => Precision::Exact(*n),
                    None => Precision::Absent,
                };
                let provider: Arc<dyn TableProvider> = Arc::new(StatsTable {
                    schema: Arc::clone(&self.schema),
                    num_rows,
                });
                // Empty partition values => no partition filter is added to the leg.
                (provider, Vec::new())
            })
            .collect()
    }

    fn should_partition(&self, _tbl: &TableScan) -> bool {
        true
    }
}

pub(crate) fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("partition_id", DataType::Int32, false),
    ]))
}

/// Builds a rule backed by [`TwoPartitionProvider`] (two partition-filtered legs, no statistics).
pub(crate) fn make_rule(schema: &SchemaRef, ctx: &SessionContext) -> PartitionedTableScanRewrite {
    PartitionedTableScanRewrite::new(
        Arc::new(TwoPartitionProvider {
            schema: Arc::clone(schema),
        }),
        ctx,
    )
}

/// Builds a rule backed by [`StatsPartitionProvider`], producing one filter-free leg per
/// element of `leg_rows` with the given exact (or absent) row count.
pub(crate) fn make_stats_rule(
    schema: &SchemaRef,
    ctx: &SessionContext,
    leg_rows: Vec<Option<usize>>,
) -> PartitionedTableScanRewrite {
    PartitionedTableScanRewrite::new(
        Arc::new(StatsPartitionProvider {
            schema: Arc::clone(schema),
            leg_rows,
        }),
        ctx,
    )
}

pub(crate) fn make_table_scan(schema: &SchemaRef) -> LogicalPlan {
    let source: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(Arc::clone(schema)));
    LogicalPlanBuilder::scan("test_table", Arc::new(DefaultTableSource::new(source)), None)
        .expect("failed to build scan")
        .build()
        .expect("failed to build plan")
}
