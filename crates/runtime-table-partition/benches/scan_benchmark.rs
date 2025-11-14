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

#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]

use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use datafusion::{
    arrow::array::{Int32Array, StringArray},
    arrow::record_batch::RecordBatch,
    catalog::TableProvider,
    datasource::MemTable,
    error::DataFusionError,
    logical_expr::{ScalarUDF, TableProviderFilterPushDown, expr::ScalarFunction},
    prelude::{Expr, col, lit},
    scalar::ScalarValue,
};
use runtime_table_partition::{
    Partition, creator::PartitionCreator, expression::PartitionedBy,
    provider::PartitionTableProvider,
};
use std::sync::Arc;
use tokio::sync::RwLock;

type PartitionsData = Arc<RwLock<Vec<(ScalarValue, Arc<dyn TableProvider>)>>>;

#[derive(Debug)]
struct MockCreator {
    partitions_data: PartitionsData,
}

#[async_trait]
impl PartitionCreator for MockCreator {
    async fn create_partition(
        &self,
        _partition_value: ScalarValue,
    ) -> Result<Partition, runtime_table_partition::creator::Error> {
        unreachable!("create_partition not needed for benchmarks")
    }

    async fn infer_existing_partitions(
        &self,
    ) -> Result<Vec<Partition>, runtime_table_partition::creator::Error> {
        let data = self.partitions_data.read().await;
        Ok(data
            .iter()
            .map(|(val, provider)| Partition {
                partition_value: val.clone(),
                table_provider: Arc::clone(provider),
            })
            .collect())
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

fn create_test_batch(region: &str, size: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("region", DataType::Utf8, false),
    ]));

    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    let ids: Vec<i32> = (0..size as i32).collect();
    let id_array = Arc::new(Int32Array::from(ids));
    let region_array = Arc::new(StringArray::from(vec![region; size]));

    RecordBatch::try_new(schema, vec![id_array, region_array])
        .unwrap_or_else(|e| panic!("failed to create test batch: {e}"))
}

fn create_provider_with_partitions(
    num_partitions: usize,
    rows_per_partition: usize,
) -> PartitionTableProvider {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("region", DataType::Utf8, false),
    ]));

    let partitions_data: Vec<_> = (0..num_partitions)
        .map(|i| {
            let region = format!("region-{i}");
            let batch = create_test_batch(&region, rows_per_partition);
            (
                ScalarValue::Utf8(Some(region)),
                Arc::new(
                    MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])
                        .unwrap_or_else(|e| panic!("failed to create MemTable: {e}")),
                ) as Arc<dyn TableProvider>,
            )
        })
        .collect();

    let creator = Arc::new(MockCreator {
        partitions_data: Arc::new(RwLock::new(partitions_data)),
    });

    let partition_by = PartitionedBy {
        name: "region".to_string(),
        expression: col("region"),
    };

    let rt =
        tokio::runtime::Runtime::new().unwrap_or_else(|e| panic!("failed to create runtime: {e}"));
    rt.block_on(async {
        PartitionTableProvider::new(creator, vec![partition_by], schema)
            .await
            .unwrap_or_else(|e| panic!("failed to create provider: {e}"))
    })
}

fn create_provider_with_bucket_partitions(
    num_partitions: usize,
    rows_per_partition: usize,
) -> PartitionTableProvider {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("user_id", DataType::Int32, false),
    ]));

    let partitions_data: Vec<_> = (0..num_partitions)
        .map(|i| {
            let ids: Vec<i32> = (0..rows_per_partition as i32).collect();
            let user_ids: Vec<i32> = (0..rows_per_partition as i32)
                .map(|j| j * 10 + i as i32)
                .collect();

            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(ids)),
                    Arc::new(Int32Array::from(user_ids)),
                ],
            )
            .unwrap_or_else(|e| panic!("failed to create batch: {e}"));

            (
                ScalarValue::Int32(Some(i as i32)),
                Arc::new(
                    MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])
                        .unwrap_or_else(|e| panic!("failed to create MemTable: {e}")),
                ) as Arc<dyn TableProvider>,
            )
        })
        .collect();

    let creator = Arc::new(MockCreator {
        partitions_data: Arc::new(RwLock::new(partitions_data)),
    });

    let bucket_udf = Arc::new(ScalarUDF::new_from_impl(
        runtime_datafusion_udfs::bucket::Bucket::new(),
    ));
    let partition_expr = Expr::ScalarFunction(ScalarFunction {
        func: bucket_udf,
        args: vec![lit(num_partitions as i32), col("user_id")],
    });

    let partition_by = PartitionedBy {
        name: format!("bucket_{num_partitions}_user_id"),
        expression: partition_expr,
    };

    let rt =
        tokio::runtime::Runtime::new().unwrap_or_else(|e| panic!("failed to create runtime: {e}"));
    rt.block_on(async {
        PartitionTableProvider::new(creator, vec![partition_by], schema)
            .await
            .unwrap_or_else(|e| panic!("failed to create provider: {e}"))
    })
}

fn bench_scan_no_filters(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan_no_filters");
    let rt =
        tokio::runtime::Runtime::new().unwrap_or_else(|e| panic!("failed to create runtime: {e}"));

    for num_partitions in [10, 50, 100] {
        let provider = create_provider_with_partitions(num_partitions, 1000);
        let session_state = datafusion::execution::context::SessionContext::new().state();

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{num_partitions}_partitions")),
            &num_partitions,
            |b, _| {
                b.to_async(&rt).iter(|| async {
                    provider
                        .scan(&session_state, None, &[], None)
                        .await
                        .unwrap_or_else(|e| panic!("scan failed: {e}"))
                });
            },
        );
    }

    group.finish();
}

fn bench_scan_with_pruning(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan_with_pruning");
    let rt =
        tokio::runtime::Runtime::new().unwrap_or_else(|e| panic!("failed to create runtime: {e}"));

    for num_partitions in [10, 50, 100] {
        let provider = create_provider_with_partitions(num_partitions, 1000);
        let session_state = datafusion::execution::context::SessionContext::new().state();
        let filters = vec![col("region").eq(lit("region-0"))];

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{num_partitions}_partitions")),
            &num_partitions,
            |b, _| {
                b.to_async(&rt).iter(|| async {
                    black_box(
                        provider
                            .scan(&session_state, None, &filters, None)
                            .await
                            .unwrap_or_else(|e| panic!("scan failed: {e}")),
                    )
                });
            },
        );
    }

    group.finish();
}

fn bench_scan_with_complex_expression(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan_complex_expression");
    let rt =
        tokio::runtime::Runtime::new().unwrap_or_else(|e| panic!("failed to create runtime: {e}"));

    for num_partitions in [10, 50, 100] {
        let provider = create_provider_with_bucket_partitions(num_partitions, 1000);
        let session_state = datafusion::execution::context::SessionContext::new().state();

        let bucket_udf = Arc::new(ScalarUDF::new_from_impl(
            runtime_datafusion_udfs::bucket::Bucket::new(),
        ));
        let partition_expr = Expr::ScalarFunction(ScalarFunction {
            func: bucket_udf,
            args: vec![lit(num_partitions as i32), col("user_id")],
        });
        let filters = vec![partition_expr.eq(lit(0i32))];

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{num_partitions}_partitions")),
            &num_partitions,
            |b, _| {
                b.to_async(&rt).iter(|| async {
                    black_box(
                        provider
                            .scan(&session_state, None, &filters, None)
                            .await
                            .unwrap_or_else(|e| panic!("scan failed: {e}")),
                    )
                });
            },
        );
    }

    group.finish();
}

fn bench_scan_prune_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan_prune_all");
    let rt =
        tokio::runtime::Runtime::new().unwrap_or_else(|e| panic!("failed to create runtime: {e}"));

    for num_partitions in [10, 50, 100] {
        let provider = create_provider_with_partitions(num_partitions, 1000);
        let session_state = datafusion::execution::context::SessionContext::new().state();
        let filters = vec![col("region").eq(lit("nonexistent-region"))];

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{num_partitions}_partitions")),
            &num_partitions,
            |b, _| {
                b.to_async(&rt).iter(|| async {
                    black_box(
                        provider
                            .scan(&session_state, None, &filters, None)
                            .await
                            .unwrap_or_else(|e| panic!("scan failed: {e}")),
                    )
                });
            },
        );
    }

    group.finish();
}

fn bench_scan_with_projection(c: &mut Criterion) {
    let mut group = c.benchmark_group("scan_with_projection");
    let rt =
        tokio::runtime::Runtime::new().unwrap_or_else(|e| panic!("failed to create runtime: {e}"));

    for num_partitions in [10, 50, 100] {
        let provider = create_provider_with_partitions(num_partitions, 1000);
        let session_state = datafusion::execution::context::SessionContext::new().state();
        let projection = vec![0]; // Only project the id column

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{num_partitions}_partitions")),
            &num_partitions,
            |b, _| {
                b.to_async(&rt).iter(|| async {
                    black_box(
                        provider
                            .scan(&session_state, Some(&projection), &[], None)
                            .await
                            .unwrap_or_else(|e| panic!("scan failed: {e}")),
                    )
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_scan_no_filters,
    bench_scan_with_pruning,
    bench_scan_with_complex_expression,
    bench_scan_prune_all,
    bench_scan_with_projection
);
criterion_main!(benches);
