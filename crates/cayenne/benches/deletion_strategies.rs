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

//! Benchmarks for deletion vector strategies in Cayenne.
//!
//! This benchmarks the three deletion strategies:
//! 1. `Int64Pk` - Single Int64 primary key using `HashSet<i64>`
//! 2. `RowConverterBased` - Composite/non-integer PKs using `RowConverter`
//! 3. `PositionBased` - No PK, uses `RoaringBitmap`
//!
//! Each strategy is tested for:
//! - Single row deletion
//! - Batch deletion (multiple rows)
//! - Query after deletion (scan with deletion filter)
//! - Insert after deletion

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::{
    metadata::CreateTableOptions, CayenneCatalog, CayenneTableProvider, MetadataCatalog,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use data_components::delete::DeletionTableProvider;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use std::hint::black_box;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Runtime;

const ROWS_SMALL: usize = 100;
const ROWS_MEDIUM: usize = 1000;
const ROWS_LARGE: usize = 10000;

// =============================================================================
// Setup Helpers
// =============================================================================

struct BenchFixture {
    catalog: Arc<CayenneCatalog>,
    data_dir: TempDir,
    _meta_dir: TempDir,
}

async fn setup_sqlite_fixture_async() -> BenchFixture {
    let meta_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let data_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = meta_dir.path().join("meta.db");
    let connection_string = format!("sqlite://{}", db_path.display());

    let catalog = CayenneCatalog::new(&connection_string).expect("create catalog");
    catalog.init().await.expect("init catalog");

    BenchFixture {
        catalog: Arc::new(catalog),
        data_dir,
        _meta_dir: meta_dir,
    }
}

#[cfg(feature = "turso")]
async fn setup_turso_fixture_async() -> BenchFixture {
    let meta_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let data_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = meta_dir.path().join("meta.db");
    let connection_string = format!("libsql://{}", db_path.display());

    let catalog = CayenneCatalog::new(&connection_string).expect("create catalog");
    catalog.init().await.expect("init catalog");

    BenchFixture {
        catalog: Arc::new(catalog),
        data_dir,
        _meta_dir: meta_dir,
    }
}

fn create_int64_pk_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn create_string_pk_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("code", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn create_no_pk_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn generate_int64_pk_batch(schema: Arc<Schema>, size: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..size as i64).collect();
    let names: Vec<String> = (0..size).map(|i| format!("name_{i}")).collect();
    let values: Vec<i64> = (0..size as i64).map(|i| i * 100).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("batch")
}

fn generate_string_pk_batch(schema: Arc<Schema>, size: usize) -> RecordBatch {
    let codes: Vec<String> = (0..size).map(|i| format!("CODE_{i:06}")).collect();
    let names: Vec<String> = (0..size).map(|i| format!("name_{i}")).collect();
    let values: Vec<i64> = (0..size as i64).map(|i| i * 100).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(codes)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("batch")
}

fn generate_no_pk_batch(schema: Arc<Schema>, size: usize) -> RecordBatch {
    let names: Vec<String> = (0..size).map(|i| format!("name_{i}")).collect();
    let values: Vec<i64> = (0..size as i64).map(|i| i * 100).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("batch")
}

async fn insert_batch(table: &Arc<CayenneTableProvider>, batch: RecordBatch) {
    let ctx = SessionContext::new();
    let schema = batch.schema();
    let input_exec = datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
        &[vec![batch]],
        schema,
        None,
    )
    .expect("memory exec");
    let insert_plan = table
        .insert_into(
            &ctx.state(),
            input_exec,
            datafusion_expr::dml::InsertOp::Append,
        )
        .await
        .expect("insert_into");
    datafusion_physical_plan::collect(insert_plan, ctx.task_ctx())
        .await
        .expect("collect");
}

async fn delete_records(table: &Arc<CayenneTableProvider>, filter: Expr) -> u64 {
    let ctx = SessionContext::new();
    let plan = table
        .delete_from(&ctx.state(), &[filter])
        .await
        .expect("delete");
    let results = datafusion_physical_plan::collect(plan, ctx.task_ctx())
        .await
        .expect("collect");
    results
        .first()
        .and_then(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
        })
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0)
}

async fn query_count(table: &Arc<CayenneTableProvider>) -> i64 {
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register");
    let df = ctx.sql("SELECT COUNT(*) FROM t").await.expect("sql");
    let results = df.collect().await.expect("collect");
    results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0)
}

// =============================================================================
// Int64Pk Strategy Benchmarks
// =============================================================================

fn bench_int64pk_single_delete(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("int64pk_single_delete");

    for size in [ROWS_SMALL, ROWS_MEDIUM, ROWS_LARGE] {
        group.bench_with_input(BenchmarkId::new("sqlite", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let fixture = setup_sqlite_fixture_async().await;
                    let schema = create_int64_pk_schema();
                    let table = Arc::new(
                        CayenneTableProvider::create_table(
                            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                            CreateTableOptions {
                                table_name: "bench_table".to_string(),
                                schema: Arc::clone(&schema),
                                primary_key: vec!["id".to_string()],
                                base_path: fixture.data_dir.path().to_string_lossy().to_string(),
                                partition_column: None,
                                vortex_config: cayenne::metadata::VortexConfig::default(),
                                on_conflict: None,
                            },
                        )
                        .await
                        .expect("create table"),
                    );

                    let batch = generate_int64_pk_batch(Arc::clone(&schema), size);
                    insert_batch(&table, batch).await;

                    // Delete middle row
                    let deleted = delete_records(&table, col("id").eq(lit(size as i64 / 2))).await;
                    black_box(deleted);
                });
            });
        });

        #[cfg(feature = "turso")]
        group.bench_with_input(BenchmarkId::new("turso", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let fixture = setup_turso_fixture_async().await;
                    let schema = create_int64_pk_schema();
                    let table = Arc::new(
                        CayenneTableProvider::create_table(
                            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                            CreateTableOptions {
                                table_name: "bench_table".to_string(),
                                schema: Arc::clone(&schema),
                                primary_key: vec!["id".to_string()],
                                base_path: fixture.data_dir.path().to_string_lossy().to_string(),
                                partition_column: None,
                                vortex_config: cayenne::metadata::VortexConfig::default(),
                                on_conflict: None,
                            },
                        )
                        .await
                        .expect("create table"),
                    );

                    let batch = generate_int64_pk_batch(Arc::clone(&schema), size);
                    insert_batch(&table, batch).await;

                    let deleted = delete_records(&table, col("id").eq(lit(size as i64 / 2))).await;
                    black_box(deleted);
                });
            });
        });
    }

    group.finish();
}

fn bench_int64pk_batch_delete(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("int64pk_batch_delete");

    for size in [ROWS_SMALL, ROWS_MEDIUM, ROWS_LARGE] {
        group.bench_with_input(BenchmarkId::new("sqlite", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let fixture = setup_sqlite_fixture_async().await;
                    let schema = create_int64_pk_schema();
                    let table = Arc::new(
                        CayenneTableProvider::create_table(
                            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                            CreateTableOptions {
                                table_name: "bench_table".to_string(),
                                schema: Arc::clone(&schema),
                                primary_key: vec!["id".to_string()],
                                base_path: fixture.data_dir.path().to_string_lossy().to_string(),
                                partition_column: None,
                                vortex_config: cayenne::metadata::VortexConfig::default(),
                                on_conflict: None,
                            },
                        )
                        .await
                        .expect("create table"),
                    );

                    let batch = generate_int64_pk_batch(Arc::clone(&schema), size);
                    insert_batch(&table, batch).await;

                    // Delete 10% of rows (even IDs in first 20%)
                    let deleted = delete_records(
                        &table,
                        col("id")
                            .lt(lit(size as i64 / 5))
                            .and((col("id") % lit(2i64)).eq(lit(0i64))),
                    )
                    .await;
                    black_box(deleted);
                });
            });
        });
    }

    group.finish();
}

fn bench_int64pk_query_after_delete(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("int64pk_query_after_delete");

    for size in [ROWS_SMALL, ROWS_MEDIUM, ROWS_LARGE] {
        group.bench_with_input(BenchmarkId::new("sqlite", size), &size, |b, &size| {
            // Setup: create and delete, then benchmark query
            let (table, _fixture) = rt.block_on(async {
                let fixture = setup_sqlite_fixture_async().await;
                let schema = create_int64_pk_schema();
                let table = Arc::new(
                    CayenneTableProvider::create_table(
                        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                        CreateTableOptions {
                            table_name: "bench_table".to_string(),
                            schema: Arc::clone(&schema),
                            primary_key: vec!["id".to_string()],
                            base_path: fixture.data_dir.path().to_string_lossy().to_string(),
                            partition_column: None,
                            vortex_config: cayenne::metadata::VortexConfig::default(),
                            on_conflict: None,
                        },
                    )
                    .await
                    .expect("create table"),
                );

                let batch = generate_int64_pk_batch(Arc::clone(&schema), size);
                insert_batch(&table, batch).await;

                // Delete half
                delete_records(&table, col("id").lt(lit(size as i64 / 2))).await;

                (table, fixture)
            });

            b.iter(|| {
                rt.block_on(async {
                    let count = query_count(&table).await;
                    black_box(count);
                });
            });
        });
    }

    group.finish();
}

// =============================================================================
// RowConverter (String PK) Strategy Benchmarks
// =============================================================================

fn bench_stringpk_single_delete(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("stringpk_single_delete");

    for size in [ROWS_SMALL, ROWS_MEDIUM, ROWS_LARGE] {
        group.bench_with_input(BenchmarkId::new("sqlite", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let fixture = setup_sqlite_fixture_async().await;
                    let schema = create_string_pk_schema();
                    let table = Arc::new(
                        CayenneTableProvider::create_table(
                            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                            CreateTableOptions {
                                table_name: "bench_table".to_string(),
                                schema: Arc::clone(&schema),
                                primary_key: vec!["code".to_string()],
                                base_path: fixture.data_dir.path().to_string_lossy().to_string(),
                                partition_column: None,
                                vortex_config: cayenne::metadata::VortexConfig::default(),
                                on_conflict: None,
                            },
                        )
                        .await
                        .expect("create table"),
                    );

                    let batch = generate_string_pk_batch(Arc::clone(&schema), size);
                    insert_batch(&table, batch).await;

                    let target_code = format!("CODE_{:06}", size / 2);
                    let deleted = delete_records(&table, col("code").eq(lit(target_code))).await;
                    black_box(deleted);
                });
            });
        });

        #[cfg(feature = "turso")]
        group.bench_with_input(BenchmarkId::new("turso", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let fixture = setup_turso_fixture_async().await;
                    let schema = create_string_pk_schema();
                    let table = Arc::new(
                        CayenneTableProvider::create_table(
                            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                            CreateTableOptions {
                                table_name: "bench_table".to_string(),
                                schema: Arc::clone(&schema),
                                primary_key: vec!["code".to_string()],
                                base_path: fixture.data_dir.path().to_string_lossy().to_string(),
                                partition_column: None,
                                vortex_config: cayenne::metadata::VortexConfig::default(),
                                on_conflict: None,
                            },
                        )
                        .await
                        .expect("create table"),
                    );

                    let batch = generate_string_pk_batch(Arc::clone(&schema), size);
                    insert_batch(&table, batch).await;

                    let target_code = format!("CODE_{:06}", size / 2);
                    let deleted = delete_records(&table, col("code").eq(lit(target_code))).await;
                    black_box(deleted);
                });
            });
        });
    }

    group.finish();
}

fn bench_stringpk_batch_delete(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("stringpk_batch_delete");

    for size in [ROWS_SMALL, ROWS_MEDIUM] {
        // Skip LARGE for string matching
        group.bench_with_input(BenchmarkId::new("sqlite", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let fixture = setup_sqlite_fixture_async().await;
                    let schema = create_string_pk_schema();
                    let table = Arc::new(
                        CayenneTableProvider::create_table(
                            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                            CreateTableOptions {
                                table_name: "bench_table".to_string(),
                                schema: Arc::clone(&schema),
                                primary_key: vec!["code".to_string()],
                                base_path: fixture.data_dir.path().to_string_lossy().to_string(),
                                partition_column: None,
                                vortex_config: cayenne::metadata::VortexConfig::default(),
                                on_conflict: None,
                            },
                        )
                        .await
                        .expect("create table"),
                    );

                    let batch = generate_string_pk_batch(Arc::clone(&schema), size);
                    insert_batch(&table, batch).await;

                    // Delete rows with value < 20% of max
                    let deleted =
                        delete_records(&table, col("value").lt(lit((size as i64 / 5) * 100))).await;
                    black_box(deleted);
                });
            });
        });
    }

    group.finish();
}

fn bench_stringpk_query_after_delete(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("stringpk_query_after_delete");

    for size in [ROWS_SMALL, ROWS_MEDIUM, ROWS_LARGE] {
        group.bench_with_input(BenchmarkId::new("sqlite", size), &size, |b, &size| {
            let (table, _fixture) = rt.block_on(async {
                let fixture = setup_sqlite_fixture_async().await;
                let schema = create_string_pk_schema();
                let table = Arc::new(
                    CayenneTableProvider::create_table(
                        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                        CreateTableOptions {
                            table_name: "bench_table".to_string(),
                            schema: Arc::clone(&schema),
                            primary_key: vec!["code".to_string()],
                            base_path: fixture.data_dir.path().to_string_lossy().to_string(),
                            partition_column: None,
                            vortex_config: cayenne::metadata::VortexConfig::default(),
                            on_conflict: None,
                        },
                    )
                    .await
                    .expect("create table"),
                );

                let batch = generate_string_pk_batch(Arc::clone(&schema), size);
                insert_batch(&table, batch).await;

                // Delete half
                delete_records(&table, col("value").lt(lit((size as i64 / 2) * 100))).await;

                (table, fixture)
            });

            b.iter(|| {
                rt.block_on(async {
                    let count = query_count(&table).await;
                    black_box(count);
                });
            });
        });
    }

    group.finish();
}

// =============================================================================
// PositionBased Strategy Benchmarks
// =============================================================================

fn bench_positionbased_single_delete(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("positionbased_single_delete");

    for size in [ROWS_SMALL, ROWS_MEDIUM, ROWS_LARGE] {
        group.bench_with_input(BenchmarkId::new("sqlite", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let fixture = setup_sqlite_fixture_async().await;
                    let schema = create_no_pk_schema();
                    let table = Arc::new(
                        CayenneTableProvider::create_table(
                            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                            CreateTableOptions {
                                table_name: "bench_table".to_string(),
                                schema: Arc::clone(&schema),
                                primary_key: vec![], // No PK -> PositionBased
                                base_path: fixture.data_dir.path().to_string_lossy().to_string(),
                                partition_column: None,
                                vortex_config: cayenne::metadata::VortexConfig::default(),
                                on_conflict: None,
                            },
                        )
                        .await
                        .expect("create table"),
                    );

                    let batch = generate_no_pk_batch(Arc::clone(&schema), size);
                    insert_batch(&table, batch).await;

                    // Delete middle row by value
                    let deleted =
                        delete_records(&table, col("value").eq(lit((size as i64 / 2) * 100))).await;
                    black_box(deleted);
                });
            });
        });

        #[cfg(feature = "turso")]
        group.bench_with_input(BenchmarkId::new("turso", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let fixture = setup_turso_fixture_async().await;
                    let schema = create_no_pk_schema();
                    let table = Arc::new(
                        CayenneTableProvider::create_table(
                            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                            CreateTableOptions {
                                table_name: "bench_table".to_string(),
                                schema: Arc::clone(&schema),
                                primary_key: vec![],
                                base_path: fixture.data_dir.path().to_string_lossy().to_string(),
                                partition_column: None,
                                vortex_config: cayenne::metadata::VortexConfig::default(),
                                on_conflict: None,
                            },
                        )
                        .await
                        .expect("create table"),
                    );

                    let batch = generate_no_pk_batch(Arc::clone(&schema), size);
                    insert_batch(&table, batch).await;

                    let deleted =
                        delete_records(&table, col("value").eq(lit((size as i64 / 2) * 100))).await;
                    black_box(deleted);
                });
            });
        });
    }

    group.finish();
}

fn bench_positionbased_batch_delete(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("positionbased_batch_delete");

    for size in [ROWS_SMALL, ROWS_MEDIUM, ROWS_LARGE] {
        group.bench_with_input(BenchmarkId::new("sqlite", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let fixture = setup_sqlite_fixture_async().await;
                    let schema = create_no_pk_schema();
                    let table = Arc::new(
                        CayenneTableProvider::create_table(
                            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                            CreateTableOptions {
                                table_name: "bench_table".to_string(),
                                schema: Arc::clone(&schema),
                                primary_key: vec![],
                                base_path: fixture.data_dir.path().to_string_lossy().to_string(),
                                partition_column: None,
                                vortex_config: cayenne::metadata::VortexConfig::default(),
                                on_conflict: None,
                            },
                        )
                        .await
                        .expect("create table"),
                    );

                    let batch = generate_no_pk_batch(Arc::clone(&schema), size);
                    insert_batch(&table, batch).await;

                    // Delete 10% of rows
                    let deleted =
                        delete_records(&table, col("value").lt(lit((size as i64 / 10) * 100)))
                            .await;
                    black_box(deleted);
                });
            });
        });
    }

    group.finish();
}

fn bench_positionbased_query_after_delete(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("positionbased_query_after_delete");

    for size in [ROWS_SMALL, ROWS_MEDIUM, ROWS_LARGE] {
        group.bench_with_input(BenchmarkId::new("sqlite", size), &size, |b, &size| {
            let (table, _fixture) = rt.block_on(async {
                let fixture = setup_sqlite_fixture_async().await;
                let schema = create_no_pk_schema();
                let table = Arc::new(
                    CayenneTableProvider::create_table(
                        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                        CreateTableOptions {
                            table_name: "bench_table".to_string(),
                            schema: Arc::clone(&schema),
                            primary_key: vec![],
                            base_path: fixture.data_dir.path().to_string_lossy().to_string(),
                            partition_column: None,
                            vortex_config: cayenne::metadata::VortexConfig::default(),
                            on_conflict: None,
                        },
                    )
                    .await
                    .expect("create table"),
                );

                let batch = generate_no_pk_batch(Arc::clone(&schema), size);
                insert_batch(&table, batch).await;

                // Delete half
                delete_records(&table, col("value").lt(lit((size as i64 / 2) * 100))).await;

                (table, fixture)
            });

            b.iter(|| {
                rt.block_on(async {
                    let count = query_count(&table).await;
                    black_box(count);
                });
            });
        });
    }

    group.finish();
}

// =============================================================================
// Strategy Comparison Benchmarks
// =============================================================================

/// Compare deletion performance across all three strategies at the same data size.
fn bench_strategy_comparison(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("strategy_comparison");

    let size = ROWS_MEDIUM;

    // Int64Pk
    group.bench_function("int64pk", |b| {
        b.iter(|| {
            rt.block_on(async {
                let fixture = setup_sqlite_fixture_async().await;
                let schema = create_int64_pk_schema();
                let table = Arc::new(
                    CayenneTableProvider::create_table(
                        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                        CreateTableOptions {
                            table_name: "bench_table".to_string(),
                            schema: Arc::clone(&schema),
                            primary_key: vec!["id".to_string()],
                            base_path: fixture.data_dir.path().to_string_lossy().to_string(),
                            partition_column: None,
                            vortex_config: cayenne::metadata::VortexConfig::default(),
                            on_conflict: None,
                        },
                    )
                    .await
                    .expect("create table"),
                );

                let batch = generate_int64_pk_batch(Arc::clone(&schema), size);
                insert_batch(&table, batch).await;

                // Delete 10% of rows
                let deleted = delete_records(&table, col("id").lt(lit(size as i64 / 10))).await;
                black_box(deleted);
            });
        });
    });

    // RowConverter (String PK)
    group.bench_function("stringpk_rowconverter", |b| {
        b.iter(|| {
            rt.block_on(async {
                let fixture = setup_sqlite_fixture_async().await;
                let schema = create_string_pk_schema();
                let table = Arc::new(
                    CayenneTableProvider::create_table(
                        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                        CreateTableOptions {
                            table_name: "bench_table".to_string(),
                            schema: Arc::clone(&schema),
                            primary_key: vec!["code".to_string()],
                            base_path: fixture.data_dir.path().to_string_lossy().to_string(),
                            partition_column: None,
                            vortex_config: cayenne::metadata::VortexConfig::default(),
                            on_conflict: None,
                        },
                    )
                    .await
                    .expect("create table"),
                );

                let batch = generate_string_pk_batch(Arc::clone(&schema), size);
                insert_batch(&table, batch).await;

                // Delete 10% of rows
                let deleted =
                    delete_records(&table, col("value").lt(lit((size as i64 / 10) * 100))).await;
                black_box(deleted);
            });
        });
    });

    // PositionBased (no PK)
    group.bench_function("positionbased", |b| {
        b.iter(|| {
            rt.block_on(async {
                let fixture = setup_sqlite_fixture_async().await;
                let schema = create_no_pk_schema();
                let table = Arc::new(
                    CayenneTableProvider::create_table(
                        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                        CreateTableOptions {
                            table_name: "bench_table".to_string(),
                            schema: Arc::clone(&schema),
                            primary_key: vec![],
                            base_path: fixture.data_dir.path().to_string_lossy().to_string(),
                            partition_column: None,
                            vortex_config: cayenne::metadata::VortexConfig::default(),
                            on_conflict: None,
                        },
                    )
                    .await
                    .expect("create table"),
                );

                let batch = generate_no_pk_batch(Arc::clone(&schema), size);
                insert_batch(&table, batch).await;

                // Delete 10% of rows
                let deleted =
                    delete_records(&table, col("value").lt(lit((size as i64 / 10) * 100))).await;
                black_box(deleted);
            });
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_int64pk_single_delete,
    bench_int64pk_batch_delete,
    bench_int64pk_query_after_delete,
    bench_stringpk_single_delete,
    bench_stringpk_batch_delete,
    bench_stringpk_query_after_delete,
    bench_positionbased_single_delete,
    bench_positionbased_batch_delete,
    bench_positionbased_query_after_delete,
    bench_strategy_comparison,
);

criterion_main!(benches);
