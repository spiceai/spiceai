// Copyright 2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0

//! Shared helpers for the Cayenne-vs-DuckDB micro-benchmarks.
//!
//! Each `vs_duckdb_*` bench compares Cayenne and DuckDB on the same Arrow
//! input, doing identical logical work. Helpers in this module own the
//! pieces that are identical across benches — schema, fixture generation,
//! parquet materialization, and the canonical Cayenne / DuckDB setup paths.
//!
//! Included via `#[path = "vs_duckdb_helpers/common.rs"] mod common;`
//! from each bench file. Placing the helper inside a subdirectory keeps
//! Cargo's bench auto-discovery from picking it up as a standalone target,
//! so no `autobenches = false` is required on the cayenne crate.

#![allow(dead_code)]
#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_sign_loss)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use duckdb::Connection;
use tempfile::TempDir;

/// Canonical schema for the comparison benches.
///
/// Three columns chosen to mirror the shape of a TPC-H `customer` / `orders`
/// row that's been keyed on a single int64 primary key:
/// - `id`: int64 PK (dense, monotonic)
/// - `name`: utf8 (variable-width, low cardinality on repeat)
/// - `value`: int64 (numeric payload)
pub fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// Build a deterministic batch of `rows` rows starting at `start_id`.
pub fn make_batch(schema: Arc<Schema>, start_id: i64, rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..rows as i64).map(|i| start_id + i).collect();
    let names: Vec<String> = ids.iter().map(|id| format!("name_{id}")).collect();
    let values: Vec<i64> = ids.iter().map(|id| id * 100).collect();

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

/// Write a single record batch to a parquet file so both engines can ingest
/// from the same on-disk source — the realistic Spice ingestion path.
pub fn write_parquet(batch: &RecordBatch, path: &Path) {
    let file = std::fs::File::create(path).expect("create parquet");
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).expect("arrow writer");
    writer.write(batch).expect("write");
    writer.close().expect("close");
}

/// A clean Cayenne table backed by a fresh SQLite metastore + temp data dir.
pub struct CayenneFixture {
    pub _temp_dir: TempDir,
    pub table: Arc<CayenneTableProvider>,
    pub catalog: Arc<dyn MetadataCatalog>,
}

pub async fn setup_cayenne(table_name: &str) -> CayenneFixture {
    setup_cayenne_with_pk(table_name, vec![], None).await
}

pub async fn setup_cayenne_pk(table_name: &str) -> CayenneFixture {
    setup_cayenne_with_pk(
        table_name,
        vec!["id".to_string()],
        Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
    )
    .await
}

async fn setup_cayenne_with_pk(
    table_name: &str,
    primary_key: Vec<String>,
    on_conflict: Option<OnConflict>,
) -> CayenneFixture {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let data_path = temp_dir.path().join("data");
    tokio::fs::create_dir_all(&data_path)
        .await
        .expect("data dir");
    let db_path = temp_dir.path().join("catalog.db");
    let catalog = Arc::new(
        CayenneCatalog::new(format!("sqlite://{}", db_path.to_string_lossy())).expect("catalog"),
    );
    catalog.init().await.expect("catalog init");

    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: table_name.to_string(),
                schema: schema(),
                primary_key,
                on_conflict,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: cayenne::metadata::VortexConfig::default(),
            },
            Arc::new(RuntimeEnv::default()),
        )
        .await
        .expect("cayenne create_table"),
    );

    CayenneFixture {
        _temp_dir: temp_dir,
        table,
        catalog: Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
    }
}

/// A clean DuckDB file-mode database with the same schema.
///
/// File-backed (not in-memory) for parity with Cayenne, which only supports
/// `mode: file`. Comparing Cayenne-file vs DuckDB-memory would not be fair
/// (see `tools/testoperator/dispatch/perf-cayenne-vs-duckdb/README.md`).
pub struct DuckDbFixture {
    pub _temp_dir: TempDir,
    pub conn: Connection,
}

pub fn setup_duckdb(table_name: &str) -> DuckDbFixture {
    setup_duckdb_with_pk(table_name, false)
}

pub fn setup_duckdb_pk(table_name: &str) -> DuckDbFixture {
    setup_duckdb_with_pk(table_name, true)
}

fn setup_duckdb_with_pk(table_name: &str, with_pk: bool) -> DuckDbFixture {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let db_path = temp_dir.path().join("duck.db");
    let conn = Connection::open(&db_path).expect("duckdb open");
    let pk_clause = if with_pk { " PRIMARY KEY" } else { "" };
    conn.execute_batch(&format!(
        "CREATE TABLE {table_name} (id BIGINT{pk_clause}, name VARCHAR NOT NULL, value BIGINT NOT NULL);"
    ))
    .expect("duckdb create table");
    DuckDbFixture {
        _temp_dir: temp_dir,
        conn,
    }
}

/// Bulk-insert via DuckDB's native parquet loader. This is DuckDB's
/// fastest ingestion path and the apples-to-apples comparison for
/// Cayenne's parquet-source insert path.
pub fn duckdb_insert_parquet(conn: &Connection, table_name: &str, parquet_path: &Path) {
    conn.execute_batch(&format!(
        "INSERT INTO {table_name} SELECT * FROM read_parquet('{}');",
        parquet_path.display()
    ))
    .expect("duckdb insert parquet");
}

/// Insert an Arrow batch through Cayenne via the DataFusion `insert_into` API.
/// Mirrors how spiced loads accelerator data in production.
pub async fn cayenne_insert(table: &Arc<CayenneTableProvider>, batch: RecordBatch) -> u64 {
    use datafusion::datasource::TableProvider;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::dml::InsertOp;

    let ctx = SessionContext::new();
    let schema = Arc::clone(batch.schema_ref());
    let input_exec =
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).expect("memory exec");
    let insert_plan = table
        .insert_into(&ctx.state(), input_exec, InsertOp::Append)
        .await
        .expect("cayenne insert plan");
    let results = datafusion_physical_plan::collect(insert_plan, ctx.task_ctx())
        .await
        .expect("cayenne insert collect");
    results
        .first()
        .and_then(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
        })
        .map_or(0, |rows| rows.value(0))
}

/// Insert from a parquet file through Cayenne via DataFusion's parquet
/// reader. Mirrors spiced's `file:` connector → accelerator ingestion path
/// and gives parity with `duckdb_insert_parquet` (both engines now consume
/// the same on-disk parquet, including the decode work).
pub async fn cayenne_insert_from_parquet(
    table: &Arc<CayenneTableProvider>,
    parquet_path: &Path,
) -> u64 {
    use datafusion::datasource::TableProvider;
    use datafusion::prelude::{ParquetReadOptions, SessionContext};
    use datafusion_expr::dml::InsertOp;

    let parquet_path = parquet_path.to_string_lossy().into_owned();
    let ctx = SessionContext::new();
    let df = ctx
        .read_parquet::<&str>(parquet_path.as_str(), ParquetReadOptions::default())
        .await
        .expect("cayenne read_parquet");
    let input_exec = df
        .create_physical_plan()
        .await
        .expect("cayenne physical plan");
    let insert_plan = table
        .insert_into(&ctx.state(), input_exec, InsertOp::Append)
        .await
        .expect("cayenne insert plan");
    let results = datafusion_physical_plan::collect(insert_plan, ctx.task_ctx())
        .await
        .expect("cayenne insert collect");
    results
        .first()
        .and_then(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
        })
        .map_or(0, |rows| rows.value(0))
}

/// Run a SQL query through Cayenne and return the collected batches.
pub async fn cayenne_query(table: &Arc<CayenneTableProvider>, sql: &str) -> Vec<RecordBatch> {
    use datafusion::datasource::TableProvider;
    use datafusion::prelude::SessionContext;

    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register table");
    let df = ctx.sql(sql).await.expect("cayenne sql");
    df.collect().await.expect("cayenne collect")
}

/// Run a SQL query through DuckDB and return the number of rows in the
/// result. Discarding the row content keeps the bench focused on engine
/// work, not on Rust-side decoding.
pub fn duckdb_query_count(conn: &Connection, sql: &str) -> i64 {
    let mut stmt = conn.prepare(sql).expect("duckdb prepare");
    let mut rows = stmt.query([]).expect("duckdb query");
    let mut count: i64 = 0;
    while let Some(_row) = rows.next().expect("duckdb row") {
        count += 1;
    }
    count
}

/// Run a SQL aggregate query that returns a single scalar i64.
pub fn duckdb_query_scalar(conn: &Connection, sql: &str) -> i64 {
    let mut stmt = conn.prepare(sql).expect("duckdb prepare");
    stmt.query_row([], |row| row.get::<_, i64>(0))
        .expect("duckdb query_row")
}
