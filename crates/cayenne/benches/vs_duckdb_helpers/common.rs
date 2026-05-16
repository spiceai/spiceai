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

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::util::pretty::pretty_format_batches;
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

/// Which Cayenne metastore backend to use in a fixture.
///
/// `Sqlite` is Cayenne's default (no `cayenne_metastore` param). `Turso` is
/// available when the bench is built with `--features turso` and matches
/// `cayenne_metastore: turso` in spicepods. The DuckDB side is unaffected;
/// pairing a `Turso` Cayenne fixture against the same DuckDB fixture isolates
/// the metastore's contribution to overall numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Metastore {
    Sqlite,
    #[cfg(feature = "turso")]
    Turso,
}

impl Metastore {
    /// Stable lane label used in `BenchmarkId`s.
    #[must_use]
    pub fn lane(self) -> &'static str {
        match self {
            Metastore::Sqlite => "cayenne",
            #[cfg(feature = "turso")]
            Metastore::Turso => "cayenne_turso",
        }
    }

    fn connection_string(self, db_path: &Path) -> String {
        let path = db_path.to_string_lossy();
        match self {
            Metastore::Sqlite => format!("sqlite://{path}"),
            #[cfg(feature = "turso")]
            Metastore::Turso => format!("libsql://{path}"),
        }
    }
}

/// All Cayenne lanes a bench should run. Compile-time gated on the `turso`
/// feature so benches built without it cleanly drop to a single lane.
pub const CAYENNE_LANES: &[Metastore] = &[
    Metastore::Sqlite,
    #[cfg(feature = "turso")]
    Metastore::Turso,
];

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
///
/// `name` is unique per row (`name_{id}`) so GROUP BY on `name` yields one
/// group per row. Use [`make_batch_grouped`] when low cardinality is wanted.
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

/// Build a deterministic batch with `groups` distinct `name` values, used by
/// the GROUP BY bench so the aggregation kernel produces a bounded number of
/// output groups regardless of row count.
pub fn make_batch_grouped(
    schema: Arc<Schema>,
    start_id: i64,
    rows: usize,
    groups: usize,
) -> RecordBatch {
    let group_count = groups.max(1);
    let ids: Vec<i64> = (0..rows as i64).map(|i| start_id + i).collect();
    let names: Vec<String> = (0..rows)
        .map(|i| format!("group_{}", i % group_count))
        .collect();
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

/// Build a small "dimension" batch for the join bench. `id` is a foreign key
/// into the fact table; `region` is a 4-way low-cardinality dimension.
pub fn make_dim_batch(schema: Arc<Schema>, rows: usize) -> RecordBatch {
    const REGIONS: [&str; 4] = ["NA", "EU", "APAC", "LATAM"];
    let ids: Vec<i64> = (0..rows as i64).collect();
    let regions: Vec<&str> = (0..rows).map(|i| REGIONS[i % REGIONS.len()]).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(regions)),
        ],
    )
    .expect("dim batch")
}

/// Schema for the dim table used by the join bench.
pub fn dim_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
    ]))
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

/// A clean Cayenne table backed by a fresh metastore + temp data dir.
///
/// The backend (`SQLite` or `Turso`) is selected at fixture-creation time
/// via [`Metastore`] so each bench can run multiple metastore lanes.
pub struct CayenneFixture {
    pub _temp_dir: TempDir,
    pub table: Arc<CayenneTableProvider>,
    pub catalog: Arc<dyn MetadataCatalog>,
}

pub async fn setup_cayenne(table_name: &str) -> CayenneFixture {
    setup_cayenne_with(table_name, Metastore::Sqlite, vec![], None, schema()).await
}

pub async fn setup_cayenne_pk(table_name: &str) -> CayenneFixture {
    setup_cayenne_with(
        table_name,
        Metastore::Sqlite,
        vec!["id".to_string()],
        Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        schema(),
    )
    .await
}

/// Build a Cayenne fixture with a chosen metastore backend (default `schema()`).
pub async fn setup_cayenne_for(table_name: &str, metastore: Metastore) -> CayenneFixture {
    setup_cayenne_with(table_name, metastore, vec![], None, schema()).await
}

/// Build a Cayenne fixture with a chosen metastore backend AND a single-column
/// `id` primary key with upsert on-conflict resolution.
pub async fn setup_cayenne_pk_for(table_name: &str, metastore: Metastore) -> CayenneFixture {
    setup_cayenne_with(
        table_name,
        metastore,
        vec!["id".to_string()],
        Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        schema(),
    )
    .await
}

/// Build a Cayenne fixture that uses the dim-table schema (for the join bench).
pub async fn setup_cayenne_dim_for(table_name: &str, metastore: Metastore) -> CayenneFixture {
    setup_cayenne_with(table_name, metastore, vec![], None, dim_schema()).await
}

async fn setup_cayenne_with(
    table_name: &str,
    metastore: Metastore,
    primary_key: Vec<String>,
    on_conflict: Option<OnConflict>,
    table_schema: Arc<Schema>,
) -> CayenneFixture {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let data_path = temp_dir.path().join("data");
    tokio::fs::create_dir_all(&data_path)
        .await
        .expect("data dir");
    let db_path = temp_dir.path().join("catalog.db");
    let catalog =
        Arc::new(CayenneCatalog::new(metastore.connection_string(&db_path)).expect("catalog"));
    catalog.init().await.expect("catalog init");

    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: table_name.to_string(),
                schema: table_schema,
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

impl DuckDbFixture {
    /// Path to the on-disk `.duckdb` file. Used by the concurrent bench to
    /// open a second connection from a background thread (DuckDB connections
    /// are not `Send`).
    #[must_use]
    pub fn db_path(&self) -> PathBuf {
        self._temp_dir.path().join("duck.db")
    }
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

/// DuckDB fixture for the join bench: a `t` fact table (default schema) and
/// a `d` dim table (id, region). Both engines see the same shape so the
/// resulting join plans are directly comparable.
pub fn setup_duckdb_with_dim(fact_table: &str, dim_table: &str) -> DuckDbFixture {
    let fixture = setup_duckdb(fact_table);
    fixture
        .conn
        .execute_batch(&format!(
            "CREATE TABLE {dim_table} (id BIGINT NOT NULL, region VARCHAR NOT NULL);"
        ))
        .expect("duckdb create dim table");
    fixture
}

/// Upsert via DuckDB's `INSERT ... ON CONFLICT DO UPDATE`. Apples-to-apples
/// with Cayenne's `OnConflict::Upsert` on the `id` primary key.
pub fn duckdb_upsert_parquet(conn: &Connection, table_name: &str, parquet_path: &Path) {
    conn.execute_batch(&format!(
        "INSERT INTO {table_name} SELECT * FROM read_parquet('{}') \
         ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, value = EXCLUDED.value;",
        parquet_path.display()
    ))
    .expect("duckdb upsert parquet");
}

/// Insert a small VALUES tuple list — used by the burst bench to mirror the
/// fine-grained per-burst insert path without paying parquet decode cost.
pub fn duckdb_insert_rows(conn: &Connection, table_name: &str, batch: &RecordBatch) {
    use arrow::array::{Array, Int64Array, StringArray};

    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("ids");
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("names");
    let values = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("values");

    let mut sql = format!("INSERT INTO {table_name} VALUES ");
    for i in 0..batch.num_rows() {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!(
            "({}, '{}', {})",
            ids.value(i),
            names.value(i).replace('\'', "''"),
            values.value(i)
        ));
    }
    sql.push(';');
    conn.execute_batch(&sql).expect("duckdb insert rows");
}

/// Insert the rows of `batch` into DuckDB's dim table.
pub fn duckdb_insert_dim_rows(conn: &Connection, table_name: &str, batch: &RecordBatch) {
    use arrow::array::{Array, Int64Array, StringArray};

    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("ids");
    let regions = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("regions");

    let mut sql = format!("INSERT INTO {table_name} VALUES ");
    for i in 0..batch.num_rows() {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!(
            "({}, '{}')",
            ids.value(i),
            regions.value(i).replace('\'', "''"),
        ));
    }
    sql.push(';');
    conn.execute_batch(&sql).expect("duckdb insert dim rows");
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

/// Run a SQL query against two Cayenne tables registered as `t` and `d`.
/// Used by the join bench so the SQL matches the DuckDB form.
pub async fn cayenne_query_join(
    fact: &Arc<CayenneTableProvider>,
    dim: &Arc<CayenneTableProvider>,
    sql: &str,
) -> Vec<RecordBatch> {
    use datafusion::datasource::TableProvider;
    use datafusion::prelude::SessionContext;

    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(fact) as Arc<dyn TableProvider>)
        .expect("register fact");
    ctx.register_table("d", Arc::clone(dim) as Arc<dyn TableProvider>)
        .expect("register dim");
    let df = ctx.sql(sql).await.expect("cayenne join sql");
    df.collect().await.expect("cayenne join collect")
}

/// Capture optimized and executed plans for a Cayenne/DuckDB query pair.
///
/// Files are written to `target/cayenne_vs_duckdb_plans/<label>.md` by default.
/// Set `CAYENNE_DUCKDB_PLAN_DIR` to choose a different output directory.
pub async fn capture_comparison_plans(
    label: &str,
    cayenne_table: &Arc<CayenneTableProvider>,
    duckdb_conn: &Connection,
    cayenne_sql: &str,
    duckdb_sql: &str,
) {
    let cayenne_explain = cayenne_plan_text(cayenne_table, "EXPLAIN", cayenne_sql).await;
    let cayenne_analyze = cayenne_plan_text(cayenne_table, "EXPLAIN ANALYZE", cayenne_sql).await;
    let duckdb_explain = duckdb_plan_text(duckdb_conn, "EXPLAIN", duckdb_sql);
    let duckdb_analyze = duckdb_plan_text(duckdb_conn, "EXPLAIN ANALYZE", duckdb_sql);

    let mut content = String::new();
    content.push_str("# Cayenne vs DuckDB Plans\n\n");
    content.push_str(&format!("## {label}\n\n"));
    content.push_str("### Cayenne SQL\n\n```sql\n");
    content.push_str(cayenne_sql);
    content.push_str("\n```\n\n### DuckDB SQL\n\n```sql\n");
    content.push_str(duckdb_sql);
    content.push_str("\n```\n\n### Cayenne EXPLAIN\n\n```text\n");
    content.push_str(&cayenne_explain);
    content.push_str("\n```\n\n### Cayenne EXPLAIN ANALYZE\n\n```text\n");
    content.push_str(&cayenne_analyze);
    content.push_str("\n```\n\n### DuckDB EXPLAIN\n\n```text\n");
    content.push_str(&duckdb_explain);
    content.push_str("\n```\n\n### DuckDB EXPLAIN ANALYZE\n\n```text\n");
    content.push_str(&duckdb_analyze);
    content.push_str("\n```\n");

    let output_path = plan_output_dir().join(format!("{}.md", sanitize_plan_label(label)));
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent).expect("create plan output directory");
    }
    fs::write(&output_path, content).expect("write plan capture");
    eprintln!("captured Cayenne/DuckDB plans: {}", output_path.display());
}

/// Capture optimized and executed plans for the parquet-ingest path used by
/// the ingestion comparison benchmarks.
pub async fn capture_parquet_ingest_plans(
    label: &str,
    cayenne_table: &Arc<CayenneTableProvider>,
    duckdb_conn: &Connection,
    duckdb_table_name: &str,
    parquet_path: &Path,
) {
    let cayenne_explain =
        cayenne_parquet_insert_plan_text(cayenne_table, parquet_path, false).await;
    let cayenne_analyze = cayenne_parquet_insert_plan_text(cayenne_table, parquet_path, true).await;
    let duckdb_sql = format!(
        "INSERT INTO {duckdb_table_name} SELECT * FROM read_parquet('{}')",
        parquet_path.display()
    );
    let duckdb_explain = duckdb_plan_text(duckdb_conn, "EXPLAIN", &duckdb_sql);
    let duckdb_analyze = duckdb_plan_text(duckdb_conn, "EXPLAIN ANALYZE", &duckdb_sql);

    let mut content = String::new();
    content.push_str("# Cayenne vs DuckDB Plans\n\n");
    content.push_str(&format!("## {label}\n\n"));
    content.push_str("### Cayenne Operation\n\n```text\n");
    content.push_str("CayenneTableProvider::insert_into(ctx.read_parquet(...))");
    content.push_str("\n```\n\n### DuckDB SQL\n\n```sql\n");
    content.push_str(&duckdb_sql);
    content.push_str("\n```\n\n### Cayenne EXPLAIN\n\n```text\n");
    content.push_str(&cayenne_explain);
    content.push_str("\n```\n\n### Cayenne EXPLAIN ANALYZE\n\n```text\n");
    content.push_str(&cayenne_analyze);
    content.push_str("\n```\n\n### DuckDB EXPLAIN\n\n```text\n");
    content.push_str(&duckdb_explain);
    content.push_str("\n```\n\n### DuckDB EXPLAIN ANALYZE\n\n```text\n");
    content.push_str(&duckdb_analyze);
    content.push_str("\n```\n");

    let output_path = plan_output_dir().join(format!("{}.md", sanitize_plan_label(label)));
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent).expect("create plan output directory");
    }
    fs::write(&output_path, content).expect("write plan capture");
    eprintln!("captured Cayenne/DuckDB plans: {}", output_path.display());
}

async fn cayenne_plan_text(
    table: &Arc<CayenneTableProvider>,
    plan_kind: &str,
    sql: &str,
) -> String {
    use datafusion::datasource::TableProvider;
    use datafusion::prelude::SessionContext;

    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(table) as Arc<dyn TableProvider>)
        .expect("register cayenne table for plan capture");
    let df = ctx
        .sql(&format!("{plan_kind} {sql}"))
        .await
        .expect("cayenne explain sql");
    let batches = df.collect().await.expect("cayenne explain collect");
    pretty_format_batches(&batches)
        .expect("format cayenne explain")
        .to_string()
}

fn duckdb_plan_text(conn: &Connection, plan_kind: &str, sql: &str) -> String {
    let explain_sql = format!("{plan_kind} {sql}");
    let mut stmt = conn.prepare(&explain_sql).expect("duckdb explain prepare");
    let batches: Vec<RecordBatch> = stmt
        .query_arrow([])
        .expect("duckdb explain query")
        .collect();
    pretty_format_batches(&batches)
        .expect("format duckdb explain")
        .to_string()
}

async fn cayenne_parquet_insert_plan_text(
    table: &Arc<CayenneTableProvider>,
    parquet_path: &Path,
    execute: bool,
) -> String {
    use datafusion::datasource::TableProvider;
    use datafusion::prelude::{ParquetReadOptions, SessionContext};
    use datafusion_expr::dml::InsertOp;

    let parquet_path = parquet_path.to_string_lossy().into_owned();
    let ctx = SessionContext::new();
    let df = ctx
        .read_parquet::<&str>(parquet_path.as_str(), ParquetReadOptions::default())
        .await
        .expect("cayenne read_parquet for plan capture");
    let input_exec = df
        .create_physical_plan()
        .await
        .expect("cayenne parquet physical plan for capture");
    let insert_plan = table
        .insert_into(&ctx.state(), input_exec, InsertOp::Append)
        .await
        .expect("cayenne insert plan for capture");

    if execute {
        let results = datafusion_physical_plan::collect(Arc::clone(&insert_plan), ctx.task_ctx())
            .await
            .expect("cayenne insert collect for plan capture");
        let output = pretty_format_batches(&results)
            .expect("format cayenne insert output")
            .to_string();
        format!(
            "{}\n\nOutput:\n{}",
            datafusion::physical_plan::displayable(insert_plan.as_ref()).indent(true),
            output,
        )
    } else {
        datafusion::physical_plan::displayable(insert_plan.as_ref())
            .indent(true)
            .to_string()
    }
}

fn plan_output_dir() -> PathBuf {
    std::env::var_os("CAYENNE_DUCKDB_PLAN_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/cayenne_vs_duckdb_plans"))
}

fn sanitize_plan_label(label: &str) -> String {
    label
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => ch,
            _ => '_',
        })
        .collect()
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
