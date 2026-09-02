// Copyright 2024-2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Support code for **result-correctness** integration tests (inventory,
//! fixtures, standalone engines, SQLLancer corpus, reports).
//!
//! Standalone engines (`standalone_engines`) are out-of-Spice oracles
//! (`duckdb` / `rusqlite` / chDB). Spice accelerators under test are Cayenne
//! here and DuckDB/SQLite accelerators in `runtime`’s `result_correctness` test.
//!
//! Not used by Criterion `vs_duckdb_*` / `vs_chdb_*` performance benches.
//! See `tests/correctness/README.md`.

#![allow(dead_code)]
#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::many_single_char_names)]
#![allow(clippy::map_unwrap_or)]
#![allow(clippy::unnested_or_patterns)]
#![allow(clippy::needless_raw_string_hashes)]

pub mod chbench_data;
pub mod harness;
pub mod inventory;
pub mod report;
pub mod sqlite_engine;
pub mod sqllancer;
pub mod ssb_data;
pub mod standalone_engines;

#[expect(unused_imports)] // re-exported for integration test crates
pub use harness::{
    assert_all_pass_or_excluded, assert_modes_agree_on_actual_results, compare_actual_results,
    execute_and_compare_cayenne_to_batches, execute_cayenne,
};

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_expr::dml::InsertOp;
use datafusion_physical_plan::collect;
use test_framework::queries::validation::{
    QueryValidationResult, RowOrder, compare_query_result_batches_with_sort_check,
    row_order_from_sql,
};
use test_framework::queries::{
    Query, get_chbench_test_queries, get_clickbench_test_queries, get_tpcds_test_queries,
    get_tpch_test_queries,
};

/// Outcome for one inventory query on one engine pair.
/// Outcome of the **harness** after executing SQL and comparing actual batches.
#[derive(Debug, Clone)]
pub enum ParityOutcome {
    Pass,
    Fail { detail: String },
    Excluded { reason: String },
    EngineError { side: &'static str, detail: String },
}

impl ParityOutcome {
    #[must_use]
    pub fn is_pass_or_excluded(&self) -> bool {
        matches!(self, Self::Pass | Self::Excluded { .. })
    }
}

/// Micro-bench SQL shapes shared by `vs_duckdb_*` / `vs_chdb_*` benches.
/// Table names: fact `t` (id, name, value), dim `d` (id, region).
#[must_use]
pub fn micro_bench_queries() -> Vec<Query> {
    vec![
        Query::new(
            "micro_count_star".into(),
            "SELECT COUNT(*) FROM t".into(),
            false,
        ),
        Query::new(
            "micro_sum_value".into(),
            "SELECT SUM(value) FROM t".into(),
            false,
        ),
        Query::new(
            "micro_filter_sum".into(),
            "SELECT SUM(value) FROM t WHERE id BETWEEN 10 AND 50".into(),
            false,
        ),
        Query::new(
            "micro_count_value".into(),
            "SELECT COUNT(value) FROM t".into(),
            false,
        ),
        Query::new(
            "micro_min_value".into(),
            "SELECT MIN(value) FROM t".into(),
            false,
        ),
        Query::new(
            "micro_max_value".into(),
            "SELECT MAX(value) FROM t".into(),
            false,
        ),
        Query::new(
            "micro_avg_value".into(),
            "SELECT AVG(value) FROM t".into(),
            false,
        ),
        Query::new(
            "micro_agg_rollup".into(),
            "SELECT COUNT(*), SUM(value), MIN(value), MAX(value), AVG(value) FROM t".into(),
            false,
        ),
        Query::new(
            "micro_groupby_name".into(),
            "SELECT name, COUNT(*), SUM(value) FROM t GROUP BY name".into(),
            false,
        ),
        Query::new(
            "micro_join_agg".into(),
            "SELECT d.region, SUM(t.value) FROM t JOIN d ON t.id = d.id GROUP BY d.region".into(),
            false,
        ),
        Query::new(
            "micro_join_filter".into(),
            "SELECT SUM(t.value) FROM t JOIN d ON t.id = d.id WHERE d.region = 'NA'".into(),
            false,
        ),
        Query::new(
            "micro_pk_lookup".into(),
            "SELECT id, name, value FROM t WHERE id = 42".into(),
            false,
        ),
        Query::new(
            "micro_order_limit".into(),
            "SELECT id, name, value FROM t ORDER BY id LIMIT 10".into(),
            false,
        ),
    ]
}

/// TPC-H tables produced by DuckDB `dbgen`.
pub const TPCH_TABLES: &[&str] = &[
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

/// How data is loaded into Cayenne — mirrors spicepod `refresh_mode` surfaces.
///
/// Correctness only: after load, query results must match the same final
/// dataset regardless of mode. Not a performance matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LoadMode {
    /// Single bulk load via `InsertOp::Overwrite` (refresh_mode: full).
    Full,
    /// Multiple `InsertOp::Append` batches (refresh_mode: append).
    Append,
    /// CDC path `write_cdc_append_stream` + `finish()` (refresh_mode: changes).
    Changes,
}

impl LoadMode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            LoadMode::Full => "full",
            LoadMode::Append => "append",
            LoadMode::Changes => "changes",
        }
    }

    #[must_use]
    pub fn all() -> &'static [LoadMode] {
        &[LoadMode::Full, LoadMode::Append, LoadMode::Changes]
    }
}

/// Build a Cayenne catalog + temp data dir.
pub struct CayenneHarness {
    pub _temp_dir: tempfile::TempDir,
    pub catalog: Arc<dyn MetadataCatalog>,
    pub data_path: PathBuf,
    pub tables: BTreeMap<String, Arc<CayenneTableProvider>>,
}

impl CayenneHarness {
    pub async fn new() -> Self {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let data_path = temp_dir.path().join("data");
        std::fs::create_dir_all(&data_path).expect("data dir");
        let db_path = temp_dir.path().join("catalog.db");
        let catalog = Arc::new(
            CayenneCatalog::new(format!("sqlite://{}", db_path.display())).expect("catalog"),
        );
        catalog.init().await.expect("catalog init");
        Self {
            _temp_dir: temp_dir,
            catalog: catalog as Arc<dyn MetadataCatalog>,
            data_path,
            tables: BTreeMap::new(),
        }
    }

    /// Create a Cayenne table from a parquet file (schema inferred via DataFusion).
    /// Default load path is [`LoadMode::Full`].
    pub async fn load_parquet_table(&mut self, table_name: &str, parquet_path: &Path) {
        self.load_parquet_table_with_mode(table_name, parquet_path, LoadMode::Full)
            .await;
    }

    /// Load parquet into Cayenne using the given refresh-mode analog.
    pub async fn load_parquet_table_with_mode(
        &mut self,
        table_name: &str,
        parquet_path: &Path,
        mode: LoadMode,
    ) {
        let ctx = SessionContext::new();
        let path_str = parquet_path.to_string_lossy().into_owned();
        let df = ctx
            .read_parquet(path_str.as_str(), ParquetReadOptions::default())
            .await
            .expect("read parquet for schema");
        let schema = Arc::new(df.schema().as_arrow().clone());

        let table_path = self
            .data_path
            .join(format!("{table_name}_{}", mode.as_str()));
        std::fs::create_dir_all(&table_path).expect("table data dir");

        let table = Arc::new(
            CayenneTableProvider::create_table(
                Arc::clone(&self.catalog),
                CreateTableOptions {
                    table_name: table_name.to_string(),
                    schema: Arc::clone(&schema),
                    primary_key: vec![],
                    on_conflict: None,
                    base_path: table_path.to_string_lossy().to_string(),
                    partition_column: None,
                    vortex_config: cayenne::metadata::VortexConfig::default(),
                },
                Arc::new(RuntimeEnv::default()),
            )
            .await
            .expect("create cayenne table"),
        );

        match mode {
            LoadMode::Full => {
                let input_exec = df
                    .create_physical_plan()
                    .await
                    .expect("parquet physical plan");
                let insert_plan = table
                    .insert_into(&ctx.state(), input_exec, InsertOp::Overwrite)
                    .await
                    .expect("overwrite insert plan");
                let _ = collect(insert_plan, ctx.task_ctx())
                    .await
                    .expect("overwrite insert collect");
            }
            LoadMode::Append => {
                // Chunk the source into several Append ops (simulates append refresh).
                let batches = df.collect().await.expect("collect parquet batches");
                let chunks = split_batches_into_chunks(&batches, 4);
                for chunk in chunks {
                    if chunk.is_empty() {
                        continue;
                    }
                    let chunk_schema = chunk[0].schema();
                    let input_exec =
                        datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
                            &[chunk],
                            chunk_schema,
                            None,
                        )
                        .expect("memory exec");
                    let insert_plan = table
                        .insert_into(&ctx.state(), input_exec, InsertOp::Append)
                        .await
                        .expect("append insert plan");
                    let _ = collect(insert_plan, ctx.task_ctx())
                        .await
                        .expect("append insert collect");
                }
            }
            LoadMode::Changes => {
                // CDC path: stream each RecordBatch through write_cdc_append_stream.
                let batches = df.collect().await.expect("collect parquet for cdc");
                let task_ctx = ctx.task_ctx();
                for batch in batches {
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    let schema = batch.schema();
                    let stream = Box::pin(
                        datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                            schema,
                            futures::stream::iter(vec![
                                Ok::<_, datafusion::error::DataFusionError>(batch),
                            ]),
                        ),
                    );
                    let cdc = table
                        .write_cdc_append_stream(stream, &task_ctx)
                        .await
                        .expect("cdc write stage A");
                    cdc.finish().await.expect("cdc write finish");
                }
            }
        }

        self.tables.insert(table_name.to_string(), table);
    }

    /// Load an in-memory RecordBatch as a named table.
    pub async fn load_batch(&mut self, table_name: &str, batch: RecordBatch) {
        let schema = batch.schema();
        let table_path = self.data_path.join(table_name);
        std::fs::create_dir_all(&table_path).expect("table data dir");

        let table = Arc::new(
            CayenneTableProvider::create_table(
                Arc::clone(&self.catalog),
                CreateTableOptions {
                    table_name: table_name.to_string(),
                    schema: Arc::clone(&schema),
                    primary_key: vec![],
                    on_conflict: None,
                    base_path: table_path.to_string_lossy().to_string(),
                    partition_column: None,
                    vortex_config: cayenne::metadata::VortexConfig::default(),
                },
                Arc::new(RuntimeEnv::default()),
            )
            .await
            .expect("create cayenne table"),
        );

        let ctx = SessionContext::new();
        let input_exec = datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            schema,
            None,
        )
        .expect("memory exec");
        let insert_plan = table
            .insert_into(&ctx.state(), input_exec, InsertOp::Append)
            .await
            .expect("insert plan");
        let _ = collect(insert_plan, ctx.task_ctx())
            .await
            .expect("insert collect");

        self.tables.insert(table_name.to_string(), table);
    }

    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>, String> {
        use cayenne::optimizer_rules::{
            CayenneAntiJoinSortMergeRewriter, CayenneDynamicFilterSharing,
            CayenneMaintainedAggregateRewriter, CayenneStatsAggregateRewriter,
        };
        use datafusion::execution::session_state::SessionStateBuilder;

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(CayenneDynamicFilterSharing::new()))
            .with_physical_optimizer_rule(Arc::new(CayenneMaintainedAggregateRewriter::new()))
            .with_physical_optimizer_rule(Arc::new(CayenneStatsAggregateRewriter::new()))
            .with_physical_optimizer_rule(Arc::new(CayenneAntiJoinSortMergeRewriter::new()))
            .build();
        let ctx = SessionContext::new_with_state(state);
        for (name, table) in &self.tables {
            ctx.register_table(name.as_str(), Arc::clone(table) as Arc<dyn TableProvider>)
                .map_err(|e| format!("register {name}: {e}"))?;
        }
        let df = ctx.sql(sql).await.map_err(|e| format!("sql: {e}"))?;
        df.collect().await.map_err(|e| format!("collect: {e}"))
    }
}

/// Split batches into up to `n_chunks` non-empty groups for append-mode loads.
fn split_batches_into_chunks(batches: &[RecordBatch], n_chunks: usize) -> Vec<Vec<RecordBatch>> {
    let n_chunks = n_chunks.max(1);
    if batches.is_empty() {
        return vec![vec![]; n_chunks];
    }
    let mut chunks: Vec<Vec<RecordBatch>> = (0..n_chunks).map(|_| Vec::new()).collect();
    for (i, batch) in batches.iter().enumerate() {
        chunks[i % n_chunks].push(batch.clone());
    }
    // If a single large batch, slice it across chunks.
    if batches.len() == 1 && batches[0].num_rows() > n_chunks {
        let batch = &batches[0];
        let rows = batch.num_rows();
        let step = rows.div_ceil(n_chunks);
        chunks = Vec::new();
        let mut start = 0;
        while start < rows {
            let end = (start + step).min(rows);
            chunks.push(vec![batch.slice(start, end - start)]);
            start = end;
        }
    }
    chunks.into_iter().filter(|c| !c.is_empty()).collect()
}

/// Write a RecordBatch to parquet.
pub fn write_parquet(batch: &RecordBatch, path: &Path) {
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::properties::WriterProperties;

    let file = std::fs::File::create(path).expect("create parquet");
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props)).expect("writer");
    writer.write(batch).expect("write");
    writer.close().expect("close");
}

/// Canonical micro-bench fact schema (id, name, value).
#[must_use]
pub fn micro_fact_schema() -> Arc<Schema> {
    use arrow::datatypes::{DataType, Field};
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// Canonical micro-bench dim schema (id, region).
#[must_use]
pub fn micro_dim_schema() -> Arc<Schema> {
    use arrow::datatypes::{DataType, Field};
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
    ]))
}

#[must_use]
pub fn make_fact_batch(rows: usize, groups: usize) -> RecordBatch {
    use arrow::array::{Int64Array, StringArray};
    let schema = micro_fact_schema();
    let group_count = groups.max(1);
    let ids: Vec<i64> = (0..rows as i64).collect();
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
    .expect("fact batch")
}

#[must_use]
pub fn make_dim_batch(rows: usize) -> RecordBatch {
    use arrow::array::{Int64Array, StringArray};
    const REGIONS: [&str; 4] = ["NA", "EU", "APAC", "LATAM"];
    let schema = micro_dim_schema();
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

/// Compare Cayenne vs reference batches for one query.
///
/// Uses multiset equality when SQL has no `ORDER BY`. When `ORDER BY` is
/// present, still uses multiset for engine-vs-engine parity unless the query
/// also has `LIMIT`/`OFFSET` — non-unique sort keys make row order among ties
/// implementation-defined, but the full result multiset must match. With
/// `LIMIT`, order among ties changes *which* rows appear, so we preserve
/// order comparison to surface nondeterminism (callers may exclude).
pub fn compare_results(
    query: &Query,
    cayenne: &[RecordBatch],
    reference: &[RecordBatch],
) -> ParityOutcome {
    let sql_upper = query.sql.to_ascii_uppercase();
    let has_order = sql_upper.contains("ORDER BY");
    let has_limit = sql_upper.contains("LIMIT") || sql_upper.contains("OFFSET");
    // Positional equality only where the row set itself depends on order. Elsewhere
    // multiset, so an `ORDER BY` on a non-unique key does not fail on the
    // engine-dependent order of tied rows. `compare_query_result_batches_with_sort_check`
    // then verifies each side against its own `ORDER BY`, which ties never violate —
    // so absorbing tie order here no longer costs the sort check with it.
    let order = if has_order && has_limit {
        RowOrder::Preserved
    } else {
        RowOrder::Multiset
    };
    match compare_query_result_batches_with_sort_check(
        &query.name,
        &query.sql,
        cayenne,
        reference,
        order,
    ) {
        Ok(QueryValidationResult::Pass) => ParityOutcome::Pass,
        Ok(QueryValidationResult::Fail(reason)) => ParityOutcome::Fail {
            detail: format!("{reason:?}"),
        },
        Err(e) => ParityOutcome::Fail {
            detail: format!("compare error: {e}"),
        },
    }
}

/// All suite queries that form the parity inventory (no exclusions applied).
pub fn suite_queries() -> Vec<(String, Query)> {
    let mut out = Vec::new();
    for q in get_tpch_test_queries(None) {
        out.push(("tpch".to_string(), q));
    }
    for q in get_tpcds_test_queries(None, Some(1.0)) {
        out.push(("tpcds".to_string(), q));
    }
    for q in get_clickbench_test_queries(None) {
        out.push(("clickbench".to_string(), q));
    }
    for q in get_chbench_test_queries(None) {
        out.push(("chbench".to_string(), q));
    }
    for q in ssb_data::ssb_queries() {
        out.push(("ssb".to_string(), q));
    }
    // SpiceBench SF1 built-in scenario is TPC-H (see spiceai/spicebench README).
    for q in get_tpch_test_queries(None) {
        let mut name = q.name.to_string();
        name = name.replacen("tpch_", "spicebench_", 1);
        out.push((
            "spicebench".to_string(),
            Query::new(name.into(), Arc::clone(&q.sql), false),
        ));
    }
    for q in sqllancer::sqllancer_queries() {
        out.push(("sqllancer".to_string(), q));
    }
    for q in micro_bench_queries() {
        out.push(("micro".to_string(), q));
    }
    out
}

/// Detect whether SQL contains an explicit ORDER BY (for reporting).
#[must_use]
pub fn sql_has_order_by(sql: &str) -> bool {
    row_order_from_sql(sql) == RowOrder::Preserved
}
