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
#[cfg(feature = "result-correctness-chdb")]
pub mod chdb_engine;
pub mod clickbench_data;
pub mod dialect;
pub mod harness;
pub mod inventory;
pub mod oracle_lane;
pub mod report;
pub mod sqlite_engine;
pub mod sqllancer;
pub mod ssb_data;
pub mod standalone_engines;
pub mod tpcds_data;
pub mod tpch_data;

#[expect(unused_imports)] // re-exported for integration test crates
pub use harness::{
    assert_all_pass_or_excluded, assert_modes_agree_on_actual_results, compare_actual_results,
    compare_actual_results_detailed, execute_and_compare_cayenne_to_batches, execute_cayenne,
};

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use cayenne::metadata::CreateTableOptions;
use cayenne::{CayenneCatalog, CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_expr::dml::InsertOp;
use datafusion_physical_plan::collect;
use test_framework::layout::{Layout, LayoutFeature, TableKeys};
use test_framework::queries::validation::{
    QueryValidationFailReason, QueryValidationResult, RowOrder,
    compare_query_result_batches_with_sort_check, has_top_level_limit, has_top_level_order_by,
    row_order_from_sql,
};
use test_framework::queries::{
    Query, get_chbench_test_queries, get_clickbench_test_queries, get_tpcds_test_queries,
    get_tpch_test_queries,
};

/// Whether `dir` already holds a fixture that *this* generator finished writing.
///
/// The scratch tree (`target/cayenne_parity_scratch` by default) outlives
/// builds, branch switches and self-hosted CI jobs, so "some parquet is here"
/// says nothing about which generator produced it. Reusing on that alone lets a
/// fixture written before a generator change quietly revert whatever the change
/// was for — and a suite comparing two engines against the same stale rows still
/// agrees with itself, so nothing turns red.
///
/// The stamp closes both halves. It is written only after every file lands, so
/// an interrupted run leaves no stamp and is regenerated rather than reused
/// half-written; and it records `revision`, so a fixture from an older generator
/// no longer matches.
#[must_use]
pub fn fixture_is_current(dir: &Path, revision: &str) -> bool {
    std::fs::read_to_string(dir.join(FIXTURE_STAMP)).is_ok_and(|stamp| stamp.trim() == revision)
}

/// Record that `dir` now holds a complete fixture built by `revision`.
///
/// Call only once every file is written: the stamp is what a later run trusts.
pub fn mark_fixture_complete(dir: &Path, revision: &str) {
    std::fs::write(dir.join(FIXTURE_STAMP), revision).expect("write fixture stamp");
}

/// Digest of a generator's own source, used as its fixture revision.
///
/// Derived from the source rather than a hand-maintained constant because the
/// constant is what gets forgotten: the edit that changes the generated rows is
/// exactly the moment someone is thinking about the data, not the version.
#[must_use]
pub fn generator_revision(source: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    source.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Name of the stamp file. Dotted so directory scans that collect `*.parquet`
/// table names never pick it up.
const FIXTURE_STAMP: &str = ".fixture-complete";

/// Outcome for one inventory query on one engine pair.
/// Outcome of the **harness** after executing SQL and comparing actual batches.
#[derive(Debug, Clone)]
pub enum ParityOutcome {
    Pass,
    /// The two results matched and held nothing to compare: no rows, or rows
    /// holding only NULL. An empty agreement proves something only when empty is
    /// the query's intended answer on the fixture; when the fixture merely fails
    /// to reach the query's filters — the usual cause — it is a comparison that
    /// never happened. Not a `Pass`: accepted only where the inventory reviews
    /// why the answer is empty.
    Vacuous {
        detail: String,
    },
    /// Content matched, but the query's `ORDER BY` was not fully verified —
    /// a term that maps to no output column, an unparseable statement, or a key
    /// type with no comparator. Not a failure, and deliberately not a `Pass`:
    /// the whole point of the sort check is that unverified order must not read
    /// as verified.
    OrderUnchecked {
        reasons: Vec<String>,
    },
    Fail {
        detail: String,
    },
    Excluded {
        reason: String,
    },
    EngineError {
        side: &'static str,
        detail: String,
    },
}

impl ParityOutcome {
    /// Whether this outcome needs no explanation.
    ///
    /// `OrderUnchecked` is deliberately absent. It means the rows matched and
    /// the order they came back in was never verified, which is the outcome the
    /// sort check exists to surface — counting it here would let a resolver
    /// regression, or an `ORDER BY` the projection stops carrying, settle into a
    /// summary bucket instead of turning a lane red. A hole that has been looked
    /// at is named in the inventory and accepted by [`report::unexplained`]; one
    /// that has not fails.
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

/// A physical layout for the tables a lane loads into Cayenne: each table's
/// primary key, secondary indexes, and sort or clustering order, from the
/// benchmark's layout keys (`test_framework::layout`, the same keys
/// `testoperator --layout` configures through a Spicepod). The lanes create
/// Cayenne tables directly, so only what Cayenne's own table options carry
/// applies: `primary_key`, `indexes`, `sort` and `cluster`.
///
/// A query's answer must not depend on the layout, so each lane compares every
/// layout's answers with the same oracle answer.
#[derive(Debug, Clone)]
pub struct CayenneLayout {
    layout: Layout,
    tables: &'static [TableKeys],
}

impl CayenneLayout {
    /// # Panics
    ///
    /// When `layout` does not parse, names a feature the lanes cannot configure,
    /// or combines `sort` with `cluster`, which Cayenne refuses.
    #[must_use]
    pub fn new(layout: &str, tables: &'static [TableKeys]) -> Self {
        let layout: Layout = layout.parse().expect("valid layout");
        for feature in layout.features() {
            assert!(
                matches!(
                    feature,
                    LayoutFeature::PrimaryKey
                        | LayoutFeature::Indexes
                        | LayoutFeature::Sort
                        | LayoutFeature::Cluster
                ),
                "the Cayenne lanes create tables directly and cannot configure '{feature}'; testoperator --layout covers it end to end"
            );
        }
        assert!(
            !(layout.contains(LayoutFeature::Sort) && layout.contains(LayoutFeature::Cluster)),
            "Cayenne does not combine sort columns with clustering"
        );
        Self { layout, tables }
    }

    /// The layout as a report label, e.g. `primary_key,sort`.
    #[must_use]
    pub fn label(&self) -> String {
        self.layout.to_string()
    }

    /// Panics unless `provider` was created the way this layout asks for
    /// `table`: the check that keeps a layout run from comparing Cayenne's
    /// default layout under the layout's label.
    fn assert_applied(&self, table: &str, provider: &CayenneTableProvider) {
        let (primary_key, config, indexes) = self.table_options(table);
        let metadata = provider.metadata();
        assert_eq!(
            metadata.primary_key, primary_key,
            "layout '{}': table '{table}' has the wrong primary key",
            self.layout
        );
        assert_eq!(
            metadata.vortex_config.sort_columns, config.sort_columns,
            "layout '{}': table '{table}' has the wrong sort columns",
            self.layout
        );
        assert_eq!(
            metadata.vortex_config.cluster_by, config.cluster_by,
            "layout '{}': table '{table}' has the wrong clustering columns",
            self.layout
        );
        assert_eq!(
            provider.lookup_index_counters().is_some(),
            !indexes.is_empty(),
            "layout '{}': table '{table}' should declare secondary indexes exactly when the layout gives it some",
            self.layout
        );
    }

    /// The primary key, Vortex configuration and secondary indexes to create
    /// `table` with.
    fn table_options(
        &self,
        table: &str,
    ) -> (
        Vec<String>,
        cayenne::metadata::VortexConfig,
        Vec<Vec<String>>,
    ) {
        let keys = self
            .tables
            .iter()
            .find(|keys| keys.table == table)
            .unwrap_or_else(|| {
                panic!(
                    "table '{table}' has no layout keys, so layout '{}' would not apply to it",
                    self.layout
                )
            });
        let columns =
            |columns: &[&str]| columns.iter().map(ToString::to_string).collect::<Vec<_>>();
        let mut config = cayenne::metadata::VortexConfig::default();
        if self.layout.contains(LayoutFeature::Sort) {
            config.sort_columns = columns(keys.sort);
            config.sort_columns_origin = cayenne::metadata::SortColumnsOrigin::User;
        }
        if self.layout.contains(LayoutFeature::Cluster) {
            config.cluster_by = columns(keys.cluster);
        }
        let primary_key = if self.layout.contains(LayoutFeature::PrimaryKey) {
            columns(keys.primary_key)
        } else {
            Vec::new()
        };
        let indexes = if self.layout.contains(LayoutFeature::Indexes) {
            keys.indexes.iter().map(|index| columns(index)).collect()
        } else {
            Vec::new()
        };
        (primary_key, config, indexes)
    }
}

/// Secondary-index probes summed over a harness's tables: how many scans the
/// layout's indexes narrowed (`full` + `partial` coverage) and how many files
/// they handed a row selection, or `None` when no table declares an index.
#[must_use]
pub fn secondary_index_use(harness: &CayenneHarness) -> Option<(u64, u64)> {
    let counters: Vec<_> = harness
        .tables
        .values()
        .filter_map(|table| table.lookup_index_counters())
        .collect();
    (!counters.is_empty()).then(|| {
        counters.iter().fold((0, 0), |(probes, selections), c| {
            (
                probes + c.full + c.partial,
                selections + c.access_plans_attached,
            )
        })
    })
}

/// The layouts the oracle lanes load a benchmark suite under besides Cayenne's
/// default: a keyed, indexed table sorted other than in generated order, and a
/// keyed table clustered on two columns. Between them they reach the primary-key,
/// secondary-index, sort and clustering paths a Spicepod can configure.
pub const KEYED_LAYOUTS: &[&str] = &["primary_key,indexes,sort", "primary_key,cluster"];

/// [`KEYED_LAYOUTS`] without the primary key, for the reduced `hits` fixture,
/// whose ranking columns repeat by design so no column set is a key.
pub const UNKEYED_LAYOUTS: &[&str] = &["indexes,sort", "cluster"];

/// Cayenne's default layout (`None`) followed by each of `layouts` on
/// `query_set`'s tables, for [`oracle_lane::run_fixture_suite_with_layouts`].
///
/// # Panics
///
/// When `query_set` has no layout keys.
#[must_use]
pub fn with_layouts(
    query_set: &test_framework::queries::QuerySet,
    layouts: &[&str],
) -> Vec<Option<CayenneLayout>> {
    let tables = test_framework::layout::benchmark_tables(query_set)
        .unwrap_or_else(|| panic!("{query_set:?} has no layout keys"));
    std::iter::once(None)
        .chain(
            layouts
                .iter()
                .map(|layout| Some(CayenneLayout::new(layout, tables))),
        )
        .collect()
}

/// Build a Cayenne catalog + temp data dir.
pub struct CayenneHarness {
    pub _temp_dir: tempfile::TempDir,
    pub catalog: Arc<dyn MetadataCatalog>,
    pub data_path: PathBuf,
    pub tables: BTreeMap<String, Arc<CayenneTableProvider>>,
    /// The layout every table loaded from now on is created with; `None` is
    /// Cayenne's defaults (no key, no indexes, no sort or clustering).
    pub layout: Option<CayenneLayout>,
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
            layout: None,
        }
    }

    /// The primary key, Vortex configuration and secondary indexes to create
    /// `table` with: [`Self::layout`]'s, or Cayenne's defaults without one.
    fn table_options(
        &self,
        table: &str,
    ) -> (
        Vec<String>,
        cayenne::metadata::VortexConfig,
        Vec<Vec<String>>,
    ) {
        self.layout.as_ref().map_or_else(
            || {
                (
                    Vec::new(),
                    cayenne::metadata::VortexConfig::default(),
                    Vec::new(),
                )
            },
            |layout| layout.table_options(table),
        )
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

        // `CayenneTableProvider::create_table`, plus the layout's secondary
        // indexes, which only the builder takes.
        let (primary_key, vortex_config, secondary_indexes) = self.table_options(table_name);
        let table = Arc::new(
            CayenneTableProviderBuilder::new(
                Arc::clone(&self.catalog),
                Arc::new(RuntimeEnv::default()),
            )
            .with_secondary_indexes(secondary_indexes)
            .create(CreateTableOptions {
                table_name: table_name.to_string(),
                schema: Arc::clone(&schema),
                primary_key,
                on_conflict: None,
                base_path: table_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config,
            })
            .await
            .expect("create cayenne table"),
        );
        if let Some(layout) = &self.layout {
            layout.assert_applied(table_name, &table);
        }

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

impl CayenneHarness {
    /// A harness holding `{table}.parquet` from `parquet_dir` for each table,
    /// loaded the given way.
    pub async fn from_parquet_dir(parquet_dir: &Path, tables: &[&str], mode: LoadMode) -> Self {
        Self::from_parquet_dir_with_layout(parquet_dir, tables, mode, None).await
    }

    /// Like [`Self::from_parquet_dir`], with every table created under `layout`.
    pub async fn from_parquet_dir_with_layout(
        parquet_dir: &Path,
        tables: &[&str],
        mode: LoadMode,
        layout: Option<CayenneLayout>,
    ) -> Self {
        let mut harness = Self::new().await;
        harness.layout = layout;
        for table in tables {
            harness
                .load_parquet_table_with_mode(
                    table,
                    &parquet_dir.join(format!("{table}.parquet")),
                    mode,
                )
                .await;
        }
        harness
    }
}

/// The column kinds of `{table}.parquet` in `parquet_dir` for each table — what
/// [`dialect::translate`] needs to rewrite a suite's SQL for an oracle.
#[must_use]
pub fn fixture_column_kinds(parquet_dir: &Path, tables: &[&str]) -> dialect::ColumnKinds {
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let schemas: Vec<Schema> = tables
        .iter()
        .map(|table| {
            let path = parquet_dir.join(format!("{table}.parquet"));
            let file = std::fs::File::open(&path)
                .unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
            ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap_or_else(|e| panic!("read schema of {}: {e}", path.display()))
                .schema()
                .as_ref()
                .clone()
        })
        .collect();
    dialect::ColumnKinds::from_schemas(schemas.iter())
}

/// Where the lanes keep fixtures and logs between runs:
/// `CAYENNE_PARITY_SCRATCH`, or `target/cayenne_parity_scratch`.
#[must_use]
pub fn scratch_dir() -> PathBuf {
    let dir = std::env::var_os("CAYENNE_PARITY_SCRATCH").map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/cayenne_parity_scratch"),
        PathBuf::from,
    );
    std::fs::create_dir_all(&dir).expect("create the parity scratch dir");
    dir
}

/// A scale factor or size from the environment, or `default`. A value that is
/// set and does not parse fails rather than silently running the default.
#[must_use]
pub fn env_f64(name: &str, default: f64) -> f64 {
    match std::env::var(name) {
        Ok(text) => text
            .parse()
            .unwrap_or_else(|e| panic!("{name}={text} is not a number: {e}")),
        Err(_) => default,
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
    compare_results_detailed(query, cayenne, reference).outcome
}

/// A comparison together with what it could not establish.
///
/// Two callers need more than the outcome. `reason` tells one kind of failure
/// from another — `Fail::detail` renders it for a human, and branching on that
/// string would make control flow depend on a `Debug` format. `unchecked`
/// carries the order the sort check could not verify, and is populated even when
/// the content comparison failed: a caller that recovers from that failure would
/// otherwise report an order nothing verified as a clean pass.
pub struct ComparedResults {
    pub outcome: ParityOutcome,
    pub reason: Option<QueryValidationFailReason>,
    pub unchecked: Vec<String>,
}

/// Downgrade a content-only recovery that passed, when the sort check had
/// already declined to verify the order.
///
/// A lane that recovers from a failed comparison by comparing content another
/// way — the chDB lane retries a schema mismatch as sorted string rows —
/// establishes the rows and nothing about the order they came back in. Returning
/// its `Pass` unqualified would report an unverified order as verified, which is
/// the outcome the sort check exists to make impossible.
#[must_use]
pub fn keep_unverified_order(recovered: ParityOutcome, unchecked: Vec<String>) -> ParityOutcome {
    match recovered {
        ParityOutcome::Pass if !unchecked.is_empty() => {
            ParityOutcome::OrderUnchecked { reasons: unchecked }
        }
        settled => settled,
    }
}

/// [`compare_results`] with the reason and the coverage holes kept.
pub fn compare_results_detailed(
    query: &Query,
    cayenne: &[RecordBatch],
    reference: &[RecordBatch],
) -> ComparedResults {
    // Positional equality only where the row set itself depends on order. Elsewhere
    // multiset, so an `ORDER BY` on a non-unique key does not fail on the
    // engine-dependent order of tied rows. `compare_query_result_batches_with_sort_check`
    // then verifies each side against its own `ORDER BY`, which ties never violate —
    // so absorbing tie order here no longer costs the sort check with it.
    //
    // Both predicates are parser-backed: a `LIMIT` or `ORDER BY` inside a subquery
    // does not make the outer result order-dependent, and a substring search cannot
    // tell that apart from a top-level one.
    let order = if has_top_level_order_by(&query.sql) && has_top_level_limit(&query.sql) {
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
        Ok(comparison) => {
            let unchecked = comparison.unchecked;
            if comparison.result == QueryValidationResult::Pass
                && holds_no_value(cayenne)
                && holds_no_value(reference)
            {
                return ComparedResults {
                    outcome: ParityOutcome::Vacuous {
                        detail: format!(
                            "both sides returned no non-NULL value ({} and {} rows)",
                            row_count(cayenne),
                            row_count(reference)
                        ),
                    },
                    reason: None,
                    unchecked,
                };
            }
            match comparison.result {
                QueryValidationResult::Pass if unchecked.is_empty() => ComparedResults {
                    outcome: ParityOutcome::Pass,
                    reason: None,
                    unchecked,
                },
                // Rows matched, but part of the ORDER BY went unverified. That is
                // a coverage hole, reported as one rather than as a clean pass.
                QueryValidationResult::Pass => ComparedResults {
                    outcome: ParityOutcome::OrderUnchecked {
                        reasons: unchecked.clone(),
                    },
                    reason: None,
                    unchecked,
                },
                QueryValidationResult::Fail(reason) => ComparedResults {
                    outcome: ParityOutcome::Fail {
                        detail: format!("{reason:?}"),
                    },
                    reason: Some(reason),
                    unchecked,
                },
            }
        }
        Err(e) => ComparedResults {
            outcome: ParityOutcome::Fail {
                detail: format!("compare error: {e}"),
            },
            reason: None,
            unchecked: Vec::new(),
        },
    }
}

/// Whether `batches` hold no value a comparison could check: no rows, or only
/// NULL cells — what `SUM` over an empty input returns.
#[must_use]
pub fn holds_no_value(batches: &[RecordBatch]) -> bool {
    batches.iter().all(|batch| {
        batch
            .columns()
            .iter()
            .all(|column| column.null_count() == column.len())
    })
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
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
