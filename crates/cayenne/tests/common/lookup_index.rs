/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! What the secondary index tests share: opening an indexed table, writing
//! and querying it, reading its index counters and plans, and waiting for its
//! index to cover every file.

#![expect(
    clippy::expect_used,
    reason = "Integration test helpers panic when fixture setup or queries fail"
)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::SchemaRef;
use cayenne::lookup_index::{IndexPersistence, LookupIndexCounters, LookupIndexVerification};
use cayenne::metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::provider::CayenneContext;
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::SessionContext;
use datafusion_expr::dml::InsertOp;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

use super::TestFixture;

/// A file-mode configuration whose files stay small, so a test's writes
/// produce several files for the index to cover.
pub fn file_mode_config() -> VortexConfig {
    VortexConfig {
        target_vortex_file_size_mb: 1,
        ..VortexConfig::default()
    }
}

/// A `mode: memory` configuration, as `apply_memory_mode_overrides` sets it,
/// with background work off so a test decides when anything moves.
pub fn memory_mode_config() -> VortexConfig {
    VortexConfig {
        memory_mode: true,
        cdc_mem_tier_shards: 1,
        cdc_mem_tier_max_age_ms: 0,
        cdc_mem_tier_checkpoint_interval_ms: 0,
        cdc_mem_tier_seal_age_ms: 0,
        compaction_background_interval_ms: 0,
        cold_tier_location: None,
        inline_max_rows: 0,
        inline_max_bytes: 0,
        inline_max_buffer_bytes: 0,
        cdc_mem_tier_max_bytes: 0,
        cdc_durability: CdcDurability::Memory,
        deletion_mode: DeletionMode::Key,
        ..VortexConfig::default()
    }
}

/// The table a test opens: its name, schema, `indexes` entries and
/// configuration, the column it upserts on, if any, and whether its index runs
/// persist, when the test decides rather than the environment.
pub struct TableSpec<'a> {
    pub name: &'a str,
    pub schema: SchemaRef,
    pub indexes: &'a [&'a [&'a str]],
    pub config: VortexConfig,
    pub upsert_key: Option<&'a str>,
    /// Enabled, as at runtime; a test of the unpersisted path opts out. Set
    /// explicitly, so the environment does not decide what a test exercises.
    pub persistence: IndexPersistence,
}

impl<'a> TableSpec<'a> {
    /// An append-only file-mode table (see [`file_mode_config`]).
    pub fn new(name: &'a str, schema: SchemaRef, indexes: &'a [&'a [&'a str]]) -> Self {
        Self {
            name,
            schema,
            indexes,
            config: file_mode_config(),
            upsert_key: None,
            persistence: IndexPersistence::Enabled,
        }
    }

    pub fn config(mut self, config: VortexConfig) -> Self {
        self.config = config;
        self
    }

    pub fn upsert_key(mut self, column: &'a str) -> Self {
        self.upsert_key = Some(column);
        self
    }

    pub fn persistence(mut self, persistence: IndexPersistence) -> Self {
        self.persistence = persistence;
        self
    }
}

/// Creates the table `spec` describes, or reopens it when the catalog
/// already holds one of its name: the same call the accelerator makes on
/// every registration.
pub async fn open_table(
    fixture: &TestFixture,
    runtime_env: Arc<RuntimeEnv>,
    spec: TableSpec<'_>,
) -> Arc<CayenneTableProvider> {
    let context = CayenneContext::new(&spec.config, Arc::clone(&runtime_env), spec.name);
    let options = CreateTableOptions {
        table_name: spec.name.to_string(),
        schema: spec.schema,
        primary_key: spec
            .upsert_key
            .map(|key| vec![key.to_string()])
            .unwrap_or_default(),
        on_conflict: spec
            .upsert_key
            .map(|key| OnConflict::Upsert(ColumnReference::new(vec![key.to_string()]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: spec.config,
    };
    let catalog = Arc::clone(&fixture.catalog);
    let catalog: Arc<dyn MetadataCatalog> = catalog;
    let builder = CayenneTableProviderBuilder::new(catalog, runtime_env)
        .with_context(context)
        .with_index_persistence(spec.persistence);
    Arc::new(
        builder
            .with_secondary_indexes(
                spec.indexes
                    .iter()
                    .map(|columns| columns.iter().map(|c| (*c).to_string()).collect())
                    .collect(),
            )
            .create(options)
            .await
            .expect("create or reopen table"),
    )
}

/// A runtime whose query memory pool holds `bytes`.
pub fn runtime_with_pool(bytes: usize) -> (Arc<RuntimeEnv>, Arc<dyn MemoryPool>) {
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes));
    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::clone(&pool))
        .build_arc()
        .expect("runtime env");
    (runtime_env, pool)
}

/// Replaces the table's rows with `batches`, in one write.
pub async fn overwrite(provider: &Arc<CayenneTableProvider>, batches: Vec<RecordBatch>) {
    super::write_batches(provider, batches, InsertOp::Overwrite)
        .await
        .expect("overwrite");
}

/// Appends `batch` through SQL (`INSERT INTO name SELECT * FROM src`), so the
/// write takes the path a SQL `INSERT` does rather than calling the table's
/// `insert_into` directly.
pub async fn insert(provider: &Arc<CayenneTableProvider>, name: &str, batch: RecordBatch) {
    let ctx = SessionContext::new();
    ctx.register_table(name, Arc::clone(provider) as Arc<dyn TableProvider>)
        .expect("register target");
    let mem = datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch]])
        .expect("memtable");
    ctx.register_table("src", Arc::new(mem))
        .expect("register src");
    ctx.sql(&format!("INSERT INTO {name} SELECT * FROM src"))
        .await
        .expect("insert plan")
        .collect()
        .await
        .expect("insert");
}

/// Runs `sql` against `provider` registered as `name`.
pub async fn query<T: TableProvider + 'static>(
    provider: &Arc<T>,
    name: &str,
    sql: &str,
) -> Vec<RecordBatch> {
    let ctx = SessionContext::new();
    ctx.register_table(name, Arc::clone(provider) as Arc<dyn TableProvider>)
        .expect("register");
    ctx.sql(sql)
        .await
        .unwrap_or_else(|e| panic!("plan {sql}: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e}"))
}

/// Every row of `batches` as `cell|cell|…`, NULL spelled out, sorted, so two
/// results compare regardless of row order.
pub fn rendered(batches: &[RecordBatch]) -> Vec<String> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = (0..batch.num_columns())
                .map(|column| {
                    let array = batch.column(column);
                    if array.is_null(row) {
                        "NULL".to_string()
                    } else {
                        arrow::util::display::array_value_to_string(array, row)
                            .expect("render cell")
                    }
                })
                .collect();
            rows.push(cells.join("|"));
        }
    }
    rows.sort();
    rows
}

/// The values of the first column of `batches`, an `Int64` column, in order.
pub fn int64_column(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("the first column is Int64")
                .values()
                .to_vec()
        })
        .collect()
}

/// The `{field}=` counts a plan reports (`candidate_files`,
/// `uncovered_files`, ...), summed over every scan that reports one.
pub fn explain_total(plan: &str, field: &str) -> usize {
    plan.split(&format!(" {field}="))
        .skip(1)
        .filter_map(|rest| {
            rest.split(|c: char| !c.is_ascii_digit())
                .next()?
                .parse::<usize>()
                .ok()
        })
        .sum()
}

/// The table's secondary index counters.
pub fn counters(provider: &Arc<CayenneTableProvider>) -> LookupIndexCounters {
    provider
        .lookup_index_counters()
        .expect("the table declares indexes")
}

/// Runs `attempt` every `interval` until it returns `Ok`, and returns that
/// value. An `Err` carries the state `attempt` observed; once `timeout` has
/// passed, the test fails with `failure` applied to the last such state.
pub async fn poll_until<T, S>(
    timeout: Duration,
    interval: Duration,
    mut attempt: impl AsyncFnMut() -> Result<T, S>,
    failure: impl FnOnce(S) -> String,
) -> T {
    let deadline = Instant::now() + timeout;
    loop {
        let state = match attempt().await {
            Ok(done) => return done,
            Err(state) => state,
        };
        assert!(
            Instant::now() < deadline,
            "{} (waited {timeout:?})",
            failure(state)
        );
        tokio::time::sleep(interval).await;
    }
}

/// Runs `poke` (a lookup, which requests a background build of any file the
/// index does not cover) until the index covers every file of the table,
/// failing with the last verification after 60 seconds; returns that
/// verification, which must also agree with a read-back of the files.
pub async fn until_covered(
    provider: &Arc<CayenneTableProvider>,
    mut poke: impl AsyncFnMut(),
) -> LookupIndexVerification {
    poll_until(
        Duration::from_secs(60),
        Duration::from_millis(20),
        async || {
            poke().await;
            let verification = provider
                .verify_lookup_index_against_read_back()
                .await
                .expect("verify the index");
            assert!(verification.agrees(), "{verification:?}");
            if verification.uncovered_files == 0 {
                Ok(verification)
            } else {
                Err(verification)
            }
        },
        |verification| format!("the index was not built over every file: {verification:?}"),
    )
    .await
}

/// A small, seedable pseudo-random generator (`SplitMix64`), so a test's data
/// is the same on every run.
pub struct SplitMix64(pub u64);

impl SplitMix64 {
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}
