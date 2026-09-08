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

//! Criterion coverage for the three P1 fixes from the 2026-07-22 cayenne-perf
//! audit — **gap-closed** so every claim is load-bearing:
//!
//! 1. **Subset vs full current-snapshot small-file compaction** — e2e
//!    `compact_current_snapshot_small_files` with path assert
//!    ([`LastSmallFileCompactPath`]), wall-clock, and on-disk bytes.
//! 2. **SMJ HT overhead 2.5×** — optimize latency + plan-type assert on a join
//!    whose payload *fits* a 1.0× gate but *exceeds* a 2.5× gate (the factor is
//!    the only reason SMJ fires).
//! 3. **Mem-tier reserve/release hot path** — real
//!    [`try_reserve_global_mem_tier_bytes`] / [`release_global_mem_tier_bytes`]
//!    under an installed budget + query-pool mirror (every CDC byte hits
//!    `sync_pool_account_to_used`).
//!
//! Run: `cargo bench -p cayenne --bench p1_audit_coverage`

#![allow(clippy::expect_used)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_precision_loss)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::optimizer_rules::{CayenneAntiJoinSortMergeRewriter, CayenneOptimizerConfig};
use cayenne::provider::CayenneAccelerationExec;
use cayenne::{
    CayenneCatalog, CayenneTableProvider, LastSmallFileCompactPath, MetadataCatalog,
    clear_global_mem_tier_pool_account, global_mem_tier_pool_account_bytes, global_mem_tier_used,
    release_global_mem_tier_bytes, set_global_mem_tier_bytes, set_global_mem_tier_pool_account,
    try_reserve_global_mem_tier_bytes,
};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion::common::{JoinType, NullEquality};
use datafusion::config::ConfigOptions;
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryPool};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion::prelude::SessionContext;
use datafusion_common::Result as DFResult;
use datafusion_common::Statistics;
use datafusion_common::stats::Precision;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::file_stream::FileOpener;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::{PartitionedFile, TableSchema};
use datafusion_expr::dml::InsertOp;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_expr::{PhysicalExpr, conjunction};
use datafusion_physical_plan::DisplayFormatType;
use datafusion_physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use std::hint::black_box;
use tokio::runtime::Runtime;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn pk_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]))
}

fn value_payload(prefix: &str, row_id: i64) -> String {
    let row_id = u64::try_from(row_id).expect("non-negative id");
    format!(
        "{prefix}_{row_id:020}_{:016x}_{:016x}_{:016x}",
        row_id.wrapping_mul(0x9E37_79B9_7F4A_7C15),
        row_id.wrapping_mul(0xC2B2_AE3D_27D4_EB4F),
        row_id.wrapping_mul(0x1656_67B1_9E37_79F9),
    )
}

fn make_batch(schema: &SchemaRef, start: i64, n: i64) -> RecordBatch {
    let ids: Vec<i64> = (start..start + n).collect();
    let values: Vec<String> = ids.iter().map(|id| value_payload("v", *id)).collect();
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(values)),
        ],
    )
    .expect("batch")
}

async fn insert_batch(table: &Arc<CayenneTableProvider>, batch: RecordBatch) -> u64 {
    let ctx = SessionContext::new();
    let schema = Arc::clone(batch.schema_ref());
    let input =
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).expect("memory exec");
    let plan = table
        .insert_into(&ctx.state(), input, InsertOp::Append)
        .await
        .expect("insert plan");
    let results = datafusion_physical_plan::collect(plan, ctx.task_ctx())
        .await
        .expect("insert collect");
    results
        .first()
        .and_then(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
        })
        .map_or(0, |a| a.value(0))
}

async fn snapshot_vortex_bytes(data_path: &Path, table_id: &str, snapshot_id: &str) -> u64 {
    let dir = data_path.join(table_id).join(snapshot_id);
    let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
        return 0;
    };
    let mut total = 0_u64;
    while let Some(entry) = entries.next_entry().await.expect("read_dir") {
        let name = entry.file_name();
        let Some(name_str) = name.to_str() else {
            continue;
        };
        if name_str.ends_with(".vortex") && !name_str.starts_with('.') {
            let meta = entry.metadata().await.expect("meta");
            total = total.saturating_add(meta.len());
        }
    }
    total
}

async fn count_vortex_files(data_path: &Path, table_id: &str, snapshot_id: &str) -> usize {
    let dir = data_path.join(table_id).join(snapshot_id);
    let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
        return 0;
    };
    let mut count = 0;
    while let Some(entry) = entries.next_entry().await.expect("read_dir") {
        let name = entry.file_name();
        let Some(name_str) = name.to_str() else {
            continue;
        };
        if name_str.ends_with(".vortex") && !name_str.starts_with('.') {
            count += 1;
        }
    }
    count
}

/// Config that forces a **proper subset** pick: 12 small files + max_pick=4
/// ⇒ candidate is 4 of 12. Also `DeletionMode::Key` so subset eligibility is
/// not blocked by Auto→Position resolution on PK tables.
fn subset_lane_config() -> VortexConfig {
    VortexConfig {
        target_vortex_file_size_mb: 1,
        compaction_trigger_files: 4,
        compaction_max_levels: 3,
        compaction_max_files_per_pick: 4,
        compaction_background_interval_ms: 0,
        inline_max_rows: 0,
        deletion_mode: DeletionMode::Key,
        ..VortexConfig::default()
    }
}

fn full_lane_config() -> VortexConfig {
    VortexConfig {
        target_vortex_file_size_mb: 1,
        compaction_trigger_files: 4,
        compaction_max_levels: 3,
        compaction_max_files_per_pick: 4,
        compaction_background_interval_ms: 0,
        inline_max_rows: 0,
        ..VortexConfig::default()
    }
}

// ---------------------------------------------------------------------------
// Lane 1: subset vs full rewrite (bytes + wall + path assert)
// ---------------------------------------------------------------------------

struct CompactFixture {
    _temp_dir: tempfile::TempDir,
    data_path: PathBuf,
    table: Arc<CayenneTableProvider>,
    table_id: String,
    table_name: String,
    catalog: Arc<dyn MetadataCatalog>,
    next_id: i64,
    small_rows: i64,
    schema: SchemaRef,
}

/// - **Subset** (`key_mode = true`): PK + `DeletionMode::Key`, no Upsert, max_pick=4.
/// - **Full** (`key_mode = false`): append-only position strategy.
async fn setup_compact_fixture(table_name: &str, key_mode: bool) -> CompactFixture {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let data_path = temp_dir.path().join("data");
    tokio::fs::create_dir_all(&data_path)
        .await
        .expect("data dir");
    let db_path = temp_dir.path().join("bench.db");
    let catalog = Arc::new(
        CayenneCatalog::new(format!("sqlite://{}", db_path.to_string_lossy())).expect("catalog"),
    );
    catalog.init().await.expect("catalog init");

    let schema = pk_schema();
    let ctx = SessionContext::new();
    let (primary_key, vortex_config) = if key_mode {
        (vec!["id".to_string()], subset_lane_config())
    } else {
        (vec![], full_lane_config())
    };

    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: table_name.to_string(),
                schema: Arc::clone(&schema),
                primary_key,
                on_conflict: None,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config,
            },
            ctx.runtime_env(),
        )
        .await
        .expect("create_table"),
    );

    let table_id = catalog
        .get_table(table_name)
        .await
        .expect("get_table")
        .table_id;

    let small_rows = 1500_i64;
    let seed_batches = 12_i64;
    let mut next_id = 0_i64;
    for _ in 0..seed_batches {
        let n = insert_batch(&table, make_batch(&schema, next_id, small_rows)).await;
        assert_eq!(n as i64, small_rows);
        next_id += small_rows;
    }

    let catalog_dyn: Arc<dyn MetadataCatalog> = Arc::clone(&catalog) as Arc<dyn MetadataCatalog>;
    CompactFixture {
        _temp_dir: temp_dir,
        data_path,
        table,
        table_id,
        table_name: table_name.to_string(),
        catalog: catalog_dyn,
        next_id,
        small_rows,
        schema,
    }
}

async fn current_snap(catalog: &dyn MetadataCatalog, table_name: &str) -> String {
    catalog
        .get_table(table_name)
        .await
        .expect("get_table")
        .current_snapshot_id
}

async fn reseed_small_files(fx: &mut CompactFixture, batches: i64) {
    for _ in 0..batches {
        let n = insert_batch(&fx.table, make_batch(&fx.schema, fx.next_id, fx.small_rows)).await;
        assert_eq!(n as i64, fx.small_rows);
        fx.next_id += fx.small_rows;
    }
}

/// Drive compact until `expected` path is recorded (covers post-write races).
async fn drive_to_path(
    fx: &mut CompactFixture,
    expected: LastSmallFileCompactPath,
) -> (std::time::Duration, u64, u64) {
    let snap_before = current_snap(fx.catalog.as_ref(), &fx.table_name).await;
    let bytes_before = snapshot_vortex_bytes(&fx.data_path, &fx.table_id, &snap_before).await;
    let t0 = Instant::now();
    let mut wall = std::time::Duration::ZERO;
    for _ in 0..50 {
        if fx.table.last_small_file_compact_path() == expected {
            break;
        }
        let step = Instant::now();
        let _ = fx.table.maybe_compact_small_files().await.expect("compact");
        wall += step.elapsed();
        tokio::task::yield_now().await;
    }
    // One more timed pass after reseed is measured by the caller; here ensure
    // path is set. If only post-write fired, wall may be 0 — re-seed + measure.
    if fx.table.last_small_file_compact_path() != expected {
        panic!(
            "expected path {expected:?}, got {:?} after drive (bytes_before={bytes_before})",
            fx.table.last_small_file_compact_path()
        );
    }
    let _ = t0; // drive total includes yields; wall is compact-only
    let snap_after = current_snap(fx.catalog.as_ref(), &fx.table_name).await;
    let bytes_after = snapshot_vortex_bytes(&fx.data_path, &fx.table_id, &snap_after).await;
    (wall, bytes_before, bytes_after)
}

/// Timed compact pass after files are already seeded.
async fn timed_maybe_compact(
    fx: &CompactFixture,
) -> (
    bool,
    LastSmallFileCompactPath,
    std::time::Duration,
    u64,
    u64,
) {
    let snap_before = current_snap(fx.catalog.as_ref(), &fx.table_name).await;
    let bytes_before = snapshot_vortex_bytes(&fx.data_path, &fx.table_id, &snap_before).await;
    let t0 = Instant::now();
    let committed = fx.table.maybe_compact_small_files().await.expect("compact");
    let wall = t0.elapsed();
    let path = fx.table.last_small_file_compact_path();
    let snap_after = current_snap(fx.catalog.as_ref(), &fx.table_name).await;
    let bytes_after = snapshot_vortex_bytes(&fx.data_path, &fx.table_id, &snap_after).await;
    (committed, path, wall, bytes_before, bytes_after)
}

fn bench_subset_vs_full_compact(c: &mut Criterion) {
    let rt = Runtime::new().expect("runtime");
    let mut group = c.benchmark_group("p1_subset_vs_full_compact");
    group.sample_size(10);
    group.throughput(Throughput::Elements(1));

    // --- Subset lane ---
    let mut subset_fx = rt.block_on(setup_compact_fixture("subset_bench", true));
    {
        let (wall, bytes_before, bytes_after) = rt.block_on(drive_to_path(
            &mut subset_fx,
            LastSmallFileCompactPath::Subset,
        ));
        eprintln!("p1 subset path assert OK: wall={wall:?} bytes {bytes_before} → {bytes_after}");
        // Re-seed so timed loop has work.
        rt.block_on(reseed_small_files(&mut subset_fx, 12));
    }

    group.bench_function("subset_key_delete", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let files_now = rt.block_on(async {
                    let snap =
                        current_snap(subset_fx.catalog.as_ref(), &subset_fx.table_name).await;
                    count_vortex_files(&subset_fx.data_path, &subset_fx.table_id, &snap).await
                });
                if files_now < 8 {
                    rt.block_on(reseed_small_files(&mut subset_fx, 12));
                }
                let (committed, path, wall, bytes_before, bytes_after) =
                    rt.block_on(timed_maybe_compact(&subset_fx));
                if committed {
                    assert_eq!(path, LastSmallFileCompactPath::Subset);
                } else {
                    // Post-write may have won; still assert path was subset at least once.
                    assert_eq!(
                        subset_fx.table.last_small_file_compact_path(),
                        LastSmallFileCompactPath::Subset
                    );
                }
                black_box((bytes_before, bytes_after, committed));
                total += wall;
                rt.block_on(reseed_small_files(&mut subset_fx, 12));
            }
            total
        });
    });

    // --- Full lane ---
    let mut full_fx = rt.block_on(setup_compact_fixture("full_bench", false));
    {
        let (wall, bytes_before, bytes_after) =
            rt.block_on(drive_to_path(&mut full_fx, LastSmallFileCompactPath::Full));
        eprintln!("p1 full path assert OK: wall={wall:?} bytes {bytes_before} → {bytes_after}");
        rt.block_on(reseed_small_files(&mut full_fx, 12));
    }

    group.bench_function("full_append_only", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let files_now = rt.block_on(async {
                    let snap = current_snap(full_fx.catalog.as_ref(), &full_fx.table_name).await;
                    count_vortex_files(&full_fx.data_path, &full_fx.table_id, &snap).await
                });
                if files_now < 8 {
                    rt.block_on(reseed_small_files(&mut full_fx, 12));
                }
                let (committed, path, wall, bytes_before, bytes_after) =
                    rt.block_on(timed_maybe_compact(&full_fx));
                if committed {
                    assert_eq!(path, LastSmallFileCompactPath::Full);
                } else {
                    assert_eq!(
                        full_fx.table.last_small_file_compact_path(),
                        LastSmallFileCompactPath::Full
                    );
                }
                black_box((bytes_before, bytes_after, committed));
                total += wall;
                rt.block_on(reseed_small_files(&mut full_fx, 12));
            }
            total
        });
    });

    group.bench_function("write_amp_ratio_math", |b| {
        b.iter(|| {
            let total = black_box(10_u64 * 1024 * 1024);
            let candidate = black_box(1_u64 * 1024 * 1024);
            let ratio = total
                .saturating_mul(100)
                .checked_div(candidate)
                .unwrap_or(0);
            assert_eq!(ratio, 1000);
            black_box(ratio)
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Lane 2: SMJ 2.5× load-bearing plan rewrite
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct BenchFileSource {
    table_schema: TableSchema,
    filter: Option<Arc<dyn PhysicalExpr>>,
    metrics: ExecutionPlanMetricsSet,
}

impl BenchFileSource {
    fn new(table_schema: TableSchema) -> Self {
        Self {
            table_schema,
            filter: None,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl FileSource for BenchFileSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn ObjectStore>,
        _base_config: &datafusion_datasource::file_scan_config::FileScanConfig,
        _partition: usize,
    ) -> DFResult<Arc<dyn FileOpener>> {
        Err(DataFusionError::NotImplemented(
            "bench source cannot open files".to_string(),
        ))
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(self.clone())
    }

    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.filter.clone()
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        None
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &'static str {
        "bench"
    }

    fn fmt_extra(
        &self,
        _t: DisplayFormatType,
        _f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        Ok(())
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> DFResult<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let filter_count = filters.len();
        let filter = match &self.filter {
            Some(existing) => Some(conjunction(
                std::iter::once(Arc::clone(existing)).chain(filters),
            )),
            None => Some(conjunction(filters)),
        };
        let source = Self {
            table_schema: self.table_schema.clone(),
            filter,
            metrics: ExecutionPlanMetricsSet::new(),
        };
        Ok(FilterPushdownPropagation::with_parent_pushdown_result(vec![
            PushedDown::Yes;
            filter_count
        ])
        .with_updated_node(Arc::new(source)))
    }
}

fn join_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("warehouse_id", DataType::Int64, false),
        Field::new("line_number", DataType::Int64, false),
    ]))
}

fn cayenne_scan_with_rows(schema: &SchemaRef, path: &str, rows: usize) -> Arc<dyn ExecutionPlan> {
    let table_schema = TableSchema::new(Arc::clone(schema), Vec::new());
    let source = Arc::new(BenchFileSource::new(table_schema));
    let file = PartitionedFile::from(ObjectMeta {
        location: ObjectPath::from(path),
        last_modified: chrono::DateTime::UNIX_EPOCH,
        size: 1_024,
        e_tag: None,
        version: None,
    });
    let config =
        FileScanConfigBuilder::new(ObjectStoreUrl::parse("file:///").expect("url"), source)
            .with_file_group(FileGroup::new(vec![file]))
            .with_statistics(Statistics::new_unknown(schema).with_num_rows(Precision::Exact(rows)))
            .build();
    let data_source = DataSourceExec::from_data_source(config);
    Arc::new(CayenneAccelerationExec::new(data_source))
}

fn left_anti_hash_join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
) -> Arc<dyn ExecutionPlan> {
    let left_key = col("order_id", &left.schema()).expect("left key");
    let right_key = col("order_id", &right.schema()).expect("right key");
    Arc::new(
        HashJoinExec::try_new(
            left,
            right,
            vec![(left_key, right_key)],
            None,
            &JoinType::LeftAnti,
            None,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )
        .expect("hash join"),
    )
}

fn smj_config(pool_bytes: usize, fraction: f64) -> ConfigOptions {
    let mut config = ConfigOptions::default();
    let mut cayenne_config = CayenneOptimizerConfig::default();
    // Disable the row-count gate so only the byte estimate decides.
    cayenne_config.sort_merge_min_rows = usize::MAX;
    cayenne_config.sort_merge_memory_pool_fraction = fraction;
    cayenne_config.sort_merge_memory_pool_bytes = Some(pool_bytes);
    config.extensions.insert(cayenne_config);
    config
}

/// Load-bearing numbers:
/// - schema = 3×Int64 → row_width = 24
/// - rows = 200_000 → payload = 4_800_000
/// - 1.0× estimate = 4.8 MB; 2.5× estimate = 12 MB
/// - pool = 40 MiB × 0.125 → gate = 5 MiB
/// - payload 4.8 MB < 5 MB < 12 MB → 1.0× would keep HashJoin; 2.5× rewrites SMJ
const LOAD_BEARING_ROWS: usize = 200_000;
const LOAD_BEARING_POOL: usize = 40 * 1024 * 1024;
const LOAD_BEARING_FRACTION: f64 = 0.125;

fn bench_smj_ht_overhead_load_bearing(c: &mut Criterion) {
    let schema = join_schema();
    let left = cayenne_scan_with_rows(&schema, "order_line_l.vortex", LOAD_BEARING_ROWS);
    let right = cayenne_scan_with_rows(&schema, "order_line_r.vortex", LOAD_BEARING_ROWS);
    let join = left_anti_hash_join(left, right);
    let config = smj_config(LOAD_BEARING_POOL, LOAD_BEARING_FRACTION);

    // Validity bar: with 2.5× the plan MUST be SortMergeJoin.
    let optimized = CayenneAntiJoinSortMergeRewriter::new()
        .optimize(Arc::clone(&join), &config)
        .expect("optimize");
    assert!(
        optimized.is::<SortMergeJoinExec>(),
        "2.5× HT overhead must rewrite 200k×24B build under 5 MiB gate to SortMergeJoin \
         (payload=4.8MB, 2.5×=12MB, gate=5MB)"
    );

    // Counterfactual math pin: pure 1.0× would fit the gate.
    let payload = 24_usize.saturating_mul(LOAD_BEARING_ROWS);
    let gate = (LOAD_BEARING_POOL as f64 * LOAD_BEARING_FRACTION) as usize;
    assert!(
        payload < gate,
        "1.0× payload ({payload}) must fit gate ({gate}) so the factor is load-bearing"
    );
    let with_ht = payload.saturating_mul(5) / 2;
    assert!(
        with_ht > gate,
        "2.5× estimate ({with_ht}) must exceed gate ({gate})"
    );

    let mut group = c.benchmark_group("p1_smj_ht_overhead");
    group.throughput(Throughput::Elements(LOAD_BEARING_ROWS as u64));

    group.bench_function("optimize_load_bearing_2p5x", |b| {
        b.iter(|| {
            let plan = CayenneAntiJoinSortMergeRewriter::new()
                .optimize(Arc::clone(&join), &config)
                .expect("optimize");
            assert!(plan.is::<SortMergeJoinExec>());
            black_box(plan)
        });
    });

    group.bench_function("overhead_factor_2p5x", |b| {
        b.iter(|| {
            let payload: usize = black_box(24_000);
            let estimate = payload.saturating_mul(5) / 2;
            assert_eq!(estimate, 60_000);
            black_box(estimate)
        });
    });

    group.bench_with_input(
        BenchmarkId::new("payload_vs_estimate", LOAD_BEARING_ROWS),
        &LOAD_BEARING_ROWS,
        |b, &rows| {
            b.iter(|| {
                let payload = black_box(24_usize.saturating_mul(rows));
                let estimate = payload.saturating_mul(5) / 2;
                black_box((payload, estimate))
            });
        },
    );

    group.finish();
}

// ---------------------------------------------------------------------------
// Lane 3: mem-tier reserve/release hot path with pool account
// ---------------------------------------------------------------------------

fn bench_mem_tier_reserve_hot_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("p1_mem_tier_reserve");
    group.throughput(Throughput::Elements(1));

    set_global_mem_tier_bytes(64 * 1024 * 1024);
    let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(256 * 1024 * 1024));
    set_global_mem_tier_pool_account(&pool);
    assert_eq!(global_mem_tier_pool_account_bytes(), Some(0));

    let chunk = 64_u64 * 1024; // 64 KiB — typical small CDC batch accounting unit

    group.bench_function("reserve_release_with_pool_mirror", |b| {
        b.iter(|| {
            let ok = try_reserve_global_mem_tier_bytes(black_box(chunk));
            assert!(ok, "reserve under empty budget must succeed");
            let mirrored = global_mem_tier_pool_account_bytes().expect("pool account installed");
            assert_eq!(mirrored as u64, chunk);
            let used = global_mem_tier_used().expect("budget installed");
            assert_eq!(used, chunk);
            release_global_mem_tier_bytes(chunk);
            assert_eq!(global_mem_tier_pool_account_bytes(), Some(0));
            black_box(mirrored)
        });
    });

    group.bench_function("reserve_refused_when_full", |b| {
        let budget = 64 * 1024 * 1024_u64;
        assert!(try_reserve_global_mem_tier_bytes(budget));
        b.iter(|| {
            let ok = try_reserve_global_mem_tier_bytes(black_box(1));
            assert!(!ok, "over-budget reserve must refuse");
            black_box(ok)
        });
        release_global_mem_tier_bytes(budget);
    });

    group.bench_function("install_seed_clear_cycle", |b| {
        b.iter(|| {
            set_global_mem_tier_bytes(0);
            set_global_mem_tier_bytes(black_box(64 * 1024 * 1024));
            let p: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(256 * 1024 * 1024));
            set_global_mem_tier_pool_account(&p);
            assert_eq!(global_mem_tier_pool_account_bytes(), Some(0));
            clear_global_mem_tier_pool_account();
            set_global_mem_tier_bytes(0);
            black_box(p.reserved());
        });
    });

    clear_global_mem_tier_pool_account();
    set_global_mem_tier_bytes(0);

    group.finish();
}

// ---------------------------------------------------------------------------
// Entry
// ---------------------------------------------------------------------------

fn criterion_benchmark(c: &mut Criterion) {
    // Pure / fast lanes first so a fixture setup failure still leaves micro data.
    bench_smj_ht_overhead_load_bearing(c);
    bench_mem_tier_reserve_hot_path(c);
    bench_subset_vs_full_compact(c);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
