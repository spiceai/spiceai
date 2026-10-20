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

//! File-level primary-key ordering audit of durable write paths.
#![expect(
    clippy::expect_used,
    reason = "audit fixture and exact outcome assertions"
)]
use crate::common;
use arrow::{
    array::{ArrayRef, Int32Array, Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use cayenne::metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{
    CayenneCatalog, CayenneTableProvider, MetadataCatalog, SlotAdvancer,
    experiment_set_sort_on_write, experiment_write_sort_counters,
};
use datafusion::{
    datasource::{
        MemTable, TableProvider,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        memory::MemorySourceConfig,
    },
    prelude::*,
};
use datafusion_expr::dml::InsertOp;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};
use std::{collections::BTreeMap, sync::Arc};
use tracing_subscriber::{Layer, layer::SubscriberExt};
use vortex::VortexSessionDefault;
use vortex_datafusion::VortexFormat;
use vortex_session::VortexSession;

struct Advancer;
#[async_trait::async_trait]
impl SlotAdvancer for Advancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
}
pub(super) fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("w", DataType::Int32, false),
        Field::new("d", DataType::Int32, false),
        Field::new("o", DataType::Int32, false),
        Field::new("line", DataType::Int32, false),
        Field::new("v", DataType::Int64, false),
    ]))
}
pub(super) fn keys() -> Vec<String> {
    ["w", "d", "o", "line"].map(str::to_owned).to_vec()
}
pub(super) fn batch(start: i32, n: i32) -> RecordBatch {
    // Seeded shuffle varies all PK columns, including the last.
    let mut ids: Vec<_> = (start..start + n).collect();
    let mut state = 0x91ab_5234_7789_u64;
    for i in (1..ids.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let other = usize::try_from(state % u64::try_from(i + 1).expect("small bound"))
            .expect("bounded shuffle");
        ids.swap(i, other);
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from_iter_values(
            ids.iter().map(|i| i / 100_000),
        )),
        Arc::new(Int32Array::from_iter_values(
            ids.iter().map(|i| i / 10_000 % 10),
        )),
        Arc::new(Int32Array::from_iter_values(
            ids.iter().map(|i| i / 10 % 1000),
        )),
        Arc::new(Int32Array::from_iter_values(ids.iter().map(|i| i % 10))),
        Arc::new(Int64Array::from_iter_values(
            ids.iter().map(|&i| i64::from(i)),
        )),
    ];
    RecordBatch::try_new(schema(), columns).expect("audit input batch")
}
pub(super) fn config() -> VortexConfig {
    VortexConfig {
        inline_max_rows: 0,
        inline_max_bytes: 0,
        inline_max_buffer_bytes: 0,
        write_concurrency: Some(4),
        target_vortex_file_size_mb: 64,
        compaction_background_interval_ms: 0,
        compaction_trigger_files: usize::MAX,
        compaction_trigger_protected_snapshots: usize::MAX,
        compaction_trigger_snapshot_age_ms: 0,
        deletion_mode: DeletionMode::Key,
        cdc_durability: CdcDurability::File,
        ..VortexConfig::default()
    }
}
pub(super) async fn table(
    f: &common::TestFixture,
    config: VortexConfig,
) -> (SessionContext, Arc<CayenneTableProvider>) {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
    let t = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&f.catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: "audit".into(),
                schema: schema(),
                primary_key: keys(),
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(keys()))),
                base_path: f.data_path.to_string_lossy().into_owned(),
                partition_column: None,
                vortex_config: config,
            },
            ctx.runtime_env(),
        )
        .await
        .expect("create audit table"),
    );
    ctx.register_table("t", Arc::clone(&t) as Arc<dyn TableProvider>)
        .expect("register audit table");
    (ctx, t)
}
async fn live_files(
    t: &CayenneTableProvider,
    catalog: &CayenneCatalog,
) -> BTreeMap<String, String> {
    let metadata = catalog
        .get_table(&t.metadata().table_name)
        .await
        .expect("current table metadata");
    let mut snapshots = catalog
        .get_all_snapshot_sequences(t.table_id())
        .await
        .expect("protected snapshot manifest");
    snapshots.insert(metadata.current_snapshot_id, 0);
    let mut files = BTreeMap::new();
    for id in snapshots.keys() {
        let directory = std::path::Path::new(&t.metadata().path)
            .join(t.table_id())
            .join(id);
        let physical_path = |name: &str| {
            directory
                .join(name)
                .canonicalize()
                .expect("live manifest file exists")
                .to_string_lossy()
                .into_owned()
        };
        let manifest = catalog
            .get_snapshot_files(t.table_id(), id)
            .await
            .expect("warm file manifest");
        if manifest.is_empty() {
            // A just-published append can precede its asynchronous manifest fill.
            for (path, _) in t
                .list_snapshot_files_with_sizes(id)
                .await
                .expect("live snapshot listing")
            {
                files.insert(physical_path(&path), id.clone());
            }
        } else {
            for file in manifest {
                files.insert(physical_path(&file.file_path), id.clone());
            }
        }
    }
    for file in catalog
        .list_cold_tier_files(t.table_id())
        .await
        .expect("cold file manifest")
    {
        files.insert(file.file_url, "cold".to_owned());
    }
    files
}
pub(super) async fn report(
    t: &Arc<CayenneTableProvider>,
    catalog: &CayenneCatalog,
    label: &str,
    enabled: bool,
    run_start: u64,
) -> usize {
    report_files(t, catalog, label, enabled, run_start, enabled).await
}

pub(super) async fn report_files(
    t: &Arc<CayenneTableProvider>,
    catalog: &CayenneCatalog,
    label: &str,
    enabled: bool,
    run_start: u64,
    require_sorted: bool,
) -> usize {
    t.drain_in_flight_maintenance()
        .await
        .expect("settle audit writes");
    println!(
        "LAYOUT write_path={label} rewrite_sort={:?}",
        t.effective_sort_columns_for_rewrite()
    );
    let files = live_files(t, catalog).await;
    assert!(
        !files.is_empty(),
        "write path must produce live files: {label}"
    );
    // Request physical scan order without adding a SortExec. Vortex otherwise
    // emits completed chunks out of order, which is not a file-order audit.
    let reader = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let mut unsorted = 0;
    let mut total_rows = 0;
    for path in files.keys() {
        let listing = ListingTable::try_new(
            ListingTableConfig::new(ListingTableUrl::parse(path).expect("file URL"))
                .with_listing_options(
                    ListingOptions::new(Arc::new(VortexFormat::new(VortexSession::default())))
                        .with_file_sort_order(vec![
                            keys().iter().map(|key| col(key).sort(true, true)).collect(),
                        ]),
                )
                .with_schema(schema()),
        )
        .expect("direct file reader");
        let file_plan = reader
            .read_table(Arc::new(listing))
            .expect("read file")
            .create_physical_plan()
            .await
            .expect("ordered physical reader");
        let reader_plan = format!(
            "{}",
            datafusion::physical_plan::displayable(file_plan.as_ref()).indent(true)
        );
        assert!(
            file_plan.properties().output_ordering().is_some(),
            "physical reader must request ordered chunks: {reader_plan}"
        );
        assert!(
            !reader_plan.contains("SortExec:"),
            "audit must not sort file rows: {reader_plan}"
        );
        let output = datafusion::physical_plan::collect(file_plan, reader.task_ctx())
            .await
            .expect("file rows");
        let mut previous = None;
        let mut rows = 0;
        let mut runs = 1;
        for b in output {
            let columns: Vec<_> = (0..4)
                .map(|c| {
                    b.column(c)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("Int32 PK")
                })
                .collect();
            for row in 0..b.num_rows() {
                let key: [i32; 4] = std::array::from_fn(|c| columns[c].value(row));
                runs += usize::from(previous.is_some_and(|p| p > key));
                previous = Some(key);
                rows += 1;
            }
        }
        println!(
            "AUDIT mode={} write_path={label} rows={rows} sorted_runs={runs} path={path}",
            usize::from(enabled)
        );
        total_rows += rows;
        assert!(rows > 0, "live data file is nonempty: {label} {path}");
        if require_sorted {
            let counters = experiment_write_sort_counters();
            if label.starts_with("round_robin") && counters[4] >= 128 * 1024 * 1024 {
                // Files can cross independently sorted run boundaries when
                // the input byte budget seals the current run.
                assert!(
                    runs <= usize::try_from(counters[3] - run_start).expect("run count"),
                    "ON file exceeds sorted-run seals: {label} {path}, runs={runs}, counters={counters:?}"
                );
            } else {
                assert_eq!(
                    runs, 1,
                    "ON file must be PK sorted: {label} {path}, counters={counters:?}"
                );
            }
        }
        unsorted += usize::from(runs > 1);
    }
    let expected_rows = match label {
        "single_writer" | "inline_checkpoint" => Some(512),
        "multi_shard" => Some(250_000),
        "round_robin_first" => Some(750_000),
        "round_robin_first_later" => Some(3_000_000),
        "overwrite" => Some(4096),
        "compaction" => Some(6144),
        "cold" => Some(1024),
        _ => None,
    };
    if let Some(expected) = expected_rows {
        assert_eq!(
            total_rows, expected,
            "physical row preservation: {label} mode={enabled}"
        );
    }
    unsorted
}
pub(super) async fn insert(t: &CayenneTableProvider, b: RecordBatch) {
    let rows = b.num_rows() as u64;
    assert_eq!(
        common::insert_batch(t, b).await.expect("audit append"),
        rows
    );
}
async fn cdc_insert(t: &CayenneTableProvider, ctx: &SessionContext, batch: RecordBatch) {
    t.write_cdc_append_stream(common::single_batch_stream(batch), &ctx.task_ctx())
        .await
        .expect("CDC apply")
        .finish()
        .await
        .expect("CDC finalize");
}
pub(super) async fn sql(ctx: &SessionContext, statement: &str) {
    ctx.sql(statement)
        .await
        .expect("audit SQL")
        .collect()
        .await
        .expect("execute audit SQL");
}
async fn scenario(enabled: bool, kind: usize) -> usize {
    let f = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("audit fixture");
    let mut cfg = config();
    if kind == 0 {
        cfg.write_concurrency = Some(1);
    }
    if kind == 4 {
        cfg.compaction_trigger_files = 2;
        cfg.compaction_trigger_protected_snapshots = 2;
    }
    if kind == 2 {
        cfg.compaction_trigger_files = 8;
        cfg.compaction_trigger_protected_snapshots = 8;
        cfg.compaction_background_interval_ms = 30_000;
        cfg.compaction_trigger_snapshot_age_ms = 300_000;
    }
    if kind == 6 || kind == 10 {
        cfg.cdc_durability = CdcDurability::Memory;
        cfg.cdc_mem_tier_shards = if kind == 10 { 4 } else { 1 };
        cfg.cdc_mem_tier_checkpoint_interval_ms = 0;
        cfg.cdc_mem_tier_max_age_ms = 0;
        cfg.cdc_mem_tier_max_bytes = 0;
    }
    if kind == 7 {
        cfg.inline_max_rows = 10000;
        cfg.inline_max_bytes = 1024 * 1024;
        cfg.inline_max_buffer_bytes = 1024 * 1024;
        cfg.inline_flush_max_rows = 100_000;
    }
    if kind == 8 {
        let cold = f.temp_dir.path().join("cold");
        std::fs::create_dir_all(&cold).expect("cold dir");
        cfg.cold_tier_location = Some(format!("file://{}", cold.display()));
        cfg.cold_tier_warm_max_files = 1;
        cfg.cold_tier_background_interval_ms = 0;
    }
    let (ctx, t) = table(&f, cfg).await;
    let label = match kind {
        0 => "single_writer",
        1 => "multi_shard",
        2 => "round_robin_after_bursts",
        3 => "upsert_bursts_deletes",
        4 => "compaction",
        5 => "overwrite",
        6 => "mem_checkpoint_bake",
        10 => "mem_sharded_checkpoint_bake",
        7 => "inline_checkpoint",
        8 => "cold",
        _ => "file_staged_appends",
    };
    let run_start = experiment_write_sort_counters()[3];
    let mut unsorted = 0;
    match kind {
        0 => insert(&t, batch(0, 512)).await,
        1 => insert(&t, batch(0, 250_000)).await,
        2 => {
            // Four independent SQL appends through MemTable, matching the CDC load.
            let shuffled = batch(0, 3_000_000);
            for part in 0..4 {
                let input = shuffled.slice(part * 750_000, 750_000);
                ctx.register_table(
                    "input",
                    Arc::new(
                        MemTable::try_new(schema(), vec![vec![input]]).expect("load memtable"),
                    ),
                )
                .expect("register load");
                sql(&ctx, "INSERT INTO t SELECT * FROM input").await;
                ctx.deregister_table("input").expect("drop load");
                unsorted += report(
                    &t,
                    &f.catalog,
                    if part == 0 {
                        "round_robin_first"
                    } else {
                        "round_robin_later"
                    },
                    enabled,
                    run_start,
                )
                .await;
            }
            unsorted += report(
                &t,
                &f.catalog,
                "round_robin_first_later",
                enabled,
                run_start,
            )
            .await;
            let base_files = live_files(&t, &f.catalog).await;
            // Drain maintenance after each small CDC write, as in the end-to-end benchmark.
            for burst in 0..1000 {
                let updates = arrow::compute::concat_batches(
                    &schema(),
                    &[
                        batch((burst * 7919) % 2_999_900, 100),
                        batch(3_000_000 + burst * 100, 100),
                    ],
                )
                .expect("old and new burst keys");
                insert(&t, updates).await;
                t.flush_pending_maintenance()
                    .await
                    .expect("burst maintenance");
                t.run_position_capture()
                    .await
                    .expect("burst position capture");
            }
            let final_files = live_files(&t, &f.catalog).await;
            let rewritten = base_files
                .keys()
                .filter(|path| !final_files.contains_key(*path))
                .count();
            println!(
                "BURST_REWRITE mode={} base_files={} retired_base_files={rewritten}",
                usize::from(enabled),
                base_files.len()
            );
            assert!(
                rewritten > 0,
                "mode={enabled}: maintenance must rewrite base files"
            );
        }
        9 => {
            for burst in 0..3 {
                cdc_insert(&t, &ctx, batch(burst * 256, 512)).await;
            }
        }
        3 => {
            insert(&t, batch(0, 2048)).await;
            for burst in 0..12 {
                insert(&t, batch((burst * 317) % 1700, 128)).await;
            }
            sql(&ctx, "DELETE FROM t WHERE w=0 AND d=0 AND o=0 AND line=0").await;
            assert_eq!(
                t.delete_from_cdc_fast(&[
                    col("w").eq(lit(0_i32)),
                    col("d").eq(lit(0_i32)),
                    col("o").eq(lit(204_i32)),
                    col("line").eq(lit(1_i32))
                ])
                .await
                .expect("key delete"),
                Some(1)
            );
        }
        4 => {
            t.install_slot_advancer(Arc::new(Advancer));
            for part in 0..12 {
                cdc_insert(&t, &ctx, batch(part * 512, 512)).await;
            }
            t.flush_pending_maintenance()
                .await
                .expect("flush compaction");
            let mut compacted = t.maybe_compact_small_files().await.expect("force compact");
            if !compacted {
                compacted = t
                    .compact_protected_snapshots_subset(usize::MAX)
                    .await
                    .expect("force protected merge");
            }
            t.drain_in_flight_maintenance()
                .await
                .expect("settle compaction");
            let live = live_files(&t, &f.catalog).await;
            assert!(
                compacted || live.len() < 12,
                "twelve immutable writes must have been consolidated: {live:?}"
            );
            println!("COMPACTION live_files={} immutable_writes=12", live.len());
            println!(
                "COMPACTION explicit={compacted} path={:?}",
                t.last_small_file_compact_path()
            );
        }
        5 => {
            insert(&t, batch(0, 512)).await;
            let input =
                MemorySourceConfig::try_new_exec(&[vec![batch(10000, 4096)]], schema(), None)
                    .expect("overwrite input");
            let plan = t
                .insert_into(&ctx.state(), input, InsertOp::Overwrite)
                .await
                .expect("overwrite plan");
            datafusion::physical_plan::collect(plan, ctx.task_ctx())
                .await
                .expect("overwrite output");
        }
        6 | 10 => {
            t.install_slot_advancer(Arc::new(Advancer));
            for burst in 0..6 {
                cdc_insert(&t, &ctx, batch(burst * 256, 512)).await;
                assert!(t.checkpoint_mem_tier().await.expect("checkpoint burst") > 0);
            }
            cdc_insert(&t, &ctx, batch(2000, 512)).await;
            assert!(
                t.checkpoint_mem_tier().await.expect("mem checkpoint") > 0,
                "mem path checkpointed real rows"
            );
            unsorted += report(&t, &f.catalog, "mem_checkpoint", enabled, run_start).await;
            let before_bake = live_files(&t, &f.catalog).await;
            let baked = t
                .bake_seq_prefix_protected_snapshots()
                .await
                .expect("seq prefix bake");
            assert!(baked, "seq-prefix bake must produce files");
            let after_bake = live_files(&t, &f.catalog).await;
            let new_files = after_bake
                .keys()
                .filter(|path| !before_bake.contains_key(*path))
                .count();
            assert!(
                new_files > 0,
                "mode={enabled}: audit must include new bake output"
            );
            println!("BAKE changed={baked} new_files={new_files}");
        }
        7 => {
            insert(&t, batch(0, 512)).await;
            assert_eq!(
                t.checkpoint_inlined_data()
                    .await
                    .expect("inline checkpoint"),
                512
            );
        }
        _ => {
            insert(&t, batch(0, 512)).await;
            insert(&t, batch(512, 512)).await;
            assert!(
                t.promote_warm_to_cold().await.expect("cold promotion"),
                "cold promotion must run"
            );
        }
    }
    t.flush_pending_maintenance()
        .await
        .expect("flush audit maintenance");
    println!(
        "WRITE_SORT_STATE write_path={label} counters={:?}",
        experiment_write_sort_counters()
    );
    unsorted + report(&t, &f.catalog, label, enabled, run_start).await
}
struct Restore;
impl Drop for Restore {
    fn drop(&mut self) {
        experiment_set_sort_on_write(true);
    }
}
#[test]
fn write_path_sortedness_audit() {
    if !std::env::args().any(|arg| {
        arg == "write_path_sortedness_audit"
            || arg == "write_path_sortedness_audit::write_path_sortedness_audit"
    }) {
        common::require_process_per_test("audit changes process-global writer experiment");
    }
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .with_test_writer()
            .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                metadata.target() == "cayenne::compaction"
                    || *metadata.level() <= tracing::Level::INFO
            })),
    );
    tracing::subscriber::set_global_default(subscriber).expect("audit tracing subscriber");
    let _restore = Restore;
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("audit runtime")
        .block_on(async {
            for enabled in [false, true] {
                experiment_set_sort_on_write(enabled);
                let before = experiment_write_sort_counters();
                println!(
                    "AUDIT_TABLE mode={} path rows sorted_runs file",
                    usize::from(enabled)
                );
                let mut unsorted = 0;
                for kind in [6, 10, 7, 8, 0, 1, 2, 3, 4, 5, 9] {
                    unsorted += scenario(enabled, kind).await;
                }
                println!(
                    "WRITE_SORT_COUNTERS mode={} before={before:?} after={:?}",
                    usize::from(enabled),
                    experiment_write_sort_counters()
                );
                if !enabled {
                    assert!(unsorted > 0, "OFF must expose unsorted files");
                }
            }
        });
}
