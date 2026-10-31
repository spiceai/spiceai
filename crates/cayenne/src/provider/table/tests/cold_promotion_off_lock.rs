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

//! A cold promotion of a table that holds no deletion state encodes without holding
//! `write_lock`, and its commit reconciles what it read with every write that landed
//! meanwhile: appends stay in the warm tier, and anything else discards the output.
//!
//! Each test pauses a promotion right after its capture, applies writes, resumes it,
//! and compares the table with an in-memory model of the operation history.

use super::*;
use std::collections::BTreeMap;
use std::time::Duration;

use arrow::array::TimestampNanosecondArray;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion_table_providers::util::column_reference::ColumnReference;

/// How long a write may take while a promotion is paused. A write held behind the
/// promotion's `write_lock` never finishes, so this only bounds the failure.
const WRITE_TIMEOUT: Duration = Duration::from_secs(60);

fn id_value_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// Primary key `id`, the cold tier on the local file system and triggered by any
/// warm file. `clean_append` skips conflict detection for fixtures with unique keys.
async fn cold_table(
    name: &str,
    inline: bool,
    clean_append: bool,
    ctx: &SessionContext,
) -> (CayenneTableProvider, tempfile::TempDir, Arc<CayenneCatalog>) {
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let metadata_dir = temp_dir.path().join("metadata");
    std::fs::create_dir_all(&metadata_dir).expect("metadata dir created");
    let cold_dir = temp_dir.path().join("cold");
    std::fs::create_dir_all(&cold_dir).expect("cold dir created");
    let catalog = Arc::new(
        CayenneCatalog::new(format!("sqlite://{}/cayenne.db", metadata_dir.display()))
            .expect("catalog created"),
    );
    catalog.init().await.expect("catalog initialized");
    let inline_rows = if inline { 1_000 } else { 0 };
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema: id_value_schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: (!clean_append)
            .then(|| OnConflict::Upsert(ColumnReference::new(vec!["id".to_string()]))),
        base_path: temp_dir.path().join("data").to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig {
            cold_tier_location: Some(format!("file://{}", cold_dir.display())),
            cluster_by: vec!["id".to_string()],
            cold_tier_warm_max_files: 1,
            cold_target_file_size_mb: 16,
            deletion_mode: crate::metadata::DeletionMode::Key,
            pk_conflict_detection: if clean_append {
                crate::metadata::PkConflictDetection::None
            } else {
                crate::metadata::PkConflictDetection::Auto
            },
            inline_max_rows: inline_rows,
            inline_max_bytes: if inline { 1 << 20 } else { 0 },
            inline_max_buffer_bytes: if inline { 1 << 22 } else { 0 },
            compaction_background_interval_ms: 3_600_000,
            ..VortexConfig::default()
        },
    };
    let catalog_for_provider = Arc::clone(&catalog);
    let catalog_for_provider: Arc<dyn MetadataCatalog> = catalog_for_provider;
    let provider = CayenneTableProviderBuilder::new(catalog_for_provider, ctx.runtime_env())
        .create(options)
        .await
        .expect("table created");
    (provider, temp_dir, catalog)
}

fn rows_batch(rows: &[(i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        id_value_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|(id, _)| *id))),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|(_, value)| *value),
            )),
        ],
    )
    .expect("batch")
}

/// The table as a scan returns it: every `(id, value)` row, sorted. A key read twice
/// shows up twice, so a duplicate fails a comparison with the model.
async fn scan_rows(provider: &CayenneTableProvider) -> Vec<(i64, i64)> {
    let ctx = SessionContext::new();
    let plan = provider
        .scan(&ctx.state(), Some(&vec![0, 1]), &[], None)
        .await
        .expect("scan plan");
    collect_rows(plan, &ctx).await
}

async fn scan_ids(provider: &CayenneTableProvider) -> Vec<i64> {
    let ctx = SessionContext::new();
    let plan = provider
        .scan(&ctx.state(), Some(&vec![0]), &[], None)
        .await
        .expect("scan plan");
    let batches = collect(plan, ctx.task_ctx()).await.expect("scan rows");
    let mut ids = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column")
                .values()
                .iter()
                .copied()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

async fn collect_rows(plan: Arc<dyn ExecutionPlan>, ctx: &SessionContext) -> Vec<(i64, i64)> {
    let batches = collect(plan, ctx.task_ctx()).await.expect("collect rows");
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value column");
        rows.extend(
            ids.values()
                .iter()
                .copied()
                .zip(values.values().iter().copied()),
        );
    }
    rows.sort_unstable();
    rows
}

/// The table as it reads after a restart.
async fn reopened_rows(provider: &CayenneTableProvider, name: &str) -> Vec<(i64, i64)> {
    let ctx = SessionContext::new();
    let reopened =
        CayenneTableProviderBuilder::new(Arc::clone(provider.catalog()), ctx.runtime_env())
            .open(name)
            .await
            .expect("reopen the table from its catalog");
    scan_rows(&reopened).await
}

async fn delete_id(provider: &CayenneTableProvider, id: i64) {
    let ctx = SessionContext::new();
    let plan = provider
        .delete_from(
            &ctx.state(),
            vec![datafusion_expr::col("id").eq(datafusion_expr::lit(id))],
        )
        .await
        .expect("delete plan");
    collect(plan, ctx.task_ctx()).await.expect("delete");
}

/// What the oracle holds: the rows the table must return.
#[derive(Default, Clone)]
struct Model(BTreeMap<i64, i64>);

impl Model {
    fn rows(&self) -> Vec<(i64, i64)> {
        self.0.iter().map(|(id, value)| (*id, *value)).collect()
    }
}

/// One write to the table and the model alike.
#[derive(Clone, Copy, Debug)]
enum Op {
    /// Rows `start..start + n` that the table does not hold.
    Append { start: i64, n: i64 },
    /// An upsert of keys the table holds, with a new value.
    Upsert(i64),
    /// A delete of a key the table holds.
    Delete(i64),
}

impl Op {
    /// Whether the write is an append, which a promotion carries; every other write
    /// changes rows the promotion read.
    fn is_append(self) -> bool {
        matches!(self, Self::Append { .. })
    }

    async fn apply(self, provider: &CayenneTableProvider, model: &mut Model, tag: i64) {
        match self {
            Self::Append { start, n } => {
                let rows: Vec<(i64, i64)> = (start..start + n).map(|id| (id, id * 2)).collect();
                for (id, value) in &rows {
                    assert!(
                        model.0.insert(*id, *value).is_none(),
                        "fixture: append of key {id} the table already holds"
                    );
                }
                tokio::time::timeout(WRITE_TIMEOUT, insert_batch(provider, rows_batch(&rows)))
                    .await
                    .expect("an append completes while the promotion is paused");
            }
            Self::Upsert(id) => {
                let value = -tag;
                assert!(
                    model.0.insert(id, value).is_some(),
                    "fixture: upsert of key {id} the table does not hold"
                );
                tokio::time::timeout(
                    WRITE_TIMEOUT,
                    insert_batch(provider, rows_batch(&[(id, value)])),
                )
                .await
                .expect("an upsert completes while the promotion is paused");
            }
            Self::Delete(id) => {
                assert!(
                    model.0.remove(&id).is_some(),
                    "fixture: delete of key {id} the table does not hold"
                );
                tokio::time::timeout(WRITE_TIMEOUT, delete_id(provider, id))
                    .await
                    .expect("a delete completes while the promotion is paused");
            }
        }
    }
}

/// A promotion parked right after it captured its input.
struct ParkedPromotion {
    release: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<bool>>,
}

/// Start a promotion and wait until it has captured and released `write_lock`.
async fn park_promotion(provider: &CayenneTableProvider) -> ParkedPromotion {
    let (captured_tx, captured_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    *provider.test_cold_promotion_post_capture_hook.lock() = Some(Box::new(move || {
        Box::pin(async move {
            let _ = captured_tx.send(());
            let _ = release_rx.await;
        })
    }));
    let promoter = provider.clone_for_write_operations();
    let task = tokio::spawn(async move { promoter.promote_warm_to_cold().await });
    tokio::time::timeout(WRITE_TIMEOUT, captured_rx)
        .await
        .expect("the promotion reaches its capture")
        .expect("the promotion signals its capture");
    ParkedPromotion {
        release: release_tx,
        task,
    }
}

impl ParkedPromotion {
    async fn resume(self) -> bool {
        self.release.send(()).expect("the promotion is parked");
        self.task
            .await
            .expect("the promotion task finishes")
            .expect("the promotion returns")
    }
}

async fn cold_manifest_rows(provider: &CayenneTableProvider) -> i64 {
    provider
        .catalog()
        .list_cold_tier_files(provider.table_id())
        .await
        .expect("cold files")
        .iter()
        .map(|file| file.row_count)
        .sum()
}

async fn cold_manifest_urls(provider: &CayenneTableProvider) -> Vec<String> {
    let mut urls: Vec<String> = provider
        .catalog()
        .list_cold_tier_files(provider.table_id())
        .await
        .expect("cold files")
        .into_iter()
        .map(|file| file.file_url)
        .collect();
    urls.sort();
    urls
}

/// The rows the warm tier holds: the table minus the cold manifest's rows.
async fn warm_files(provider: &CayenneTableProvider) -> usize {
    provider
        .list_snapshot_files_with_sizes(&provider.get_current_snapshot_id())
        .await
        .expect("current snapshot files")
        .len()
}

async fn flush(provider: &CayenneTableProvider) {
    provider
        .checkpoint_inlined_data()
        .await
        .expect("inline rows flushed");
}

/// Rows `ids` appended to both the table and the model.
async fn load(provider: &CayenneTableProvider, model: &mut Model, start: i64, n: i64) {
    Op::Append { start, n }.apply(provider, model, 0).await;
}

/// A retention delete can unlink a file after the first carry copied it into the
/// prospective warm snapshot. The second carry and commit must reject that snapshot.
#[tokio::test]
async fn retention_of_a_carried_file_discards_cold_promotion() {
    const NAME: &str = "off_lock_retention_of_carried_file";
    let ctx = SessionContext::new();
    let temp_dir = tempfile::tempdir().expect("temp dir created");
    let metadata_dir = temp_dir.path().join("metadata");
    let cold_dir = temp_dir.path().join("cold");
    std::fs::create_dir_all(&metadata_dir).expect("metadata dir created");
    std::fs::create_dir_all(&cold_dir).expect("cold dir created");
    let catalog = Arc::new(
        CayenneCatalog::new(format!("sqlite://{}/cayenne.db", metadata_dir.display()))
            .expect("catalog created"),
    ) as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("catalog initialized");
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]));
    let provider = CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
        .create(CreateTableOptions {
            table_name: NAME.to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec!["id".to_string()],
            on_conflict: None,
            base_path: temp_dir.path().join("data").to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: VortexConfig {
                cold_tier_location: Some(format!("file://{}", cold_dir.display())),
                cold_tier_warm_max_files: 1,
                cold_target_file_size_mb: 16,
                deletion_mode: crate::metadata::DeletionMode::Key,
                pk_conflict_detection: crate::metadata::PkConflictDetection::None,
                inline_max_rows: 0,
                inline_max_bytes: 0,
                inline_max_buffer_bytes: 0,
                compaction_background_interval_ms: 3_600_000,
                ..VortexConfig::default()
            },
        })
        .await
        .expect("retention table created");

    let batch = |start: i64, time: i64| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from((start..start + 50).collect::<Vec<_>>())),
                Arc::new(TimestampNanosecondArray::from(vec![time; 50])),
            ],
        )
        .expect("batch")
    };
    insert_batch(&provider, batch(0, 10)).await;
    let captured_snapshot = provider.get_current_snapshot_id();
    assert_eq!(warm_files(&provider).await, 1, "fixture: one captured file");
    let parked = park_promotion(&provider).await;
    insert_batch(&provider, batch(50, 1)).await;
    assert_eq!(
        provider.get_current_snapshot_id(),
        captured_snapshot,
        "fixture: the append stays in the captured current snapshot"
    );
    assert_eq!(
        warm_files(&provider).await,
        2,
        "fixture: the first carry has an appended file to link"
    );

    let racer = provider.clone_for_write_operations();
    let racer_ctx = ctx.clone();
    *provider.test_cold_promotion_post_carry_hook.lock() = Some(Box::new(move || {
        Box::pin(async move {
            // Call the physical retention path directly so these disjoint-key
            // appends stay in the current snapshot until the race is staged.
            let plan =
                racer
                    .delete_using_files(&[datafusion_expr::col("event_time").lt(
                        datafusion_expr::lit(ScalarValue::TimestampNanosecond(Some(2), None)),
                    )])
                    .expect("retention delete plan");
            let output = collect(plan, racer_ctx.task_ctx())
                .await
                .expect("retention delete executed");
            let deleted = output[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .expect("delete count is UInt64")
                .value(0);
            assert_eq!(deleted, 50, "the carried file was physically removed");
            assert!(
                !racer.has_rows_hidden_from_file_statistics(),
                "retention unlinked the file rather than hiding its rows"
            );
        })
    }));

    assert!(
        !parked.resume().await,
        "the promotion must discard the copied file after retention removes its source"
    );
    assert!(
        provider
            .test_cold_promotion_post_carry_hook
            .lock()
            .is_none()
    );
    let expected: Vec<i64> = (0..50).collect();
    assert_eq!(
        scan_ids(&provider).await,
        expected,
        "expired rows stay deleted"
    );
    let reopened =
        CayenneTableProviderBuilder::new(Arc::clone(provider.catalog()), ctx.runtime_env())
            .open(NAME)
            .await
            .expect("reopen table");
    assert_eq!(
        scan_ids(&reopened).await,
        expected,
        "deletion survives reopen"
    );
}

/// A transaction may commit and still report a transport error. Read back the
/// snapshot pointer before discarding files that the committed catalog now owns.
#[tokio::test]
async fn committed_cold_promotion_with_failed_response_keeps_its_files() {
    const NAME: &str = "off_lock_committed_but_reported_failed";
    let ctx = SessionContext::new();
    let (provider, _temp_dir, catalog) = cold_table(NAME, false, true, &ctx).await;
    let mut model = Model::default();
    load(&provider, &mut model, 0, 100).await;
    flush(&provider).await;

    let parked = park_promotion(&provider).await;
    load(&provider, &mut model, 100, 50).await;
    assert_eq!(
        warm_files(&provider).await,
        2,
        "the append is ready to carry"
    );
    catalog.report_next_cold_promotion_commit_as_failed();
    assert!(
        parked.resume().await,
        "the catalog commit succeeded despite its reported failure"
    );
    assert!(
        !catalog.cold_promotion_commit_failure_pending(),
        "the off-lock catalog commit consumed the injected failure"
    );
    assert_eq!(
        cold_manifest_rows(&provider).await,
        100,
        "the committed cold files contain all source rows"
    );
    assert_eq!(
        warm_files(&provider).await,
        1,
        "the appended file was carried"
    );
    assert_eq!(scan_rows(&provider).await, model.rows());
    assert_eq!(reopened_rows(&provider, NAME).await, model.rows());
}

/// Appends that land while a promotion encodes finish while it is paused, and stay in
/// the warm tier when it commits: the cold files hold exactly the rows it read, every
/// row stays visible exactly once, a scan planned before the commit still reads the
/// state it captured, and the table holds the same rows after a reopen. Run with the
/// inline memtable off (each append is a file) and on (each is an inline row the
/// commit flushes).
#[rstest::rstest]
#[case::files("off_lock_appends_files", false)]
#[case::inline("off_lock_appends_inline", true)]
#[tokio::test]
async fn appends_during_an_off_lock_promotion_stay_warm(#[case] name: &str, #[case] inline: bool) {
    let ctx = SessionContext::new();
    let (provider, _temp_dir, _catalog) = cold_table(name, inline, false, &ctx).await;
    let mut model = Model::default();
    load(&provider, &mut model, 0, 100).await;
    flush(&provider).await;
    assert!(
        provider
            .promote_warm_to_cold()
            .await
            .expect("first promotion"),
        "fixture: the first promotion commits"
    );
    load(&provider, &mut model, 100, 100).await;
    flush(&provider).await;
    let promoted = model.rows();
    let cold_before = cold_manifest_urls(&provider).await;

    let parked = park_promotion(&provider).await;
    // Both land while the promotion is parked after its capture; each would wait out
    // the whole promotion if it held `write_lock`.
    load(&provider, &mut model, 1_000, 40).await;
    load(&provider, &mut model, 2_000, 7).await;
    assert_eq!(
        scan_rows(&provider).await,
        model.rows(),
        "the appends are visible while the promotion is paused"
    );
    let during = model.rows();
    let ctx_during = SessionContext::new();
    let plan_during = provider
        .scan(&ctx_during.state(), Some(&vec![0, 1]), &[], None)
        .await
        .expect("a scan planned before the commit");

    assert!(
        parked.resume().await,
        "a promotion that met only appends commits"
    );

    assert_eq!(
        scan_rows(&provider).await,
        model.rows(),
        "after the commit every row is visible exactly once"
    );
    assert_eq!(
        collect_rows(plan_during, &ctx_during).await,
        during,
        "a scan planned before the commit reads the state it captured"
    );
    assert_ne!(
        cold_manifest_urls(&provider).await,
        cold_before,
        "the commit registered new cold files"
    );
    assert_eq!(
        cold_manifest_rows(&provider).await,
        i64::try_from(promoted.len()).expect("fits"),
        "the cold files hold exactly the rows the promotion read, not the appends"
    );
    assert!(
        warm_files(&provider).await >= 1 || inline,
        "the appends stay in the warm tier"
    );
    assert_eq!(
        reopened_rows(&provider, name).await,
        model.rows(),
        "after a reopen the table holds the same rows"
    );
    let stats = provider
        .catalog()
        .get_table_statistics(provider.table_id())
        .await
        .expect("statistics read");
    if let Some(stats) = stats
        && stats.num_rows_exact
    {
        assert_eq!(
            stats.num_rows,
            i64::try_from(model.0.len()).expect("fits"),
            "an exact maintained row count counts the carried appends"
        );
    }
    assert!(
        provider
            .promote_warm_to_cold()
            .await
            .expect("next promotion"),
        "the carried appends promote on the next pass"
    );
    assert_eq!(scan_rows(&provider).await, model.rows());
    assert_eq!(
        cold_manifest_rows(&provider).await,
        i64::try_from(model.0.len()).expect("fits"),
        "the next promotion moves the carried appends to the cold files"
    );
}

/// A delete or an upsert of a key the promotion read, landing while it encodes, is not
/// a change the commit can fold in: the promotion discards its output and leaves the
/// warm tier and the cold manifest as they were. The delete and the upsert both
/// survive, nothing is duplicated or resurrected, and the next promotion, which holds
/// writers off, commits and applies them.
#[rstest::rstest]
#[case::delete_of_a_warm_key("off_lock_delete_warm", false, Op::Delete(150))]
#[case::delete_of_a_cold_key("off_lock_delete_cold", false, Op::Delete(5))]
#[case::upsert_of_a_warm_key("off_lock_upsert_warm", false, Op::Upsert(150))]
#[case::upsert_of_a_cold_key("off_lock_upsert_cold", false, Op::Upsert(5))]
#[case::delete_inline("off_lock_delete_inline", true, Op::Delete(150))]
#[case::upsert_inline("off_lock_upsert_inline", true, Op::Upsert(150))]
#[tokio::test]
async fn a_delete_or_upsert_during_an_off_lock_promotion_discards_it(
    #[case] name: &str,
    #[case] inline: bool,
    #[case] op: Op,
) {
    let ctx = SessionContext::new();
    let (provider, _temp_dir, _catalog) = cold_table(name, inline, false, &ctx).await;
    let mut model = Model::default();
    load(&provider, &mut model, 0, 100).await;
    flush(&provider).await;
    assert!(
        provider
            .promote_warm_to_cold()
            .await
            .expect("first promotion")
    );
    load(&provider, &mut model, 100, 100).await;
    flush(&provider).await;
    let cold_before = cold_manifest_urls(&provider).await;
    let warm_before = warm_files(&provider).await;

    let parked = park_promotion(&provider).await;
    load(&provider, &mut model, 1_000, 10).await;
    op.apply(&provider, &mut model, 7).await;
    assert_eq!(
        scan_rows(&provider).await,
        model.rows(),
        "the write is visible while the promotion is paused"
    );

    assert!(
        !parked.resume().await,
        "a promotion that met a {op:?} discards its output"
    );
    assert_eq!(
        scan_rows(&provider).await,
        model.rows(),
        "a discarded promotion changes nothing"
    );
    assert_eq!(
        cold_manifest_urls(&provider).await,
        cold_before,
        "the cold manifest is as it was"
    );
    assert!(
        warm_files(&provider).await >= warm_before,
        "the warm tier is intact"
    );
    assert!(
        provider.cold_promotion_locked_next.load(Ordering::Acquire),
        "the next promotion holds writers off"
    );

    assert!(
        provider
            .promote_warm_to_cold()
            .await
            .expect("next promotion"),
        "the promotion that holds writers off commits"
    );
    assert!(
        !provider.cold_promotion_locked_next.load(Ordering::Acquire),
        "and clears the request"
    );
    assert_eq!(
        scan_rows(&provider).await,
        model.rows(),
        "the delete or upsert holds after the table moved to the cold files"
    );
    assert_eq!(
        cold_manifest_rows(&provider).await,
        i64::try_from(model.0.len()).expect("fits"),
        "the cold files hold exactly the live rows"
    );
    assert_eq!(reopened_rows(&provider, name).await, model.rows());
}

/// A seeded differential run: each round loads a table, parks a promotion, applies a
/// random mix of appends, upserts of keys the table holds (promoted or not) and deletes
/// of them, resumes, and compares the table with the model — before the commit, after
/// it, in a scan planned before it, and after a reopen. A promotion commits exactly
/// when only appends landed.
#[rstest::rstest]
#[case::files(false)]
#[case::inline(true)]
#[tokio::test]
async fn random_writes_during_an_off_lock_promotion_match_the_model(#[case] inline: bool) {
    for seed in 1..=6_u64 {
        let ctx = SessionContext::new();
        let name = format!("off_lock_random_{}_{seed}", u8::from(inline));
        let (provider, _temp_dir, _catalog) = cold_table(&name, inline, false, &ctx).await;
        let mut rng = Xorshift(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let mut model = Model::default();
        let mut next_id = 0_i64;
        let mut tag = 0_i64;
        let mut cold_before;

        for round in 0..4 {
            let context = format!("seed {seed}, round {round}, inline {inline}");
            let n = i64::try_from(30 + rng.below(120)).expect("fits");
            load(&provider, &mut model, next_id, n).await;
            next_id += n;
            flush(&provider).await;
            let at_capture = model.clone();
            cold_before = cold_manifest_urls(&provider).await;

            let parked = park_promotion(&provider).await;
            let mut ops = Vec::new();
            for _ in 0..rng.below(5) {
                let held: Vec<i64> = model.0.keys().copied().collect();
                let op = match rng.below(4) {
                    0 => {
                        let n = i64::try_from(1 + rng.below(40)).expect("fits");
                        let op = Op::Append { start: next_id, n };
                        next_id += n;
                        op
                    }
                    1 | 2 => Op::Upsert(held[rng.below(held.len())]),
                    _ => Op::Delete(held[rng.below(held.len())]),
                };
                tag += 1;
                op.apply(&provider, &mut model, tag).await;
                ops.push(op);
            }
            // Only appends: the writes this promotion must carry rather than lose.
            let only_appends = ops.iter().all(|op| op.is_append());
            assert_eq!(
                scan_rows(&provider).await,
                model.rows(),
                "{context}: writes are visible while the promotion is paused: {ops:?}"
            );
            let during = model.rows();
            let ctx_during = SessionContext::new();
            let plan_during = provider
                .scan(&ctx_during.state(), Some(&vec![0, 1]), &[], None)
                .await
                .expect("a scan planned before the commit");

            let committed = parked.resume().await;
            assert_eq!(
                committed, only_appends,
                "{context}: a promotion commits exactly when only appends landed: {ops:?}"
            );
            assert_eq!(
                scan_rows(&provider).await,
                model.rows(),
                "{context}: the table matches the model after the promotion: {ops:?}"
            );
            assert_eq!(
                collect_rows(plan_during, &ctx_during).await,
                during,
                "{context}: a scan planned before the commit reads the state it captured"
            );
            if committed {
                assert_eq!(
                    cold_manifest_rows(&provider).await,
                    i64::try_from(at_capture.0.len()).expect("fits"),
                    "{context}: the cold files hold exactly the rows read at the capture"
                );
            } else {
                assert_eq!(
                    cold_manifest_urls(&provider).await,
                    cold_before,
                    "{context}: a discarded promotion leaves the cold manifest as it was"
                );
                assert!(
                    provider
                        .promote_warm_to_cold()
                        .await
                        .expect("next promotion"),
                    "{context}: the promotion that holds writers off commits"
                );
                assert_eq!(
                    scan_rows(&provider).await,
                    model.rows(),
                    "{context}: the table matches the model after the retry"
                );
                assert_eq!(
                    cold_manifest_rows(&provider).await,
                    i64::try_from(model.0.len()).expect("fits"),
                    "{context}: the retry moves every live row to the cold files"
                );
            }
            assert_eq!(
                reopened_rows(&provider, &name).await,
                model.rows(),
                "{context}: the table matches the model after a reopen"
            );
        }
    }
}

/// A deterministic generator, so a failing round names its seed and replays.
struct Xorshift(u64);

impl Xorshift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: usize) -> usize {
        usize::try_from(self.next() % u64::try_from(bound).expect("fits")).expect("fits")
    }
}
