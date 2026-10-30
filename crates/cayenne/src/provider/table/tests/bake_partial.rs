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

//! A seq-prefix bake re-encodes only the input files its deletion filter removes
//! a row from, and links the rest into its output. These tests run the same
//! history through a table that bakes that way and one that rewrites every file,
//! and hold both to a model of what the table must contain.

use super::*;
use arrow::array::StringArray;
use std::collections::BTreeMap;

/// Rows per checkpoint: about 4 MiB, four 1 MiB files routed on `ts`.
const ROWS_PER_CHECKPOINT: i64 = 20_000;

/// Keys of the previous checkpoint each checkpoint writes again, the newest
/// ones: a replay of recent points, as the metrics workload sends.
const REPLAYED_PER_CHECKPOINT: i64 = 500;

/// Keys of the previous checkpoint each checkpoint deletes outright, below the
/// replayed ones.
const DELETED_PER_CHECKPOINT: i64 = 100;

/// Live rows by key `(ts, h)`, with their version.
type Model = BTreeMap<(i64, i64), i64>;

/// The primary key a test table is keyed by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Key {
    /// `(ts, h)`: a composite key, probed by its encoded bytes — the metrics
    /// workload's `(time_unix_nano, point_hash)`.
    Composite,
    /// `ts` alone: a single Int64 key, whose probe can skip a file outright when
    /// the file's key range misses every tombstoned key.
    Int64,
}

impl Key {
    fn columns(self) -> Vec<String> {
        match self {
            Self::Composite => vec!["ts".to_string(), "h".to_string()],
            Self::Int64 => vec!["ts".to_string()],
        }
    }
}

fn schema(key: Key) -> SchemaRef {
    let mut fields = vec![Field::new("ts", DataType::Int64, false)];
    if key == Key::Composite {
        fields.push(Field::new("h", DataType::Int64, false));
    }
    fields.push(Field::new("v", DataType::Int64, false));
    fields.push(Field::new("pad", DataType::Utf8, false));
    Arc::new(Schema::new(fields))
}

fn hash_of(ts: i64) -> i64 {
    (ts.wrapping_mul(7_919)).rem_euclid(1_000)
}

/// Incompressible hex, so files stay large enough to split.
fn pad(ts: i64, v: i64) -> String {
    use std::fmt::Write as _;
    let mut x = (ts.unsigned_abs() << 8) ^ v.unsigned_abs() ^ 0x9E37_79B9_7F4A_7C15;
    let mut hex = String::with_capacity(4 * 32);
    for _ in 0..4 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        write!(hex, "{x:016x}{:016x}", x.rotate_left(29)).expect("write to a String");
    }
    hex
}

fn config() -> VortexConfig {
    VortexConfig {
        cdc_durability: crate::metadata::CdcDurability::Memory,
        deletion_mode: crate::metadata::DeletionMode::Key,
        compaction_background_interval_ms: 0,
        write_concurrency: Some(4),
        target_vortex_file_size_mb: 1,
        ..VortexConfig::default()
    }
}

fn on_conflict(key: Key) -> OnConflict {
    OnConflict::Upsert(
        datafusion_table_providers::util::column_reference::ColumnReference::new(key.columns()),
    )
}

struct Table {
    provider: CayenneTableProvider,
    catalog: Arc<dyn MetadataCatalog>,
    name: &'static str,
    key: Key,
    tmp: TempDir,
}

async fn table_keyed(name: &'static str, key: Key, rewrite_all: bool) -> Table {
    let ctx = SessionContext::new();
    let (provider, catalog, tmp) = create_cdc_table_with_schema(
        name,
        ctx.runtime_env(),
        schema(key),
        key.columns(),
        config(),
        on_conflict(key),
    )
    .await;
    provider.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    provider.test_bake_files.lock().rewrite_all = rewrite_all;
    Table {
        provider,
        catalog,
        name,
        key,
        tmp,
    }
}

async fn table(name: &'static str, rewrite_all: bool) -> Table {
    table_keyed(name, Key::Composite, rewrite_all).await
}

/// Open the table again over its catalog and files, as a restart does.
async fn reopen(
    catalog: &Arc<dyn MetadataCatalog>,
    tmp: &TempDir,
    name: &str,
    key: Key,
) -> CayenneTableProvider {
    let ctx = SessionContext::new();
    let base_path = format!("{}/data", tmp.path().to_str().expect("utf-8 temp dir"));
    let provider = CayenneTableProviderBuilder::new(Arc::clone(catalog), ctx.runtime_env())
        .create(CreateTableOptions {
            table_name: name.to_string(),
            schema: schema(key),
            primary_key: key.columns(),
            on_conflict: Some(on_conflict(key)),
            base_path,
            partition_column: None,
            vortex_config: config(),
        })
        .await
        .expect("table reopened");
    provider.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    provider
}

fn key_columns(key: Key, keys: &[i64]) -> Vec<ArrayRef> {
    let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(keys.to_vec()))];
    if key == Key::Composite {
        columns.push(Arc::new(Int64Array::from_iter_values(
            keys.iter().map(|ts| hash_of(*ts)),
        )));
    }
    columns
}

/// Upsert `(ts, v)` rows into `provider`.
async fn upsert(provider: &CayenneTableProvider, key: Key, rows: &[(i64, i64)]) {
    let keys: Vec<i64> = rows.iter().map(|(ts, _)| *ts).collect();
    let mut columns = key_columns(key, &keys);
    columns.push(Arc::new(Int64Array::from_iter_values(
        rows.iter().map(|(_, v)| *v),
    )));
    columns.push(Arc::new(StringArray::from_iter_values(
        rows.iter().map(|(ts, v)| pad(*ts, *v)),
    )));
    let batch = RecordBatch::try_new(schema(key), columns).expect("batch");
    let ctx = SessionContext::new();
    provider
        .write_cdc_append_stream(single_batch_stream(batch), &ctx.task_ctx())
        .await
        .expect("CDC upsert")
        .finish()
        .await
        .expect("CDC upsert published");
}

/// Delete the rows keyed by `keys` from `provider`.
async fn delete(provider: &CayenneTableProvider, key: Key, keys: &[i64]) {
    let fields: Vec<Field> = key
        .columns()
        .into_iter()
        .map(|name| Field::new(name, DataType::Int64, false))
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), key_columns(key, keys))
        .expect("key batch");
    assert!(
        provider
            .write_cdc_delete_keys_in_memory(&batch)
            .await
            .expect("CDC delete")
            .is_some(),
        "precondition: the delete went through the in-memory CDC path"
    );
}

/// Upsert `(ts, v)` rows into `provider` and checkpoint them.
async fn write_checkpoint(provider: &CayenneTableProvider, key: Key, rows: &[(i64, i64)]) {
    upsert(provider, key, rows).await;
    provider.checkpoint_mem_tier().await.expect("checkpoint");
}

/// Checkpoint `c`: its own fresh keys at version `c`, the newest
/// `REPLAYED_PER_CHECKPOINT` keys of checkpoint `c - 1` again at version `c`,
/// and, `with_deletes`, the `DELETED_PER_CHECKPOINT` keys of `c - 1` below
/// those deleted.
async fn write_checkpoints_keyed(
    tables: &[(&CayenneTableProvider, Key)],
    model: &mut Model,
    range: std::ops::Range<i64>,
    with_deletes: bool,
) {
    for c in range {
        let fresh = c * ROWS_PER_CHECKPOINT;
        let mut rows: Vec<(i64, i64)> = (fresh..fresh + ROWS_PER_CHECKPOINT)
            .map(|ts| (ts, c))
            .collect();
        if c > 0 {
            rows.extend((fresh - REPLAYED_PER_CHECKPOINT..fresh).map(|ts| (ts, c)));
        }
        let deleted: Vec<i64> = if with_deletes && c > 0 {
            let top = fresh - REPLAYED_PER_CHECKPOINT;
            (top - DELETED_PER_CHECKPOINT..top).collect()
        } else {
            Vec::new()
        };
        for (ts, v) in &rows {
            model.insert((*ts, hash_of(*ts)), *v);
        }
        for ts in &deleted {
            model.remove(&(*ts, hash_of(*ts)));
        }
        for (provider, key) in tables {
            upsert(provider, *key, &rows).await;
            if !deleted.is_empty() {
                delete(provider, *key, &deleted).await;
            }
            provider.checkpoint_mem_tier().await.expect("checkpoint");
        }
    }
}

async fn write_checkpoints(t: &Table, model: &mut Model, range: std::ops::Range<i64>) {
    write_checkpoints_keyed(&[(&t.provider, t.key)], model, range, false).await;
}

/// Every `(ts, h, v)` the table returns, in key order. A table keyed by `ts`
/// alone reports `h` as the function of `ts` it is everywhere else.
async fn rows_of(provider: &CayenneTableProvider, name: &str) -> Vec<(i64, i64, i64)> {
    let ctx = SessionContext::new();
    ctx.register_table(name, Arc::new(provider.clone_for_write()))
        .expect("table registered");
    let batches = ctx
        .sql(&format!("SELECT ts, v FROM {name} ORDER BY ts, v"))
        .await
        .expect("query planned")
        .collect()
        .await
        .expect("query ran");
    let mut rows = Vec::new();
    for batch in &batches {
        let column = |i: usize| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 column")
                .clone()
        };
        let (ts, v) = (column(0), column(1));
        for row in 0..batch.num_rows() {
            rows.push((ts.value(row), hash_of(ts.value(row)), v.value(row)));
        }
    }
    rows
}

fn model_rows(model: &Model) -> Vec<(i64, i64, i64)> {
    model.iter().map(|((ts, h), v)| (*ts, *h, *v)).collect()
}

/// Assert the table returns exactly the model's rows, naming the first
/// difference rather than dumping 100k rows.
fn assert_rows(actual: &[(i64, i64, i64)], model: &Model, what: &str) {
    let expected = model_rows(model);
    if let Some(index) = actual
        .iter()
        .zip(&expected)
        .position(|(actual, expected)| actual != expected)
    {
        panic!(
            "{what}: row {index} is {:?}, the model has {:?} ({} rows returned, {} expected)",
            actual[index],
            expected[index],
            actual.len(),
            expected.len()
        );
    }
    assert_eq!(
        actual.len(),
        expected.len(),
        "{what}: {} rows returned, the model has {}",
        actual.len(),
        expected.len()
    );
}

/// Bake `t` and require the bake to commit.
async fn bake(provider: &CayenneTableProvider, what: &str) {
    assert!(
        provider
            .bake_seq_prefix_protected_snapshots()
            .await
            .expect("bake"),
        "{what}: the bake commits"
    );
}

/// Physically delete the snapshot directories `provider` has retired: elapse
/// their grace, sweep, and wait until they are gone.
///
/// A scan first, so the provider's cached scan view moves to the live snapshots
/// and releases its pins on the retired ones, as the next query does.
async fn sweep_retired(provider: &CayenneTableProvider, name: &str) {
    let _ = rows_of(provider, name).await;
    let retired: Vec<String> = provider
        .retired_snapshot_dirs
        .lock()
        .keys()
        .cloned()
        .collect();
    let past = std::time::Instant::now()
        .checked_sub(CayenneTableProvider::SNAPSHOT_CLEANUP_GRACE * 2)
        .expect("the clock has run longer than two graces");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        // A pass that finds the file-deletion hold taken (an orphaned-DV sweep the
        // bake scheduled) deletes nothing, so sweep again on every poll.
        for at in provider.retired_snapshot_dirs.lock().values_mut() {
            *at = past;
        }
        for at in provider.snapshot_last_listed.lock().values_mut() {
            *at = past;
        }
        provider.sweep_retired_snapshot_dirs();
        let mut left: Vec<&String> = Vec::new();
        for id in &retired {
            if tokio::fs::try_exists(provider.snapshot_dir_path_for(id))
                .await
                .unwrap_or(true)
            {
                left.push(id);
            }
        }
        if left.is_empty() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "retired snapshot dirs still on disk after 30 s: {left:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// One history — recent keys replayed, keys deleted, two bakes, the bakes'
/// retired inputs swept from disk, a restart and a bake after it — gives the
/// same rows from a table that keeps the files a bake removes no row from as
/// from one that rewrites every file, and both match the model. The partial
/// table must actually keep files, and still rewrite the ones holding replayed
/// or deleted keys, or the comparison proves nothing.
async fn partial_bake_matches_full_bake(
    key: Key,
    partial_name: &'static str,
    full_name: &'static str,
) {
    let partial = table_keyed(partial_name, key, false).await;
    let full = table_keyed(full_name, key, true).await;
    let mut model = Model::new();

    write_checkpoints_keyed(
        &[(&partial.provider, key), (&full.provider, key)],
        &mut model,
        0..6,
        true,
    )
    .await;
    assert!(
        partial.provider.pk_deletion_snapshot().delete_len() > 0,
        "precondition: the replays and deletes left tombstones"
    );

    for round in 1..=2 {
        bake(&partial.provider, &format!("round {round}: partial")).await;
        bake(&full.provider, &format!("round {round}: full")).await;
        let (rewritten, kept) = partial
            .provider
            .test_bake_files
            .lock()
            .last
            .expect("the partial table's bake split its inputs");
        assert!(
            rewritten > 0 && kept > 0,
            "round {round}: the bake rewrites the files holding replayed or deleted keys \
             and keeps the rest (rewritten {rewritten}, kept {kept})"
        );
        assert_eq!(
            full.provider.test_bake_files.lock().last,
            None,
            "round {round}: the full table's bake rewrote every file"
        );
        assert_eq!(
            partial.provider.pk_deletion_snapshot().delete_len(),
            full.provider.pk_deletion_snapshot().delete_len(),
            "round {round}: both bakes prune the same tombstones"
        );

        sweep_retired(&partial.provider, partial.name).await;
        assert_rows(
            &rows_of(&full.provider, full.name).await,
            &model,
            &format!("round {round}: full bake"),
        );
        assert_rows(
            &rows_of(&partial.provider, partial.name).await,
            &model,
            &format!("round {round}: partial bake, retired inputs swept"),
        );

        write_checkpoints_keyed(
            &[(&partial.provider, key), (&full.provider, key)],
            &mut model,
            (6 + 4 * (round - 1))..(6 + 4 * round),
            true,
        )
        .await;
    }

    assert_rows(
        &rows_of(&partial.provider, partial.name).await,
        &model,
        "after the last writes",
    );
    // Restart: the old provider goes away before the table is opened again.
    let Table {
        provider,
        catalog,
        name,
        tmp,
        ..
    } = partial;
    drop(provider);
    let reopened = reopen(&catalog, &tmp, name, key).await;
    assert_rows(&rows_of(&reopened, name).await, &model, "after a restart");

    // Bake again after the restart, over files earlier bakes kept and linked.
    write_checkpoints_keyed(&[(&reopened, key)], &mut model, 14..17, true).await;
    bake(&reopened, "after the restart").await;
    sweep_retired(&reopened, name).await;
    assert_rows(
        &rows_of(&reopened, name).await,
        &model,
        "after a bake following the restart",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partial_bake_returns_the_rows_a_full_bake_does() {
    partial_bake_matches_full_bake(Key::Composite, "bake_partial_kept", "bake_partial_full").await;
}

/// As [`a_partial_bake_returns_the_rows_a_full_bake_does`] on a single Int64
/// key, whose probe skips the filter for a file whose key range misses every
/// tombstoned key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partial_bake_on_an_int64_key_returns_the_rows_a_full_bake_does() {
    partial_bake_matches_full_bake(
        Key::Int64,
        "bake_partial_int64_kept",
        "bake_partial_int64_full",
    )
    .await;
}

/// The kept files' names are checked before any is linked: a name repeated
/// among them, or already written by the bake, is a collision.
#[test]
fn a_bake_refuses_to_link_two_files_of_one_name() {
    let written: HashSet<String> = ["w2_p000_00000.vortex".to_string()].into();
    assert_eq!(
        first_bake_link_collision(["a.vortex", "b.vortex"], &written),
        None
    );
    assert_eq!(
        first_bake_link_collision(["a.vortex", "b.vortex", "a.vortex"], &written),
        Some("a.vortex")
    );
    assert_eq!(
        first_bake_link_collision(["a.vortex", "w2_p000_00000.vortex"], &written),
        Some("w2_p000_00000.vortex")
    );
}

/// Splitting a bake's inputs scans subsets of their files, which must leave
/// every later capture of those snapshots reading all of their files.
///
/// The snapshot listing a capture reuses is keyed only by snapshot id and
/// generation, and a scan stores its listing there. A subset scan that stored
/// its listing would leave the next capture of the snapshot — a compaction that
/// would then retire it, after a bake that did not commit — reading only the
/// subset. The split runs through the bake's own session.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn splitting_a_bakes_inputs_leaves_their_listings_whole() {
    let t = table("bake_partial_listing", false).await;
    let mut model = Model::new();
    write_checkpoints(&t, &mut model, 0..4).await;

    let mut selected: Vec<(String, i64)> = t
        .provider
        .protected_snapshots
        .load()
        .iter()
        .map(|(id, threshold)| (id.clone(), *threshold))
        .collect();
    selected.sort();
    let mut whole: Vec<(String, usize, u64)> = Vec::new();
    let ctx = t.provider.create_compaction_session_context();
    let state = ctx.state();
    for (id, _) in &selected {
        let files = t
            .provider
            .capture_snapshot_files_under_fence(id)
            .await
            .expect("capture")
            .files
            .len();
        let plan = t
            .provider
            .create_snapshot_scan_plan(&state, id, None, &[], None)
            .await
            .expect("scan plan");
        let rows = count_plan_rows(plan, state.task_ctx()).await.expect("scan");
        whole.push((id.clone(), files, rows));
    }

    let pk_indices = t.provider.pk_column_indices.clone();
    let partition = t
        .provider
        .bake_partition_input_files(
            &state,
            &selected,
            &pk_indices,
            &t.provider.pk_deletion_snapshot(),
        )
        .await
        .expect("the inputs split: older files hold no replayed key");
    assert!(
        partition.kept_files() > 0 && partition.rewritten_files() > 0,
        "precondition: the split both keeps and rewrites files"
    );

    // Newest first: the split checks the inputs in order, so a listing it stored
    // would be the last input's, and a capture of any other snapshot would
    // replace it before the last input is checked.
    for (id, files, rows) in whole.iter().rev() {
        let captured = t
            .provider
            .capture_snapshot_files_under_fence(id)
            .await
            .expect("capture after the split");
        assert_eq!(
            captured.files.len(),
            *files,
            "snapshot {id}: a capture after the split lists every file"
        );
        let plan = t
            .provider
            .create_snapshot_scan_plan(&state, id, None, &[], None)
            .await
            .expect("scan plan after the split");
        assert_eq!(
            count_plan_rows(plan, state.task_ctx()).await.expect("scan"),
            *rows,
            "snapshot {id}: a scan after the split reads every row"
        );
    }
}

/// The per-file check takes each input file's row count from its footer, as an
/// exact count equal to the rows a scan of the file reads, so it reads the
/// file's key columns once rather than twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bake_input_files_row_count_comes_exact_from_its_footer() {
    let t = table("bake_partial_exact_rows", false).await;
    let mut model = Model::new();
    write_checkpoints(&t, &mut model, 0..2).await;

    let ctx = t.provider.create_compaction_session_context();
    let state = ctx.state();
    let projection = t.provider.pk_column_indices.clone();
    let protected: Vec<String> = t
        .provider
        .protected_snapshots
        .load()
        .keys()
        .cloned()
        .collect();
    let mut checked = 0;
    for id in &protected {
        let captured = t
            .provider
            .capture_snapshot_files_under_fence(id)
            .await
            .expect("capture");
        for file in captured.files.iter() {
            let footer = t
                .provider
                .bake_file_row_count(&state, id, file)
                .await
                .expect("footer row count");
            let scan = t
                .provider
                .scan_bake_files(
                    &state,
                    id,
                    Some(&projection),
                    &captured.subset(vec![file.clone()]),
                )
                .await
                .expect("key scan");
            let counted = count_plan_rows(scan, state.task_ctx())
                .await
                .expect("count");
            assert_eq!(
                footer,
                Some(counted),
                "snapshot {id}: the footer reports the file's exact row count"
            );
            checked += 1;
        }
    }
    assert!(
        checked > 0,
        "precondition: the table has input files to check"
    );
}
