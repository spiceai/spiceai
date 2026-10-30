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

//! A seq-prefix bake runs for tens of seconds on a large table. These tests hold
//! one mid-encode and check what it may keep other tables' writes from doing.

use super::*;
use arrow::array::StringArray;
use std::time::Duration;

/// Encode permits on the evidence rig: eight cores give six, and the EBS
/// write-bandwidth cap lowers them to four, which leaves maintenance two.
const RIG_ENCODE_PERMITS: usize = 4;

/// Removes the process-global encode budget when dropped.
struct InstalledBudget;
impl Drop for InstalledBudget {
    fn drop(&mut self) {
        crate::provider::write_budget::clear_global_encode_budget();
    }
}

/// Pseudo-random hex per key, so on-disk sizes stay large enough to shard.
fn pad(id: i64) -> String {
    use std::fmt::Write as _;
    let mut x = id.unsigned_abs() ^ 0x9E37_79B9_7F4A_7C15;
    let mut hex = String::with_capacity(6 * 32);
    for _ in 0..6 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        write!(hex, "{x:016x}{:016x}", x.rotate_left(29)).expect("write to a String");
    }
    hex
}

fn pad_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("pad", DataType::Utf8, false),
    ]))
}

fn pad_batch(schema: &SchemaRef, keys: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(keys.to_vec())),
            Arc::new(StringArray::from_iter_values(keys.iter().map(|id| pad(*id)))),
        ],
    )
    .expect("pad batch")
}

/// A memory-durability upsert table whose writes shard four ways once they
/// exceed four 1 MiB target files.
async fn pad_table(name: &str) -> (CayenneTableProvider, Arc<dyn MetadataCatalog>, TempDir) {
    let ctx = SessionContext::new();
    let (provider, catalog, tmp) = create_cdc_table_with_schema(
        name,
        ctx.runtime_env(),
        pad_schema(),
        vec!["id".to_string()],
        VortexConfig {
            cdc_durability: crate::metadata::CdcDurability::Memory,
            deletion_mode: crate::metadata::DeletionMode::Key,
            compaction_background_interval_ms: 0,
            write_concurrency: Some(4),
            target_vortex_file_size_mb: 1,
            ..VortexConfig::default()
        },
        OnConflict::Upsert(
            datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                "id".to_string(),
            ]),
        ),
    )
    .await;
    provider.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    (provider, catalog, tmp)
}

async fn upsert_keys(provider: &CayenneTableProvider, keys: &[i64]) {
    let ctx = SessionContext::new();
    provider
        .write_cdc_append_stream(
            single_batch_stream(pad_batch(&provider.table_schema(), keys)),
            &ctx.task_ctx(),
        )
        .await
        .expect("CDC upsert")
        .finish()
        .await
        .expect("CDC upsert published");
}

/// Five checkpoints, each re-upserting part of the first, so the deletion index
/// holds tombstones the oldest checkpoints' rows answer to and a bake has a
/// prefix to fold.
async fn bakeable_table(name: &str) -> (CayenneTableProvider, Arc<dyn MetadataCatalog>, TempDir) {
    const ROWS_PER_CHECKPOINT: i64 = 10_000;
    let (provider, catalog, tmp) = pad_table(name).await;
    for checkpoint in 0..5 {
        let fresh = checkpoint * ROWS_PER_CHECKPOINT;
        let mut keys: Vec<i64> = (fresh..fresh + ROWS_PER_CHECKPOINT).collect();
        if checkpoint > 0 {
            // Supersede a slice of the first checkpoint's keys.
            keys.extend((checkpoint * 500)..(checkpoint * 500 + 500));
        }
        upsert_keys(&provider, &keys).await;
        provider.checkpoint_mem_tier().await.expect("checkpoint");
    }
    assert!(
        provider.pk_deletion_snapshot().delete_len() > 0,
        "precondition: re-upserted keys left tombstones for the bake to apply"
    );
    (provider, catalog, tmp)
}

/// Install a hook that holds `provider`'s next bake once its encode starts:
/// `reached` fires when the bake pulls its first input row, and the bake then
/// waits for `release`.
fn hold_bake_mid_encode(
    provider: &CayenneTableProvider,
) -> (
    tokio::sync::oneshot::Receiver<()>,
    Arc<tokio::sync::Notify>,
) {
    let (reached_tx, reached_rx) = tokio::sync::oneshot::channel();
    let release = Arc::new(tokio::sync::Notify::new());
    let release_in_hook = Arc::clone(&release);
    *provider.test_bake_input_hook.lock() = Some(Box::new(move || {
        Box::pin(async move {
            let _ = reached_tx.send(());
            release_in_hook.notified().await;
        })
    }));
    (reached_rx, release)
}

/// A mem-tier checkpoint on one table must not wait for a bake on another to
/// finish encoding.
///
/// On the evidence rig the encode budget is four permits. A bake takes the two
/// the maintenance class may hold for its whole encode — tens of seconds on a
/// large table — and a checkpoint that shards four ways asked for all four, so
/// it waited out the whole bake while its table's appends backed up behind the
/// tier it could not flush.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_checkpoint_does_not_wait_for_another_tables_bake_to_encode() {
    let _serialized = crate::provider::write_budget::GLOBAL_ENCODE_BUDGET_TEST_LOCK
        .lock()
        .await;
    crate::provider::write_budget::set_global_encode_concurrency(RIG_ENCODE_PERMITS);
    let _installed = InstalledBudget;

    let (baking, _baking_catalog, _baking_tmp) = bakeable_table("bake_isolation_baker").await;
    let (writer, _writer_catalog, _writer_tmp) = pad_table("bake_isolation_writer").await;
    // About 6 MiB in the tier: the checkpoint shards four ways.
    let writer_keys: Vec<i64> = (0..30_000).collect();
    upsert_keys(&writer, &writer_keys).await;

    let (reached, release) = hold_bake_mid_encode(&baking);
    let baking = Arc::new(baking);
    let bake = tokio::spawn({
        let baking = Arc::clone(&baking);
        async move { baking.bake_seq_prefix_protected_snapshots().await }
    });
    tokio::time::timeout(Duration::from_secs(60), reached)
        .await
        .expect("the bake reaches its encode within 60 s")
        .expect("the bake hook fired");
    let held = crate::provider::write_budget::encode_budget_snapshot()
        .expect("budget installed");
    assert!(
        held.available < held.total,
        "precondition: the bake holds encode permits mid-encode ({held:?})"
    );

    let checkpoint = tokio::time::timeout(Duration::from_secs(20), writer.checkpoint_mem_tier()).await;
    release.notify_one();
    let flushed = checkpoint
        .unwrap_or_else(|_| {
            panic!(
                "the checkpoint waited more than 20 s for the bake on another table to \
                 finish encoding (budget while the bake held its permits: {held:?})"
            )
        })
        .expect("checkpoint");
    assert_eq!(flushed, 30_000, "the checkpoint flushed the whole tier");

    assert!(
        bake.await.expect("bake task").expect("bake"),
        "the bake commits once released"
    );
    let ctx = SessionContext::new();
    let rows = read_all(&ctx, &writer, "bake_isolation_writer").await;
    assert_eq!(
        rows.iter().map(RecordBatch::num_rows).sum::<usize>(),
        30_000,
        "every row the checkpoint flushed is readable"
    );
}

/// While one table bakes, another table's bake declines rather than run
/// alongside it, and runs once the first has finished.
///
/// Two overlapping whole-prefix bakes on the evidence rig ran 30 and 52 s while
/// ingest halved and query P99 rose five-fold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_tables_bake_waits_for_the_first_to_finish() {
    let (first, _first_catalog, _first_tmp) = bakeable_table("bake_isolation_first").await;
    let (second, _second_catalog, _second_tmp) = bakeable_table("bake_isolation_second").await;
    let second_protected_before = second.protected_snapshots.load().len();

    let (reached, release) = hold_bake_mid_encode(&first);
    let first = Arc::new(first);
    let first_bake = tokio::spawn({
        let first = Arc::clone(&first);
        async move { first.bake_seq_prefix_protected_snapshots().await }
    });
    tokio::time::timeout(Duration::from_secs(60), reached)
        .await
        .expect("the first bake reaches its encode within 60 s")
        .expect("the bake hook fired");

    let concurrent = tokio::time::timeout(
        Duration::from_secs(60),
        second.bake_seq_prefix_protected_snapshots(),
    )
    .await
    .expect("the second bake returns within 60 s")
    .expect("second bake");
    let second_protected_during = second.protected_snapshots.load().len();
    release.notify_one();
    assert!(
        first_bake.await.expect("bake task").expect("first bake"),
        "the first bake commits once released"
    );
    assert!(
        !concurrent,
        "the second table must not bake while the first table's bake runs"
    );
    assert_eq!(
        second_protected_during, second_protected_before,
        "the declined bake left the second table's protected snapshots alone"
    );

    assert!(
        second
            .bake_seq_prefix_protected_snapshots()
            .await
            .expect("second bake after the first"),
        "the second table bakes once the first bake has finished"
    );
}

/// A bake encodes on at most half of the permits the maintenance class may
/// hold, so other tables' compaction and checkpoints keep the rest for its
/// whole duration.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bake_encodes_on_half_the_maintenance_permits() {
    let _serialized = crate::provider::write_budget::GLOBAL_ENCODE_BUDGET_TEST_LOCK
        .lock()
        .await;
    crate::provider::write_budget::set_global_encode_concurrency(RIG_ENCODE_PERMITS);
    let _installed = InstalledBudget;
    // Four permits reserve two for deltas, so maintenance may hold two and a
    // bake one. Uncapped, this bake's two encode shards take both.
    let (baking, _catalog, _tmp) = bakeable_table("bake_isolation_shards").await;

    let (reached, release) = hold_bake_mid_encode(&baking);
    let baking = Arc::new(baking);
    let bake = tokio::spawn({
        let baking = Arc::clone(&baking);
        async move { baking.bake_seq_prefix_protected_snapshots().await }
    });
    tokio::time::timeout(Duration::from_secs(60), reached)
        .await
        .expect("the bake reaches its encode within 60 s")
        .expect("the bake hook fired");
    let held = crate::provider::write_budget::encode_budget_snapshot()
        .expect("budget installed");
    release.notify_one();
    assert!(
        bake.await.expect("bake task").expect("bake"),
        "the bake commits once released"
    );
    assert_eq!(
        held.total - held.available,
        1,
        "the bake holds half of the two maintenance permits ({held:?})"
    );
}
