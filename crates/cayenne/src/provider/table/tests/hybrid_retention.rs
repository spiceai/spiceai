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

//! `cdc_durability: memory` (durable files + RAM tier) `retention_sql` in the
//! apply's own tier swap, against the other mutators, with deterministic
//! barriers (test seams that hold a mutator at a known point) rather than
//! timing: checkpoint, compaction and the retention maintenance pass.
//!
//! Predicate `aged(value)`, switched by the test: OFF matches nothing, ON
//! matches a negative value ("archived"), BROKEN fails on a negative value.

use super::*;
use crate::metadata::DeletionMode;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility, create_udf};
use datafusion::prelude::col;
use std::sync::atomic::AtomicU8;

const OFF: u8 = 0;
const ON: u8 = 1;
const BROKEN: u8 = 2;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn aged(mode: Arc<AtomicU8>) -> ScalarUDF {
    create_udf(
        "aged",
        vec![DataType::Int64],
        DataType::Boolean,
        Volatility::Volatile,
        Arc::new(move |args: &[ColumnarValue]| {
            let array = args[0].clone().into_array(1)?;
            let values = array
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("int64");
            let mode = mode.load(Ordering::SeqCst);
            let mut out = Vec::with_capacity(values.len());
            for v in values.iter() {
                let archived = v.is_some_and(|v| v < 0);
                if archived && mode == BROKEN {
                    return Err(datafusion_common::DataFusionError::Execution(
                        "aged() cannot evaluate this row".to_string(),
                    ));
                }
                out.push(Some(archived && mode == ON));
            }
            Ok(ColumnarValue::Array(Arc::new(
                arrow::array::BooleanArray::from(out),
            )))
        }),
    )
}

fn config(shards: usize) -> VortexConfig {
    VortexConfig {
        cdc_durability: crate::metadata::CdcDurability::Memory,
        deletion_mode: DeletionMode::Key,
        cdc_mem_tier_shards: shards,
        cdc_mem_tier_min_flush_bytes: 0,
        cdc_mem_tier_checkpoint_interval_ms: 0,
        cdc_mem_tier_max_age_ms: 0,
        cdc_mem_tier_seal_age_ms: 0,
        compaction_background_interval_ms: 3_600_000,
        compaction_trigger_protected_snapshots: 2,
        inline_max_rows: 0,
        inline_max_bytes: 0,
        ..VortexConfig::default()
    }
}

type Seen = Arc<ParkingMutex<Vec<Vec<(i64, i64)>>>>;

/// Records, at every source acknowledgement, the rows a freshly opened provider
/// reads from the catalog: what is durable at the moment the source is told.
struct DurableAtAck {
    catalog: Arc<dyn MetadataCatalog>,
    name: String,
    seen: Seen,
}

#[async_trait::async_trait]
impl crate::provider::mem_tier::SlotAdvancer for DurableAtAck {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {
        let reopened = open(&self.catalog, &self.name, 1, vec![]).await;
        let rows = rows(&reopened).await;
        self.seen.lock().push(rows);
    }
}

async fn open(
    catalog: &Arc<dyn MetadataCatalog>,
    name: &str,
    shards: usize,
    filters: Vec<Expr>,
) -> CayenneTableProvider {
    let runtime_env = SessionContext::new().runtime_env();
    CayenneTableProviderBuilder::new(Arc::clone(catalog), Arc::clone(&runtime_env))
        .with_context(crate::CayenneContext::new(
            &config(shards),
            runtime_env,
            name,
        ))
        .with_retention_filters(filters)
        .open(name)
        .await
        .expect("table reopened")
}

struct Fixture {
    name: String,
    shards: usize,
    provider: CayenneTableProvider,
    catalog: Arc<dyn MetadataCatalog>,
    mode: Arc<AtomicU8>,
    udf: ScalarUDF,
    acks: Seen,
    _dir: TempDir,
}

impl Fixture {
    fn retention(&self) -> Vec<Expr> {
        vec![self.udf.call(vec![col("value")])]
    }

    fn set(&self, mode: u8) {
        self.mode.store(mode, Ordering::SeqCst);
    }

    fn arm(&self, provider: &CayenneTableProvider) {
        provider.install_slot_advancer(Arc::new(DurableAtAck {
            catalog: Arc::clone(&self.catalog),
            name: self.name.clone(),
            seen: Arc::clone(&self.acks),
        }));
    }

    /// The table as a restarted process opens it: retention configured, armed.
    async fn restart(&self) -> CayenneTableProvider {
        let provider = open(&self.catalog, &self.name, self.shards, self.retention()).await;
        self.arm(&provider);
        provider
    }
}

async fn hybrid(name: &str, shards: usize) -> Fixture {
    let dir = tempfile::tempdir().expect("temp dir");
    let metadata_dir = format!("{}/metadata", dir.path().to_str().expect("path"));
    let data_dir = format!("{}/data", dir.path().to_str().expect("path"));
    std::fs::create_dir_all(&metadata_dir).expect("metadata dir");
    let catalog = Arc::new(
        crate::CayenneCatalog::new(format!("sqlite://{metadata_dir}/cayenne.db")).expect("catalog"),
    ) as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("catalog init");
    let mode = Arc::new(AtomicU8::new(OFF));
    let udf = aged(Arc::clone(&mode));
    let options = CreateTableOptions {
        table_name: name.to_string(),
        schema: schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(
            datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                "id".to_string(),
            ]),
        )),
        base_path: data_dir,
        partition_column: None,
        vortex_config: config(shards),
    };
    let runtime_env = SessionContext::new().runtime_env();
    let provider = CayenneTableProviderBuilder::new(Arc::clone(&catalog), Arc::clone(&runtime_env))
        .with_context(crate::CayenneContext::new(
            &config(shards),
            runtime_env,
            name,
        ))
        .with_retention_filters(vec![udf.call(vec![col("value")])])
        .create(options)
        .await
        .expect("table created");
    assert!(provider.is_cdc_memory_mode() && !provider.is_memory_resident_mode());
    assert_eq!(provider.mem_tier_shard_count(), shards.max(1));
    let fixture = Fixture {
        name: name.to_string(),
        shards,
        provider,
        catalog,
        mode,
        udf,
        acks: Arc::new(ParkingMutex::new(Vec::new())),
        _dir: dir,
    };
    fixture.arm(&fixture.provider);
    fixture
}

async fn apply(
    provider: &CayenneTableProvider,
    rows: &[(i64, i64)],
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    let values: Vec<i64> = rows.iter().map(|(_, v)| *v).collect();
    let ctx = SessionContext::new();
    let write = provider
        .write_cdc_append_stream(
            single_batch_stream(id_value_batch(schema(), &ids, &values)),
            &ctx.task_ctx(),
        )
        .await?;
    if write.has_pending_finalize() {
        write.finish().await?;
    }
    Ok(())
}

async fn rows(provider: &CayenneTableProvider) -> Vec<(i64, i64)> {
    use arrow::array::Int64Array;
    let ctx = SessionContext::new();
    let plan = provider
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("scan plan");
    let batches = collect(plan, ctx.task_ctx()).await.expect("scan");
    let mut out = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value");
        out.extend((0..batch.num_rows()).map(|r| (ids.value(r), values.value(r))));
    }
    out.sort_unstable();
    out
}

/// What sits in the RAM tier across all shards: every resident row version
/// (sorted) and every key tombstone (sorted).
fn tier_view(provider: &CayenneTableProvider) -> (Vec<(i64, i64)>, Vec<i64>) {
    use arrow::array::Int64Array;
    let mut rows = Vec::new();
    let mut tombstones = Vec::new();
    for shard in provider.mem_tier.shards() {
        let tier = shard.load();
        for segment in tier.segments.iter() {
            for batch in segment.batches.iter() {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("id");
                let values = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("value");
                rows.extend((0..batch.num_rows()).map(|r| (ids.value(r), values.value(r))));
            }
        }
        tombstones.extend(tier.tombstones.int64_pk.keys().copied());
    }
    rows.sort_unstable();
    tombstones.sort_unstable();
    (rows, tombstones)
}

/// The rows durable in the catalog: a fresh provider (no retention) opened on
/// it, as the ack observer does.
async fn durable_rows(f: &Fixture) -> Vec<(i64, i64)> {
    rows(&open(&f.catalog, &f.name, f.shards, vec![]).await).await
}

/// The durable rows [`seed_keeping_tier`] leaves: keys 2 and 3 at their older
/// versions (retained 2, archived 3).
const DURABLE_SEED: [(i64, i64); 5] = [(1, -1), (2, 2), (3, -3), (4, 4), (5, -5)];
/// The tier rows [`seed_keeping_tier`] leaves: the newer versions of 2 and 3.
const TIER_SEED: [(i64, i64); 2] = [(2, -2), (3, 3)];

/// [`seed_keeping_tier`] with the newer versions of keys 2 and 3 checkpointed too: three
/// protected runs and no pending mem-tier delete, so the delete fence is the
/// durable maximum and every run is at or below it. With the newer versions
/// still in the tier their key deletes are pending and the fence sits below
/// both runs' thresholds, so a merge is declined
/// (`DeclinedAboveDeleteFence`) unless something else checkpoints them first,
/// which made a compaction barrier on tier-resident state nondeterministic.
async fn seed_durable(f: &Fixture) {
    let p = &f.provider;
    f.set(OFF);
    apply(p, &[(1, -1), (2, 2), (3, -3)]).await.expect("seed a");
    p.checkpoint_mem_tier().await.expect("checkpoint a");
    apply(p, &[(4, 4), (5, -5)]).await.expect("seed b");
    p.checkpoint_mem_tier().await.expect("checkpoint b");
    apply(p, &[(2, -2), (3, 3)]).await.expect("seed c");
    p.checkpoint_mem_tier().await.expect("checkpoint c");
    settle(p).await;
    assert_eq!(rows(p).await, SEEDED.to_vec(), "{}: seeded", f.name);
}

/// Wait until no maintenance the writes armed is scheduled or running: the
/// post-checkpoint retention/statistics loop and every flagged detached pass
/// (post-write compaction, orphan-DV sweep, snapshot cleanup, inline
/// checkpoint). Unlike [`quiesce`] it does not finish with a `compaction_lock`
/// barrier, so it is safe while the test holds [`CayenneTableProvider::hold_compaction`]
/// (a compaction pass declines a busy lock with `try_write` and clears its flag).
async fn settle(provider: &CayenneTableProvider) {
    provider
        .drain_in_flight_maintenance_holding_compaction()
        .await
        .expect("maintenance settled");
}

/// Let the post-checkpoint maintenance and post-write compaction the seeding
/// armed finish, so nothing runs behind the barriers below.
async fn quiesce(provider: &CayenneTableProvider) {
    provider
        .drain_in_flight_maintenance()
        .await
        .expect("maintenance drained");
}

const SEEDED: [(i64, i64); 5] = [(1, -1), (2, -2), (3, 3), (4, 4), (5, -5)];
/// After an apply of `(6, 6)` with retention ON: 1 and 5 (durable) and 2
/// (tier over a retained durable version) are gone; 3 (tier over an archived
/// durable version) stays.
const RETAINED: [(i64, i64); 3] = [(3, 3), (4, 4), (6, 6)];

/// Run a protected-subset compaction; `"true"` when it merged, otherwise
/// `"false"` with the merge inputs (protected thresholds, the delete fence and
/// the pending mem-tier delete floor) so a declined merge says why.
async fn compaction_outcome(probe: &CayenneTableProvider) -> String {
    let protected: Vec<i64> = {
        let mut t: Vec<i64> = probe.protected_snapshots.load().values().copied().collect();
        t.sort_unstable();
        t
    };
    let durable_max = probe
        .pk_deletion_snapshot()
        .max_sequence_number()
        .unwrap_or(0);
    let (fence, floor) = probe.protected_snapshot_merge_fence_and_floor(durable_max);
    let merged = probe
        .compact_protected_snapshots_subset(usize::MAX)
        .await
        .expect("compaction");
    if merged {
        "true".to_string()
    } else {
        format!(
            "false (thresholds {protected:?}, durable max {durable_max}, fence {fence}, floor {floor:?})"
        )
    }
}

/// A held mid-scan hook: returns once the scan reaches it, runs `during`
/// there, and records its outcome.
fn hold_mid_scan<F>(provider: &CayenneTableProvider, during: F) -> Arc<ParkingMutex<Option<String>>>
where
    F: FnOnce(CayenneTableProvider) -> futures::future::BoxFuture<'static, String> + Send + 'static,
{
    let outcome = Arc::new(ParkingMutex::new(None));
    let probe = provider.clone_for_write();
    let outcome_in_hook = Arc::clone(&outcome);
    *provider.test_hybrid_retention_mid_scan_hook.lock() = Some(Box::new(move || {
        Box::pin(async move {
            let result = during(probe).await;
            *outcome_in_hook.lock() = Some(result);
        })
    }));
    outcome
}

/// The checkpoint cannot interleave with a hybrid apply's scan and swap: held
/// inside the scan, a checkpoint attempt is refused because its lock is held.
/// The checkpoint that runs afterwards persists the retention deletes before
/// the acknowledgement: the ack observer reopens the table from the catalog and
/// already reads the post-apply rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hybrid_apply_excludes_the_checkpoint_and_acks_only_durable_deletes() {
    for shards in [1_usize, 4] {
        Box::pin(async move {
            let f = hybrid(&format!("hy_ckpt_{shards}"), shards).await;
            let p = &f.provider;
            let hold = seed_keeping_tier(&f).await;
            let probe = hold_mid_scan(p, |probe| {
                Box::pin(async move {
                    // The evaluation barrier: the tier still holds 2 and 3.
                    let tier = tier_view(&probe);
                    let attempt = match probe.try_checkpoint_mem_tier().await.expect("attempt") {
                        CheckpointAttempt::Completed(rows) => format!("ran ({rows} rows)"),
                        CheckpointAttempt::Busy => "busy".to_string(),
                    };
                    format!("{attempt}; tier {tier:?}")
                })
            });
            f.set(ON);
            apply(p, &[(6, 6)]).await.expect("apply");
            assert_eq!(
                probe.lock().take(),
                Some(format!("busy; tier {:?}", (TIER_SEED.to_vec(), vec![2, 3]))),
                "shards={shards}"
            );
            assert_eq!(
                durable_rows(&f).await,
                DURABLE_SEED.to_vec(),
                "shards={shards}: the apply wrote nothing durable"
            );
            drop(hold);
            assert_eq!(
                rows(p).await,
                RETAINED.to_vec(),
                "shards={shards}: in the apply"
            );
            let acks_before = f.acks.lock().len();
            p.checkpoint_mem_tier().await.expect("checkpoint");
            let acks = f.acks.lock().clone();
            assert!(acks.len() > acks_before, "shards={shards}: acked");
            assert_eq!(
                acks.last().cloned(),
                Some(RETAINED.to_vec()),
                "shards={shards}: durable when acked"
            );
        })
        .await;
    }
}

/// A compaction that commits while the scan runs (between the sink's capture of
/// the protected set and its scan of the current listing) changes the resident
/// file generation; the scan is discarded and rerun against the new generation,
/// so it neither misses a row nor pairs two generations. The compaction really
/// runs (asserted), the scan runs exactly twice, and the rows are exact. Then
/// the same with an evaluation error: the apply fails after its retry and
/// nothing is published, in memory or durably.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hybrid_apply_rescans_when_a_compaction_commits_mid_scan() {
    for shards in [1_usize, 4] {
        Box::pin(async move {
            let f = hybrid(&format!("hy_compact_{shards}"), shards).await;
            let p = &f.provider;
            // No post-write compaction may merge the seeded runs before the barrier.
            let held = p.hold_compaction().await;
            seed_durable(&f).await;
            drop(held);
            assert!(
                p.protected_snapshots.load().len() >= 2,
                "shards={shards}: two protected runs to merge"
            );
            assert_eq!(
                p.min_pending_mem_tier_delete_sequence(),
                None,
                "shards={shards}: no pending delete holds the merge fence down"
            );

            let compacted = hold_mid_scan(p, |probe| {
                Box::pin(async move { compaction_outcome(&probe).await })
            });
            p.test_hybrid_retention_captures.store(0, Ordering::SeqCst);
            f.set(ON);
            apply(p, &[(6, 6)]).await.expect("apply");
            assert_eq!(
                compacted.lock().take().as_deref(),
                Some("true"),
                "shards={shards}: merged"
            );
            assert_eq!(
                p.test_hybrid_retention_captures.load(Ordering::SeqCst),
                2,
                "shards={shards}: the scan straddling the commit was rerun once"
            );
            assert_eq!(
                rows(p).await,
                RETAINED.to_vec(),
                "shards={shards}: exact rows"
            );
            p.checkpoint_mem_tier().await.expect("checkpoint");
            assert_eq!(
                rows(&f.restart().await).await,
                RETAINED.to_vec(),
                "shards={shards}: durable"
            );

            // Error after a mid-scan commit: two more protected runs to merge, an
            // archived resident row, then BROKEN.
            f.set(OFF);
            quiesce(p).await;
            let held = p.hold_compaction().await;
            apply(p, &[(7, -7)]).await.expect("archived resident row");
            p.checkpoint_mem_tier().await.expect("checkpoint c");
            apply(p, &[(8, 8)]).await.expect("filler");
            p.checkpoint_mem_tier().await.expect("checkpoint d");
            settle(p).await;
            drop(held);
            assert!(
                p.protected_snapshots.load().len() >= 2,
                "shards={shards}: two protected runs to merge"
            );
            assert_eq!(
                p.min_pending_mem_tier_delete_sequence(),
                None,
                "shards={shards}: no pending delete holds the merge fence down"
            );
            let before = rows(p).await;
            let compacted = hold_mid_scan(p, |probe| {
                Box::pin(async move { compaction_outcome(&probe).await })
            });
            f.set(BROKEN);
            assert!(apply(p, &[(9, 9)]).await.is_err(), "shards={shards}: fails");
            f.set(OFF);
            assert_eq!(
                compacted.lock().take().as_deref(),
                Some("true"),
                "shards={shards}: merged"
            );
            assert_eq!(rows(p).await, before, "shards={shards}: nothing published");
            p.checkpoint_mem_tier().await.expect("checkpoint e");
            assert_eq!(
                rows(&f.restart().await).await,
                before,
                "shards={shards}: nothing durable"
            );
        })
        .await;
    }
}

/// The retention maintenance pass and a hybrid apply are serialized, proven by
/// an entry handshake rather than a timeout: the pass signals once it holds
/// `write_lock` and waits there; while it waits `write_lock` is held, the apply
/// started meanwhile has published nothing, and it publishes after the pass
/// releases, against the state the pass left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn maintenance_pass_and_hybrid_apply_serialize_by_entry_handshake() {
    let f = hybrid("hy_maint", 1).await;
    let p = &f.provider;
    let hold = seed_keeping_tier(&f).await;
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel::<()>();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    *p.test_retention_pass_entered_hook.lock() = Some(Box::new(move || {
        Box::pin(async move {
            entered_tx.send(()).expect("test waits for entry");
            release_rx.await.expect("test releases the pass");
        })
    }));
    f.set(ON);
    drop(hold);
    let pass_table = p.clone_for_write();
    let pass = tokio::spawn(async move { pass_table.apply_retention_filters().await });
    entered_rx.await.expect("the pass entered");
    assert!(
        p.write_lock.try_lock().is_err(),
        "the pass holds write_lock at its entry"
    );
    let apply_table = p.clone_for_write();
    let applied = tokio::spawn(async move { apply(&apply_table, &[(6, 6)]).await });
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
    assert!(!applied.is_finished(), "the apply waits for the pass");
    assert_eq!(
        rows(p).await,
        SEEDED.to_vec(),
        "nothing published while the pass holds"
    );
    release_tx.send(()).expect("pass waiting");
    pass.await.expect("pass task").expect("pass");
    applied.await.expect("apply task").expect("apply");
    assert_eq!(rows(p).await, RETAINED.to_vec(), "pass then apply");
}

/// Replay after a crash. Staged but uncommitted (no checkpoint): a restart
/// reads the previous durable generation and nothing was acknowledged, so the
/// source replays the apply, which publishes the same rows. Committed but
/// unacknowledged (the checkpoint committed, the ack was lost): a restart
/// reads the retention deletes, and replaying the same apply again is a no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hybrid_replay_after_staged_uncommitted_and_committed_unacked_crashes() {
    for shards in [1_usize, 4] {
        Box::pin(async move {
            let f = hybrid(&format!("hy_replay_{shards}"), shards).await;
            let p = &f.provider;
            // Every seeded version durable: this test covers no tier-resident row
            // (see `hybrid_replay_with_tier_resident_rows`).
            seed_durable(&f).await;
            quiesce(p).await;
            let acked = f.acks.lock().len();
            f.set(ON);
            apply(p, &[(6, 6)]).await.expect("apply");
            assert_eq!(rows(p).await, RETAINED.to_vec(), "shards={shards}");
            assert_eq!(f.acks.lock().len(), acked, "shards={shards}: not acked yet");

            // Crash before the checkpoint.
            let restarted = f.restart().await;
            assert_eq!(
                rows(&restarted).await,
                SEEDED.to_vec(),
                "shards={shards}: previous generation"
            );
            apply(&restarted, &[(6, 6)]).await.expect("replay");
            assert_eq!(
                rows(&restarted).await,
                RETAINED.to_vec(),
                "shards={shards}: replayed"
            );

            // The checkpoint commits; treat its ack as lost and crash.
            restarted.checkpoint_mem_tier().await.expect("checkpoint");
            let restarted = f.restart().await;
            assert_eq!(
                rows(&restarted).await,
                RETAINED.to_vec(),
                "shards={shards}: committed"
            );
            apply(&restarted, &[(6, 6)])
                .await
                .expect("replay of the unacked apply");
            restarted.checkpoint_mem_tier().await.expect("checkpoint");
            assert_eq!(
                rows(&restarted).await,
                RETAINED.to_vec(),
                "shards={shards}: idempotent"
            );
            assert_eq!(
                rows(&f.restart().await).await,
                RETAINED.to_vec(),
                "shards={shards}: durable"
            );
        })
        .await;
    }
}

/// Retention OFF while seeding. Durable: two checkpoint snapshots (two
/// protected runs of one size tier) holding [`DURABLE_SEED`]. Tier: the newer
/// versions of keys 2 (retained -> archived) and 3 (archived -> retained), kept
/// there by holding the retention maintenance pass from before the tier write
/// (the pass forces a tier checkpoint, and [`settle`] runs it). Asserts both
/// layers independently, then returns the hold. The caller must not
/// [`settle`]/[`quiesce`] while holding it: the drain would wait on the pass.
async fn seed_keeping_tier(f: &Fixture) -> tokio::sync::OwnedRwLockWriteGuard<()> {
    let p = &f.provider;
    f.set(OFF);
    apply(p, &[(1, -1), (2, 2), (3, -3)]).await.expect("seed a");
    p.checkpoint_mem_tier().await.expect("checkpoint a");
    apply(p, &[(4, 4), (5, -5)]).await.expect("seed b");
    p.checkpoint_mem_tier().await.expect("checkpoint b");
    settle(p).await;
    let hold = Arc::clone(&p.test_retention_pass_gate).write_owned().await;
    apply(p, &TIER_SEED).await.expect("seed tier");
    assert_tier_seeded(f, "seeded").await;
    hold
}

/// The two layers [`seed_keeping_tier`] built, asserted independently: the
/// tier holds exactly the newer versions of 2 and 3 and their key tombstones,
/// the catalog the older ones, and the scan the merged view.
async fn assert_tier_seeded(f: &Fixture, at: &str) {
    assert_eq!(
        tier_view(&f.provider),
        (TIER_SEED.to_vec(), vec![2, 3]),
        "{}: {at}: tier versions and tombstones",
        f.name
    );
    assert_eq!(
        durable_rows(f).await,
        DURABLE_SEED.to_vec(),
        "{}: {at}: durable",
        f.name
    );
    assert_eq!(
        rows(&f.provider).await,
        SEEDED.to_vec(),
        "{}: {at}: merged view",
        f.name
    );
}

/// The durable write routes that reach an armed `cdc_durability: memory` table
/// besides the CDC apply: a transaction commit and an SQL insert.
#[derive(Clone, Copy, Debug)]
enum DurableRoute {
    Transaction,
    SqlInsert,
}

async fn durable_write(provider: &CayenneTableProvider, route: DurableRoute, rows: &[(i64, i64)]) {
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    let values: Vec<i64> = rows.iter().map(|(_, v)| *v).collect();
    let batch = id_value_batch(schema(), &ids, &values);
    match route {
        DurableRoute::Transaction => {
            let txn = crate::CayenneTransaction::new();
            let token = provider.transaction_write_token().await;
            txn.register(
                provider.table_id().to_string(),
                token,
                provider.clone_for_write_operations(),
            );
            let staged = provider
                .begin_staged_upsert_occ(token, single_batch_stream(batch), 1)
                .await
                .expect("staged");
            txn.set_staged(provider.table_id(), staged);
            txn.commit().await.expect("transaction commit");
        }
        DurableRoute::SqlInsert => insert_batch(provider, batch).await,
    }
}

/// Replay after a crash with tier-resident rows. The newer versions of 2 and 3
/// live only in the tier and were never acknowledged (the last ack saw
/// [`DURABLE_SEED`]); the apply retains against them in memory and writes
/// nothing durable. A crash before the checkpoint loses the tier: the restart
/// reads [`DURABLE_SEED`], the source replays from its ack (the tier seed, then
/// the apply), and the replay converges on the same view, durably.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hybrid_replay_with_tier_resident_rows() {
    for shards in [1_usize, 4] {
        Box::pin(async move {
            let f = hybrid(&format!("hy_replay_tier_{shards}"), shards).await;
            let p = &f.provider;
            let hold = seed_keeping_tier(&f).await;
            assert_eq!(
                f.acks.lock().last().cloned(),
                Some(DURABLE_SEED.to_vec()),
                "shards={shards}: the tier seed was never acknowledged"
            );
            let acked = f.acks.lock().len();
            f.set(ON);
            apply(p, &[(6, 6)]).await.expect("apply");
            assert_eq!(rows(p).await, RETAINED.to_vec(), "shards={shards}: applied");
            assert_eq!(f.acks.lock().len(), acked, "shards={shards}: not acked");
            assert_eq!(
                durable_rows(&f).await,
                DURABLE_SEED.to_vec(),
                "shards={shards}: nothing durable"
            );

            // Crash before the checkpoint: replay from the ack.
            let restarted = f.restart().await;
            assert_eq!(
                rows(&restarted).await,
                DURABLE_SEED.to_vec(),
                "shards={shards}: the durable generation"
            );
            apply(&restarted, &TIER_SEED)
                .await
                .expect("replay tier seed");
            apply(&restarted, &[(6, 6)]).await.expect("replay apply");
            assert_eq!(
                rows(&restarted).await,
                RETAINED.to_vec(),
                "shards={shards}: replayed"
            );
            restarted.checkpoint_mem_tier().await.expect("checkpoint");
            assert_eq!(
                rows(&f.restart().await).await,
                RETAINED.to_vec(),
                "shards={shards}: durable"
            );
            drop(hold);
        })
        .await;
    }
}

/// [`seed_keeping_tier`], then a durable write of `(6, 6)` with retention ON by
/// `route`, still holding the retention pass. Asserts the write acknowledged
/// nothing. Returns the fixture and the hold.
async fn armed_durable_write(
    route: DurableRoute,
    step: &str,
) -> (Fixture, tokio::sync::OwnedRwLockWriteGuard<()>) {
    let f = hybrid(&format!("hyb_{step}_{route:?}").to_lowercase(), 1).await;
    let hold = seed_keeping_tier(&f).await;
    let acks_before = f.acks.lock().len();
    f.set(ON);
    durable_write(&f.provider, route, &[(6, 6)]).await;
    assert_eq!(
        f.acks.lock().len(),
        acks_before,
        "{route:?}: the write acknowledged nothing"
    );
    (f, hold)
}

/// A durable write (transaction or SQL insert) to an armed hybrid table must
/// apply `retention_sql` to the whole logical view it publishes, tier rows
/// included: key 2's tier version (retained -> archived) is retired, key 3's
/// tier version (archived -> retained) stays and its archived durable version
/// neither fails nor resurrects.
async fn assert_armed_published_view(route: DurableRoute) {
    let (f, hold) = armed_durable_write(route, "live").await;
    assert_eq!(
        rows(&f.provider).await,
        RETAINED.to_vec(),
        "{route:?}: published view"
    );
    drop(hold);
}

/// The same write's view after the tier checkpoints and after a restart,
/// checked independently of the live view so a resurrection at either step
/// shows even when the live view is already wrong.
async fn assert_armed_view_after_checkpoint_and_restart(route: DurableRoute) {
    let (f, hold) = armed_durable_write(route, "ckpt").await;
    drop(hold);
    f.provider.checkpoint_mem_tier().await.expect("checkpoint");
    let after_checkpoint = rows(&f.provider).await;
    settle(&f.provider).await;
    let after_restart = rows(&f.restart().await).await;
    assert_eq!(
        (after_checkpoint, after_restart),
        (RETAINED.to_vec(), RETAINED.to_vec()),
        "{route:?}: (after the tier checkpoint, after a restart)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn armed_tier_transaction_publishes_the_whole_logical_view() {
    assert_armed_published_view(DurableRoute::Transaction).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn armed_tier_sql_insert_publishes_the_whole_logical_view() {
    assert_armed_published_view(DurableRoute::SqlInsert).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn armed_tier_transaction_view_survives_checkpoint_and_restart() {
    assert_armed_view_after_checkpoint_and_restart(DurableRoute::Transaction).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn armed_tier_sql_insert_view_survives_checkpoint_and_restart() {
    assert_armed_view_after_checkpoint_and_restart(DurableRoute::SqlInsert).await;
}

/// A user write (no `retention_sql` involved: retention stays OFF) that changes
/// a key whose newest version lives only in the armed RAM tier.
#[derive(Clone, Copy, Debug)]
enum UserWrite {
    /// `DELETE ... WHERE id = 3`.
    SqlDelete,
    /// `UPDATE ... SET value = 30 WHERE id = 3`.
    SqlUpdate,
    /// A transaction upserting `(3, 30)`.
    TransactionUpsert,
}

/// Independent reproducer for the user-write question: does a committed user
/// DELETE/UPDATE of a tier-only version survive a crash before the next
/// checkpoint and the source's replay of every unacknowledged event? The
/// source replays from its last acknowledgement: if the write's own path made
/// the tier durable (and so acknowledged it), nothing is replayed; otherwise
/// the tier seed is re-delivered. Either way the committed write must hold.
async fn assert_user_write_survives_crash_and_replay(write: UserWrite) {
    use datafusion::prelude::lit;
    let f = hybrid(&format!("hyb_user_{write:?}").to_lowercase(), 1).await;
    let hold = seed_keeping_tier(&f).await;
    let acks_before = f.acks.lock().len();
    let p = &f.provider;
    let ctx = SessionContext::new();
    match write {
        UserWrite::SqlDelete => {
            let plan = p
                .delete_from(&ctx.state(), vec![col("id").eq(lit(3_i64))])
                .await
                .expect("delete plan");
            collect(plan, ctx.task_ctx()).await.expect("delete");
        }
        UserWrite::SqlUpdate => {
            let plan = p
                .update(
                    &ctx.state(),
                    vec![("value".to_string(), lit(30_i64))],
                    vec![col("id").eq(lit(3_i64))],
                )
                .await
                .expect("update plan");
            collect(plan, ctx.task_ctx()).await.expect("update");
        }
        UserWrite::TransactionUpsert => {
            durable_write(p, DurableRoute::Transaction, &[(3, 30)]).await;
        }
    }
    let expected: Vec<(i64, i64)> = match write {
        UserWrite::SqlDelete => vec![(1, -1), (2, -2), (4, 4), (5, -5)],
        UserWrite::SqlUpdate | UserWrite::TransactionUpsert => {
            vec![(1, -1), (2, -2), (3, 30), (4, 4), (5, -5)]
        }
    };
    assert_eq!(rows(p).await, expected, "{write:?}: live");
    // Whether the write's path acknowledged the tier seed (made it durable).
    let tier_acked = f.acks.lock().len() > acks_before;
    drop(hold);

    // Crash: no further checkpoint. Replay every unacknowledged event.
    let restarted = f.restart().await;
    if !tier_acked {
        apply(&restarted, &TIER_SEED).await.expect("replay");
    }
    assert_eq!(
        rows(&restarted).await,
        expected,
        "{write:?}: after a crash and the source's replay (tier acked by the write: {tier_acked})"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn user_sql_delete_of_a_tier_only_version_survives_crash_and_replay() {
    assert_user_write_survives_crash_and_replay(UserWrite::SqlDelete).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn user_sql_update_of_a_tier_only_version_survives_crash_and_replay() {
    assert_user_write_survives_crash_and_replay(UserWrite::SqlUpdate).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn user_transaction_upsert_over_a_tier_only_version_survives_crash_and_replay() {
    assert_user_write_survives_crash_and_replay(UserWrite::TransactionUpsert).await;
}
