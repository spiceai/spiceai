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

//! F2: a commit (multi-table transaction or single-table staged upsert) takes
//! its prerequisites (the mem-tier checkpoint lock, the inline move), then
//! every visibility lock and listing fence, then ONE `now()` for its
//! `retention_sql`, and prepares and publishes under all four guards. Its
//! retention scans lease the held fence: a lease never keeps the fence alive,
//! cannot be pinned once revoked, and a pin still held at the durable point
//! aborts the commit before anything is durable.
//!
//! Deterministic: barriers are seams, held locks and observed queued waiters,
//! never sleeps. Every wait is bounded.

use super::*;
use crate::provider::append_commit::test_seams;
use crate::provider::delete::HeldListingFence;
use crate::provider::transaction::CayenneTransaction;
use arrow::array::TimestampNanosecondArray;
use datafusion::logical_expr::{ColumnarValue, Volatility, create_udf};
use datafusion::prelude::{col, lit, now};

const BOUND: std::time::Duration = std::time::Duration::from_secs(30);

type Seen = Arc<ParkingMutex<Vec<i64>>>;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn wall_nanos() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos(),
    )
    .expect("nanos")
}

/// `retention_sql` = `seen_now(now())`: records every `now()` it is evaluated
/// with and keeps every row.
fn now_recording_retention(seen: &Seen) -> Vec<Expr> {
    let s = Arc::clone(seen);
    let udf = create_udf(
        "seen_now",
        vec![DataType::Timestamp(
            arrow::datatypes::TimeUnit::Nanosecond,
            Some("+00:00".into()),
        )],
        DataType::Boolean,
        Volatility::Volatile,
        Arc::new(move |args: &[ColumnarValue]| {
            let rows = match &args[0] {
                ColumnarValue::Array(a) => a.len(),
                ColumnarValue::Scalar(_) => 1,
            };
            let array = args[0].clone().into_array(rows)?;
            let ts = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("timestamp");
            s.lock().extend(ts.iter().flatten());
            Ok(ColumnarValue::Array(Arc::new(
                arrow::array::BooleanArray::from(vec![Some(false); rows]),
            )))
        }),
    );
    vec![udf.call(vec![now()])]
}

/// Tables sharing ONE metastore database (so they can join one transaction),
/// keyed upsert tables with key deletes, no inlining, the background
/// retention pass held for the fixture's lifetime.
struct Shared {
    catalog: Arc<dyn MetadataCatalog>,
    seen: Seen,
    retention: Vec<Expr>,
    dir: TempDir,
    holds: Vec<tokio::sync::OwnedRwLockWriteGuard<()>>,
}

impl Shared {
    async fn new() -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let metadata_dir = format!("{}/metadata", dir.path().to_str().expect("path"));
        std::fs::create_dir_all(&metadata_dir).expect("metadata dir");
        let catalog = Arc::new(
            crate::CayenneCatalog::new(format!("sqlite://{metadata_dir}/cayenne.db"))
                .expect("catalog"),
        ) as Arc<dyn MetadataCatalog>;
        catalog.init().await.expect("catalog init");
        let seen: Seen = Arc::default();
        let retention = now_recording_retention(&seen);
        Self {
            catalog,
            seen,
            retention,
            dir,
            holds: Vec::new(),
        }
    }

    /// A table seeded with `(1, 1)`; `trigger`: its
    /// `compaction_trigger_protected_snapshots`.
    async fn table(&mut self, name: &str, trigger: Option<usize>) -> CayenneTableProvider {
        let p = CayenneTableProviderBuilder::new(
            Arc::clone(&self.catalog),
            SessionContext::new().runtime_env(),
        )
        .with_retention_filters(self.retention.clone())
        .create(CreateTableOptions {
            table_name: name.to_string(),
            schema: schema(),
            primary_key: vec!["id".to_string()],
            on_conflict: Some(OnConflict::Upsert(
                datafusion_table_providers::util::column_reference::ColumnReference::new(vec![
                    "id".to_string(),
                ]),
            )),
            base_path: format!("{}/data", self.dir.path().to_str().expect("path")),
            partition_column: None,
            vortex_config: VortexConfig {
                compaction_trigger_protected_snapshots: trigger.unwrap_or_else(|| {
                    VortexConfig::default().compaction_trigger_protected_snapshots
                }),
                deletion_mode: crate::metadata::DeletionMode::Key,
                inline_max_rows: 0,
                inline_max_bytes: 0,
                inline_max_buffer_bytes: 0,
                compaction_background_interval_ms: 3_600_000,
                ..VortexConfig::default()
            },
        })
        .await
        .expect("table created");
        self.holds
            .push(Arc::clone(&p.test_retention_pass_gate).write_owned().await);
        insert_batch(&p, id_value_batch(schema(), &[1], &[1])).await;
        p
    }

    async fn reopen(&self, name: &str) -> CayenneTableProvider {
        CayenneTableProviderBuilder::new(
            Arc::clone(&self.catalog),
            SessionContext::new().runtime_env(),
        )
        .with_retention_filters(self.retention.clone())
        .open(name)
        .await
        .expect("reopen")
    }

    fn take_seen(&self) -> Vec<i64> {
        std::mem::take(&mut *self.seen.lock())
    }
}

async fn rows(provider: &CayenneTableProvider) -> Vec<(i64, i64)> {
    let ctx = SessionContext::new();
    let plan = provider
        .scan(&ctx.state(), None, &[], None)
        .await
        .expect("scan plan");
    let batches = collect(plan, ctx.task_ctx()).await.expect("scan");
    let mut out = Vec::new();
    for b in &batches {
        let ids = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id");
        let values = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("v");
        out.extend((0..b.num_rows()).map(|r| (ids.value(r), values.value(r))));
    }
    out.sort_unstable();
    out
}

async fn staged(
    p: &CayenneTableProvider,
    token: crate::provider::staged_upsert::TransactionWriteToken,
    row: (i64, i64),
) -> crate::provider::staged_upsert::CayenneStagedUpsert {
    p.begin_staged_upsert_occ(
        token,
        single_batch_stream(id_value_batch(schema(), &[row.0], &[row.1])),
        1,
    )
    .await
    .expect("staged")
}

/// One transaction writing `(2, 20)` to `a` and `(3, 30)` to `b`, registered
/// in the given order (the commit always runs in canonical `table_id` order).
async fn two_table_txn(
    a: &CayenneTableProvider,
    b: &CayenneTableProvider,
    register_b_first: bool,
) -> CayenneTransaction {
    let txn = CayenneTransaction::new();
    let order: [(&CayenneTableProvider, (i64, i64)); 2] = if register_b_first {
        [(b, (3, 30)), (a, (2, 20))]
    } else {
        [(a, (2, 20)), (b, (3, 30))]
    };
    for (p, row) in order {
        let token = p.transaction_write_token().await;
        txn.register(
            p.table_id().to_string(),
            token,
            p.clone_for_write_operations(),
        );
        txn.set_staged(p.table_id(), staged(p, token, row).await);
    }
    txn
}

/// Wait (bounded) until a writer is queued on `fence`: tokio hands a queued
/// writer every free permit, so `try_read` fails from then on.
async fn until_writer_queued(fence: &tokio::sync::RwLock<()>, what: &str) {
    tokio::time::timeout(BOUND, async {
        while fence.try_read().is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}: a writer queued on the listing fence"));
}

fn assert_all_guards_free(p: &CayenneTableProvider, what: &str) {
    assert!(p.write_lock.try_lock().is_ok(), "{what}: write_lock free");
    assert!(
        p.mem_checkpoint_lock.try_lock().is_ok(),
        "{what}: mem-tier checkpoint lock free"
    );
    assert!(
        p.visibility_lock.try_lock().is_ok(),
        "{what}: visibility lock free"
    );
    assert!(
        p.listing_fence.try_write().is_ok(),
        "{what}: listing fence free"
    );
}

// ---------------------------------------------------------------------------
// Lease lifetime and ownership (no table).
// ---------------------------------------------------------------------------

/// A lease is a weak handle: once the commit drops its held fence, an
/// outstanding lease neither keeps the fence locked nor can pin it.
#[tokio::test]
async fn a_lease_does_not_keep_the_listing_fence_alive() {
    let fence = Arc::new(tokio::sync::RwLock::new(()));
    let held = HeldListingFence::new(Arc::clone(&fence).write_owned().await);
    let lease = held.lease();
    assert!(fence.try_read().is_err(), "the commit holds the fence");
    drop(held);
    assert!(
        fence.try_write().is_ok(),
        "an outstanding lease does not keep the fence locked"
    );
    assert!(lease.pin().is_none(), "nor can it pin a released fence");
}

/// A pin taken before the revoke keeps the fence and fails the pre-durable
/// idle check; after the revoke no new pin can be taken, and once the pin is
/// dropped the check passes.
#[tokio::test]
async fn a_pin_held_at_the_durable_point_fails_the_idle_check_and_none_follow_the_revoke() {
    let fence = Arc::new(tokio::sync::RwLock::new(()));
    let held = HeldListingFence::new(Arc::clone(&fence).write_owned().await);
    let lease = held.lease();
    let pin = lease.pin().expect("a live lease pins before the revoke");
    assert!(
        !held.revoke_leases_and_check_idle(),
        "a running pinned scan fails the idle check"
    );
    assert!(lease.pin().is_none(), "no pin after the revoke");
    drop(held);
    assert!(
        fence.try_read().is_err(),
        "the outstanding pin still owns the guard (the commit aborts, it does not publish)"
    );
    drop(pin);
    assert!(fence.try_write().is_ok(), "released with the last pin");

    let held = HeldListingFence::new(Arc::clone(&fence).write_owned().await);
    let lease = held.lease();
    drop(lease.pin().expect("pin"));
    assert!(
        held.revoke_leases_and_check_idle(),
        "idle once every pin dropped"
    );
    assert!(lease.pin().is_none(), "revoked");
}

/// A held fence recognises only its own lock.
#[tokio::test]
async fn a_held_fence_recognises_only_its_own_lock() {
    let mine = Arc::new(tokio::sync::RwLock::new(()));
    let other = Arc::new(tokio::sync::RwLock::new(()));
    let held = HeldListingFence::new(Arc::clone(&mine).write_owned().await);
    assert!(held.locks(&mine));
    assert!(!held.locks(&other));
}

// ---------------------------------------------------------------------------
// Wrong table, escaped work, cancellation (one or two tables).
// ---------------------------------------------------------------------------

/// Every held-fence entry point refuses another table's fence: the leased
/// sink, the published-runs read, and a staged upsert's guarded prepare (which
/// also removes its staged directory and publishes nothing).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_held_fence_entry_point_refuses_another_tables_fence() {
    let mut shared = Shared::new().await;
    let a = shared.table("f2_wrong_a", None).await;
    let b = shared.table("f2_wrong_b", None).await;
    let fence_b = HeldListingFence::new(b.lock_listing_fence_write_owned().await);
    let filters = vec![col("id").eq(lit(1_i64))];
    assert!(
        a.build_deletion_vector_sink_under_fence(
            &filters,
            super::DeletionRequestSource::User,
            &fence_b
        )
        .await
        .is_err(),
        "a leased sink on another table's fence"
    );
    assert!(
        a.published_runs_under_fence(&fence_b).is_none(),
        "published runs under another table's fence"
    );
    drop(fence_b);

    let token = a.transaction_write_token().await;
    let stage = staged(&a, token, (2, 20)).await;
    let snapshot_dir = a.snapshot_dir_path_for(stage.staged_snapshot_id());
    assert!(snapshot_dir.exists(), "staged directory present");
    let prerequisites = stage
        .acquire_commit_prerequisites()
        .await
        .expect("prerequisites");
    let fence_b = HeldListingFence::new(b.lock_listing_fence_write_owned().await);
    let refused = stage
        .prepare_commit_under_guards(prerequisites, &fence_b, None)
        .await;
    assert!(
        refused.is_err(),
        "a guarded prepare on another table's fence"
    );
    assert!(
        !snapshot_dir.exists(),
        "the refused prepare removed its staged directory"
    );
    drop(fence_b);
    assert_eq!(rows(&a).await, vec![(1, 1)], "nothing published");
    assert_eq!(rows(&shared.reopen("f2_wrong_a").await).await, vec![(1, 1)]);
    assert_all_guards_free(&a, "after the refusal");
}

/// A sink that escapes its commit (kept past the commit's revoke and release)
/// neither keeps the fence locked nor can execute: its pin is refused, so it
/// reads nothing a later publication may retire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_escaped_leased_sink_cannot_execute_after_its_commit_released_the_fence() {
    let mut shared = Shared::new().await;
    let a = shared.table("f2_escaped", None).await;
    let filters = vec![col("id").eq(lit(1_i64))];
    for revoke_first in [true, false] {
        let fence = HeldListingFence::new(a.lock_listing_fence_write_owned().await);
        let sink = Arc::new(
            a.build_deletion_vector_sink_under_fence(
                &filters,
                super::DeletionRequestSource::User,
                &fence,
            )
            .await
            .expect("leased sink"),
        );
        if revoke_first {
            assert!(fence.revoke_leases_and_check_idle(), "nothing pinned");
        }
        drop(fence);
        assert!(
            a.listing_fence.try_write().is_ok(),
            "revoke_first={revoke_first}: the escaped sink does not keep the fence"
        );
        let escaped = Arc::clone(&sink);
        let outcome = tokio::time::timeout(BOUND, async move { escaped.prepare_delete().await })
            .await
            .expect("bounded");
        assert!(
            outcome.is_err(),
            "revoke_first={revoke_first}: an escaped sink cannot execute"
        );
        assert_eq!(rows(&a).await, vec![(1, 1)], "nothing deleted");
    }
    assert_all_guards_free(&a, "after the escaped sinks");
}

/// A caller cancelled while its commit holds every guard (paused after the
/// guarded retention prepare, before the durable point) releases all four
/// guards on every table, publishes nothing, and leaves the tables accepting
/// the next commit. Both commit routes; for the transaction, both
/// registration orders.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_cancelled_under_its_guards_releases_them_and_publishes_nothing() {
    for (route, register_b_first) in [("txn", false), ("txn", true), ("single", false)] {
        let mut shared = Shared::new().await;
        let a = shared
            .table(&format!("f2_cancel_a_{route}_{register_b_first}"), None)
            .await;
        let b = shared
            .table(&format!("f2_cancel_b_{route}_{register_b_first}"), None)
            .await;
        let label = format!("{route} register_b_first={register_b_first}");
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel::<()>();
        // Pause on whichever table the commit prepares (a for single).
        let paused = if route == "single" || a.table_id() < b.table_id() {
            &a
        } else {
            &b
        };
        test_seams::pause_after_commit_time_retention(
            paused.table_id(),
            Box::new(move || {
                Box::pin(async move {
                    let _ = reached_tx.send(());
                    std::future::pending::<()>().await;
                })
            }),
        );
        let commit = if route == "single" {
            let token = a.transaction_write_token().await;
            let stage = staged(&a, token, (2, 20)).await;
            tokio::spawn(async move {
                stage
                    .commit(std::collections::HashSet::new(), true)
                    .await
                    .map(|_| ())
            })
        } else {
            let txn = two_table_txn(&a, &b, register_b_first).await;
            tokio::spawn(async move { txn.commit().await.map(|_| ()) })
        };
        tokio::time::timeout(BOUND, reached_rx)
            .await
            .unwrap_or_else(|_| panic!("{label}: reached the guarded prepare"))
            .expect("seam");
        assert!(
            paused.listing_fence.try_read().is_err(),
            "{label}: the commit holds the fence at the pause"
        );
        commit.abort();
        assert!(
            commit.await.expect_err("aborted").is_cancelled(),
            "{label}: cancelled"
        );
        assert_all_guards_free(&a, &label);
        assert_all_guards_free(&b, &label);
        assert_eq!(rows(&a).await, vec![(1, 1)], "{label}: a unchanged");
        assert_eq!(rows(&b).await, vec![(1, 1)], "{label}: b unchanged");
        // The tables accept the next commit.
        two_table_txn(&a, &b, false)
            .await
            .commit()
            .await
            .unwrap_or_else(|e| panic!("{label}: next commit: {e}"));
        let ra = shared
            .reopen(&format!("f2_cancel_a_{route}_{register_b_first}"))
            .await;
        let rb = shared
            .reopen(&format!("f2_cancel_b_{route}_{register_b_first}"))
            .await;
        assert_eq!(
            rows(&ra).await,
            vec![(1, 1), (2, 20)],
            "{label}: a reopened"
        );
        assert_eq!(
            rows(&rb).await,
            vec![(1, 1), (3, 30)],
            "{label}: b reopened"
        );
    }
}

// ---------------------------------------------------------------------------
// One `now()` after every wait; opposite registration orders; exact rows and
// reopen.
// ---------------------------------------------------------------------------

/// The checkpoint-lock wait on the participant the commit prepares LAST (in
/// canonical order), for both registration orders: every table is evaluated
/// with ONE `now()`, taken after that wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_table_commit_takes_one_now_after_every_participants_checkpoint_wait() {
    for register_b_first in [false, true] {
        let mut shared = Shared::new().await;
        let a = shared
            .table(&format!("f2_ckpt_a_{register_b_first}"), None)
            .await;
        let b = shared
            .table(&format!("f2_ckpt_b_{register_b_first}"), None)
            .await;
        let later = if a.table_id() > b.table_id() { &a } else { &b };
        let txn = two_table_txn(&a, &b, register_b_first).await;
        shared.take_seen();

        let hold = later.lock_mem_tier_checkpoint_for_durable_write().await;
        let (reached_tx, reached_rx) = tokio::sync::oneshot::channel::<()>();
        test_seams::pause_at_checkpoint_wait(
            later.table_id(),
            Box::new(move || Box::pin(async move { reached_tx.send(()).expect("test waits") })),
        );
        let commit = tokio::spawn(async move { txn.commit().await.map(|_| ()) });
        tokio::time::timeout(BOUND, reached_rx)
            .await
            .expect("reached the later checkpoint wait")
            .expect("seam");
        assert!(
            !commit.is_finished(),
            "the commit waits on the held checkpoint lock"
        );
        let released_at = wall_nanos();
        drop(hold);
        tokio::time::timeout(BOUND, commit)
            .await
            .expect("bounded")
            .expect("join")
            .expect("commit");

        let seen = shared.take_seen();
        assert!(!seen.is_empty(), "retention was evaluated");
        let distinct: std::collections::BTreeSet<i64> = seen.iter().copied().collect();
        assert_eq!(
            distinct.len(),
            1,
            "one now() for the publication: {distinct:?}"
        );
        assert!(
            seen.iter().all(|&t| t >= released_at),
            "register_b_first={register_b_first}: now() {seen:?} predates the wait {released_at}"
        );
        assert_eq!(rows(&a).await, vec![(1, 1), (2, 20)]);
        assert_eq!(rows(&b).await, vec![(1, 1), (3, 30)]);
        let ra = shared
            .reopen(&format!("f2_ckpt_a_{register_b_first}"))
            .await;
        let rb = shared
            .reopen(&format!("f2_ckpt_b_{register_b_first}"))
            .await;
        assert_eq!(rows(&ra).await, vec![(1, 1), (2, 20)], "a reopened");
        assert_eq!(rows(&rb).await, vec![(1, 1), (3, 30)], "b reopened");
    }
}

/// A reader holding one participant's listing fence (each participant in
/// turn, both registration orders): the commit queues at its fence step, and
/// its one `now()` follows the reader's release.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_table_commit_takes_now_after_its_listing_fence_waits() {
    for register_b_first in [false, true] {
        for reader_on_a in [true, false] {
            let mut shared = Shared::new().await;
            let tag = format!("{register_b_first}_{reader_on_a}");
            let a = shared.table(&format!("f2_fence_a_{tag}"), None).await;
            let b = shared.table(&format!("f2_fence_b_{tag}"), None).await;
            let txn = two_table_txn(&a, &b, register_b_first).await;
            shared.take_seen();
            let blocked = if reader_on_a { &a } else { &b };
            let reader = Arc::clone(&blocked.listing_fence).read_owned().await;
            let commit = tokio::spawn(async move { txn.commit().await.map(|_| ()) });
            until_writer_queued(&blocked.listing_fence, &tag).await;
            assert!(
                !commit.is_finished(),
                "{tag}: the commit waits on the reader"
            );
            assert!(
                shared.take_seen().is_empty(),
                "{tag}: nothing evaluated before the wait"
            );
            let released_at = wall_nanos();
            drop(reader);
            tokio::time::timeout(BOUND, commit)
                .await
                .expect("bounded")
                .expect("join")
                .expect("commit");
            let seen = shared.take_seen();
            assert!(!seen.is_empty(), "{tag}: retention was evaluated");
            let distinct: std::collections::BTreeSet<i64> = seen.iter().copied().collect();
            assert_eq!(distinct.len(), 1, "{tag}: one now(): {distinct:?}");
            assert!(
                seen.iter().all(|&t| t >= released_at),
                "{tag}: now() {seen:?} predates the fence wait {released_at}"
            );
            assert_eq!(rows(&a).await, vec![(1, 1), (2, 20)]);
            assert_eq!(rows(&b).await, vec![(1, 1), (3, 30)]);
            let ra = shared.reopen(&format!("f2_fence_a_{tag}")).await;
            let rb = shared.reopen(&format!("f2_fence_b_{tag}")).await;
            assert_eq!(rows(&ra).await, vec![(1, 1), (2, 20)], "{tag}: a reopened");
            assert_eq!(rows(&rb).await, vec![(1, 1), (3, 30)], "{tag}: b reopened");
        }
    }
}

/// The single-table staged-upsert commit keeps the same contract: its
/// `now()` follows its listing-fence wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_table_commit_takes_now_after_its_listing_fence_wait() {
    let mut shared = Shared::new().await;
    let a = shared.table("f2_single_fence", None).await;
    let token = a.transaction_write_token().await;
    let stage = staged(&a, token, (2, 20)).await;
    shared.take_seen();
    let reader = Arc::clone(&a.listing_fence).read_owned().await;
    let commit = tokio::spawn(async move {
        stage
            .commit(std::collections::HashSet::new(), true)
            .await
            .map(|_| ())
    });
    until_writer_queued(&a.listing_fence, "single").await;
    assert!(!commit.is_finished(), "the commit waits on the reader");
    let released_at = wall_nanos();
    drop(reader);
    tokio::time::timeout(BOUND, commit)
        .await
        .expect("bounded")
        .expect("join")
        .expect("commit");
    let seen = shared.take_seen();
    assert!(!seen.is_empty(), "retention was evaluated");
    assert!(
        seen.iter().all(|&t| t >= released_at),
        "now() {seen:?} predates the fence wait {released_at}"
    );
    assert_eq!(rows(&a).await, vec![(1, 1), (2, 20)]);
    assert_eq!(
        rows(&shared.reopen("f2_single_fence").await).await,
        vec![(1, 1), (2, 20)]
    );
}

/// A protected-run compaction of one participant, started while the
/// transaction holds every guard, flips only after the publication, for both
/// registration orders and whichever participant compacts: exact rows on both
/// tables, live and reopened.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_compaction_started_under_a_multi_table_commit_flips_after_it() {
    for register_b_first in [false, true] {
        for compact_a in [true, false] {
            let mut shared = Shared::new().await;
            let tag = format!("{register_b_first}_{compact_a}");
            let a = shared.table(&format!("f2_compact_a_{tag}"), Some(2)).await;
            let b = shared.table(&format!("f2_compact_b_{tag}"), Some(2)).await;
            let target = if compact_a { &a } else { &b };
            // Two more protected runs on the target, with no merge yet.
            let held = target.hold_compaction().await;
            for row in [(5_i64, 5_i64), (6, 6)] {
                let token = target.transaction_write_token().await;
                staged(target, token, row)
                    .await
                    .commit(std::collections::HashSet::new(), true)
                    .await
                    .expect("seed");
            }
            while target.post_write_compaction_state.load(Ordering::Acquire) != COALESCED_TASK_IDLE
            {
                tokio::task::yield_now().await;
            }
            drop(held);
            let probe = target.clone_for_write();
            let compaction: Arc<ParkingMutex<Option<tokio::task::JoinHandle<String>>>> =
                Arc::default();
            let spawned = Arc::clone(&compaction);
            test_seams::pause_after_commit_time_retention(
                target.table_id(),
                Box::new(move || {
                    Box::pin(async move {
                        assert!(probe.listing_fence.try_read().is_err(), "fence held");
                        *spawned.lock() = Some(tokio::spawn(async move {
                            let merged = probe.compact_protected_snapshots_subset(usize::MAX).await;
                            format!("{merged:?}")
                        }));
                    })
                }),
            );
            let txn = two_table_txn(&a, &b, register_b_first).await;
            tokio::time::timeout(BOUND, txn.commit())
                .await
                .unwrap_or_else(|_| panic!("{tag}: commit bounded"))
                .expect("commit");
            let handle = compaction
                .lock()
                .take()
                .expect("the compaction was started");
            let outcome = tokio::time::timeout(BOUND, handle)
                .await
                .expect("compaction bounded")
                .expect("join");
            assert!(outcome.starts_with("Ok("), "{tag}: compaction {outcome}");
            let mut want_target = vec![(1, 1), (5, 5), (6, 6)];
            let (want_a, want_b) = if compact_a {
                want_target.push((2, 20));
                want_target.sort_unstable();
                (want_target, vec![(1, 1), (3, 30)])
            } else {
                want_target.push((3, 30));
                want_target.sort_unstable();
                (vec![(1, 1), (2, 20)], want_target)
            };
            assert_eq!(rows(&a).await, want_a, "{tag}: a live");
            assert_eq!(rows(&b).await, want_b, "{tag}: b live");
            let ra = shared.reopen(&format!("f2_compact_a_{tag}")).await;
            let rb = shared.reopen(&format!("f2_compact_b_{tag}")).await;
            assert_eq!(rows(&ra).await, want_a, "{tag}: a reopened");
            assert_eq!(rows(&rb).await, want_b, "{tag}: b reopened");
        }
    }
}
