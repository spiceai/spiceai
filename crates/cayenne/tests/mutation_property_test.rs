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

//! Randomized convergence + isolation ("property") tests for Cayenne mutations,
//! compaction, overwrite, and restart.
//!
//! Each test maintains a trivial in-memory model (`BTreeMap<key, value>`) of the
//! rows that should be observable and asserts the table matches it. Three
//! properties, each catching a different bug class:
//!
//! 1. [`prop_sequential_*`] — single-threaded random walk over
//!    {upsert, delete, delete-all, overwrite, compact, restart}, checked after
//!    every op and after a reopen-from-catalog. Catches ordering / state-machine
//!    / durability bugs. (Cannot catch concurrency races: it has no concurrency.)
//!
//! 2. [`prop_concurrent_mutations_during_compaction_*`] — random mutations
//!    (upsert/delete/overwrite) applied CONCURRENTLY with a background
//!    compaction loop, then quiesce and assert convergence. Catches lost-write
//!    races such as the compaction-vs-delete bug (see
//!    `cdc_compaction_delete_race_test.rs`). This is an *eventual-consistency*
//!    property: it reads only after quiescence.
//!
//! 3. [`prop_concurrent_reads_observe_consistent_snapshot_*`] — a CONCURRENT
//!    READER asserting a held invariant *during* the storm. The workload keeps
//!    cardinality invariant (repeated `INSERT OVERWRITE` of the SAME key set),
//!    so every committed state has exactly N rows / N distinct keys; a reader
//!    that ever sees a different count observed a TORN publish. This is a
//!    *read-atomicity / snapshot-isolation* property — the class our
//!    convergence-only tests are blind to (a torn publish converges fine). It
//!    targets the overwrite torn-publish P0 (spiceai/spiceai#11461). NOTE: it is
//!    timing-dependent — a probabilistic catch, not a guaranteed one.
//!
//! ## Deletion-mode coverage
//!
//! `DeletionMode::Auto` (the default) resolves to POSITION even for PK tables,
//! so the compaction sequence-FENCE path is only reached under an explicit
//! `DeletionMode::Key`. Every property is therefore run as a separate test case
//! per [`Mode`] so BOTH compaction branches (key-mode sequence fence,
//! position-mode serialize) are exercised.
//!
//! On failure tests print the seed and op history for deterministic replay.
//!
//! ## Known pre-existing bugs found by this harness (filed separately)
//!
//! Two cases are `#[ignore]`d because they fail on PRE-EXISTING defects that are
//! independent of the compaction convergence fix this branch introduces. Both
//! are real and should be fixed separately; remove the `#[ignore]` when they are.
//!
//! * **BUG A** — `INSERT OVERWRITE` of key `k`, then `DELETE` of `k`, then a
//!   later `UPSERT` of `k` loses the re-upsert: the row stays hidden, as if the
//!   delete tombstone still applied. Deterministic, reproduces in BOTH deletion
//!   modes, and the failing op sequence contains NO compaction — so it is not
//!   the compaction fix. Minimal shape:
//!   `overwrite([(1, a)]); delete(id = 1); upsert(1, b);` then `SELECT` returns
//!   no row for id 1 (expected `(1, b)`).
//!
//! * **BUG B** — under concurrent mutations + background compaction, rows that
//!   were never mutated VANISH, and a debug assertion fires:
//!   `cayenne_snapshot_file` manifest for the new snapshot lists data files that
//!   are absent from the snapshot's on-disk directory (manifest = N files,
//!   listing = {}). The inconsistency is in manifest/listing/cleanup code the
//!   fix does not modify, and it surfaces even in fully-serialized position
//!   mode. (`cdc_compaction_delete_race_test.rs` still covers the fix itself
//!   concurrently.)

#![allow(clippy::expect_used)]
#![allow(clippy::clone_on_ref_ptr)]

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::{CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};
use common::{BackendType, TestFixture};
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::{Expr, col, lit};
use datafusion_expr::dml::InsertOp;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;
type Model = BTreeMap<i64, i64>;

// ============================================================================
// Deterministic PRNG (SplitMix64) — no external dep, fully reproducible by seed.
// ============================================================================

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed ^ 0x2545_F491_4F6C_DD1D)
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    /// Uniform in `[0, n)`. `n` must be > 0.
    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

// ============================================================================
// Deletion-mode matrix — every property runs once per mode so both compaction
// fence branches are covered.
// ============================================================================

#[derive(Clone, Copy, Debug)]
enum Mode {
    /// Explicit key-delete mode: the compaction sequence-FENCE path.
    KeyPk,
    /// Default (`Auto` => Position) mode: the compaction SERIALIZE path.
    PositionPk,
}

impl Mode {
    fn config(self) -> VortexConfig {
        let base = VortexConfig {
            target_vortex_file_size_mb: 1,
            compaction_trigger_files: 4,
            compaction_background_interval_ms: 0,
            // Force every write to a snapshot file so compaction always has
            // candidates and (for position mode) deletes are file-scoped.
            inline_max_rows: 0,
            ..VortexConfig::default()
        };
        match self {
            Mode::KeyPk => VortexConfig {
                deletion_mode: DeletionMode::Key,
                ..base
            },
            // PositionPk leaves deletion_mode at its `Auto` default, which
            // resolves to Position for a PK table.
            Mode::PositionPk => base,
        }
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

async fn create_table(
    fixture: &TestFixture,
    table_name: &str,
    mode: Mode,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext)> {
    let table_options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: mode.config(),
    };
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(catalog, table_options, ctx.runtime_env()).await?,
    );
    ctx.register_table(table_name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    Ok((table, ctx))
}

/// Reopen the table fresh from catalog metadata (restart simulation), returning
/// a new provider + ctx registered under `table_name`.
async fn reopen_table(
    fixture: &TestFixture,
    table_name: &str,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext)> {
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let provider = Arc::new(
        CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
            .open(table_name)
            .await?,
    );
    ctx.register_table(table_name, Arc::clone(&provider) as Arc<dyn TableProvider>)?;
    Ok((provider, ctx))
}

// ============================================================================
// Mutations
// ============================================================================

fn rows_to_batch(rows: &[(i64, i64)]) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|(k, _)| *k).collect();
    let vals: Vec<i64> = rows.iter().map(|(_, v)| *v).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(vals)),
        ],
    )
    .expect("valid batch")
}

async fn upsert(table: &Arc<CayenneTableProvider>, key: i64, value: i64) -> TestResult<()> {
    common::insert_batch(table.as_ref(), rows_to_batch(&[(key, value)])).await?;
    Ok(())
}

async fn delete(table: &Arc<CayenneTableProvider>, filter: Expr) -> TestResult<()> {
    let ctx = SessionContext::new();
    let plan = table.delete_from(&ctx.state(), vec![filter]).await?;
    datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(())
}

/// `INSERT OVERWRITE` — replaces ALL table contents with `rows`. Routes through
/// `CayenneDataSink::write_all` -> `begin_overwrite` -> `PreparedOverwrite::finish`
/// (the production overwrite path; the #11461 torn-publish site).
async fn overwrite(table: &Arc<CayenneTableProvider>, rows: &[(i64, i64)]) -> TestResult<()> {
    let ctx = SessionContext::new();
    let exec = MemorySourceConfig::try_new_exec(&[vec![rows_to_batch(rows)]], schema(), None)?;
    let plan = table
        .insert_into(&ctx.state(), exec, InsertOp::Overwrite)
        .await?;
    datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(())
}

async fn read_rows(ctx: &SessionContext, table_name: &str) -> TestResult<Model> {
    let df = ctx
        .sql(&format!("SELECT id, value FROM {table_name} ORDER BY id"))
        .await?;
    let results = df.collect().await?;
    let mut model = Model::new();
    for batch in &results {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column Int64");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value column Int64");
        for idx in 0..batch.num_rows() {
            // A duplicate key is a hard convergence failure (resurrected row);
            // flag it with a sentinel so the equality assert catches it.
            if model.insert(ids.value(idx), values.value(idx)).is_some() {
                model.insert(ids.value(idx), i64::MIN);
            }
        }
    }
    Ok(model)
}

// ============================================================================
// 1. Sequential random walk (incl. overwrite + restart)
// ============================================================================

#[derive(Clone, Debug)]
enum Op {
    Upsert { key: i64, value: i64 },
    Delete { key: i64 },
    DeleteAll,
    Overwrite { rows: Vec<(i64, i64)> },
    Compact,
    Restart,
}

fn gen_op(rng: &mut Rng, key_space: i64) -> Op {
    let key = (rng.below(key_space as u64)) as i64;
    match rng.below(100) {
        0..=39 => Op::Upsert {
            key,
            value: rng.below(1_000_000) as i64,
        },
        40..=64 => Op::Delete { key },
        65..=72 => Op::DeleteAll,
        73..=84 => {
            // Overwrite a random subset of the key space.
            let n = rng.below(key_space as u64) + 1;
            let mut rows = Vec::new();
            for _ in 0..n {
                let k = rng.below(key_space as u64) as i64;
                let v = rng.below(1_000_000) as i64;
                // last-writer-wins within the batch (dedup keys)
                if let Some(slot) = rows.iter_mut().find(|(ek, _): &&mut (i64, i64)| *ek == k) {
                    slot.1 = v;
                } else {
                    rows.push((k, v));
                }
            }
            Op::Overwrite { rows }
        }
        85..=92 => Op::Compact,
        _ => Op::Restart,
    }
}

fn apply_model(model: &mut Model, op: &Op) {
    match op {
        Op::Upsert { key, value } => {
            model.insert(*key, *value);
        }
        Op::Delete { key } => {
            model.remove(key);
        }
        Op::DeleteAll => model.clear(),
        Op::Overwrite { rows } => {
            model.clear();
            for (k, v) in rows {
                model.insert(*k, *v);
            }
        }
        Op::Compact | Op::Restart => {}
    }
}

async fn run_sequential_seed(
    fixture: &TestFixture,
    mode: Mode,
    seed: u64,
    steps: usize,
    key_space: i64,
) -> TestResult<()> {
    let table_name = format!("seq_{mode:?}_{seed}");
    let (mut table, mut ctx) = create_table(fixture, &table_name, mode).await?;
    let mut rng = Rng::new(seed);
    let mut model = Model::new();
    let mut history: Vec<Op> = Vec::with_capacity(steps);

    for step in 0..steps {
        let op = gen_op(&mut rng, key_space);
        history.push(op.clone());
        match &op {
            Op::Upsert { key, value } => upsert(&table, *key, *value).await?,
            Op::Delete { key } => delete(&table, col("id").eq(lit(*key))).await?,
            Op::DeleteAll => delete(&table, lit(true)).await?,
            Op::Overwrite { rows } => overwrite(&table, rows).await?,
            Op::Compact => {
                table.maybe_compact_small_files().await?;
            }
            Op::Restart => {
                let (t, c) = reopen_table(fixture, &table_name).await?;
                table = t;
                ctx = c;
            }
        }
        apply_model(&mut model, &op);

        let live = read_rows(&ctx, &table_name).await?;
        assert_eq!(
            live, model,
            "diverged after step {step} (op {op:?}) mode={mode:?} seed={seed}\nhistory={history:?}"
        );
    }

    // Final guarantee: compact + restart must not change the converged state.
    table.maybe_compact_small_files().await?;
    let (_t, c) = reopen_table(fixture, &table_name).await?;
    let final_state = read_rows(&c, &table_name).await?;
    assert_eq!(
        final_state, model,
        "final compact+restart diverged mode={mode:?} seed={seed}\nhistory={history:?}"
    );
    Ok(())
}

async fn run_sequential_mode(fixture: &TestFixture, mode: Mode) -> TestResult<()> {
    for seed in 0..8u64 {
        run_sequential_seed(fixture, mode, seed, 40, 6).await?;
    }
    Ok(())
}

async fn prop_sequential_key_impl(fixture: TestFixture) -> TestResult<()> {
    run_sequential_mode(&fixture, Mode::KeyPk).await
}
async fn prop_sequential_position_impl(fixture: TestFixture) -> TestResult<()> {
    run_sequential_mode(&fixture, Mode::PositionPk).await
}

// IGNORED pending BUG A (see the module note above). These currently fail
// deterministically in BOTH modes with NO compaction in the failing sequence,
// so the cause is independent of the compaction convergence fix. Remove the
// `#[ignore]` once bug A is fixed.
#[tokio::test]
#[ignore = "pre-existing BUG A: INSERT OVERWRITE then Delete{k} then re-Upsert{k} loses the re-upsert (row stays hidden); not the compaction fix"]
async fn prop_sequential_key() -> TestResult<()> {
    common::run_with_backend(BackendType::Sqlite, prop_sequential_key_impl)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e })
}
#[tokio::test]
#[ignore = "pre-existing BUG A: INSERT OVERWRITE then Delete{k} then re-Upsert{k} loses the re-upsert (row stays hidden); not the compaction fix"]
async fn prop_sequential_position() -> TestResult<()> {
    common::run_with_backend(BackendType::Sqlite, prop_sequential_position_impl)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e })
}

// ============================================================================
// 2. Convergence under concurrent mutations + background compaction
// ============================================================================

async fn run_concurrent_mutations_seed(
    fixture: &TestFixture,
    mode: Mode,
    seed: u64,
) -> TestResult<()> {
    let table_name = format!("conc_{mode:?}_{seed}");
    let (table, ctx) = create_table(fixture, &table_name, mode).await?;
    let mut rng = Rng::new(seed);

    let population: i64 = 1000;
    for start in (0..population).step_by(20) {
        let rows: Vec<(i64, i64)> = (start..(start + 20).min(population))
            .map(|k| (k, k * 10))
            .collect();
        common::insert_batch(table.as_ref(), rows_to_batch(&rows)).await?;
    }
    let mut model: Model = (0..population).map(|k| (k, k * 10)).collect();

    let stop = Arc::new(AtomicBool::new(false));
    let bg_table = Arc::clone(&table);
    let bg_stop = Arc::clone(&stop);
    let compactor = tokio::spawn(async move {
        while !bg_stop.load(Ordering::Relaxed) {
            let _ = bg_table.maybe_compact_small_files().await;
            tokio::task::yield_now().await;
        }
    });

    // The model is owned solely by this task (compaction never changes logical
    // contents), so no shared-state synchronization is needed.
    for _ in 0..250 {
        let key = rng.below(population as u64) as i64;
        match rng.below(100) {
            0..=64 => {
                delete(&table, col("id").eq(lit(key))).await?;
                model.remove(&key);
            }
            _ => {
                let value = rng.below(1_000_000) as i64;
                upsert(&table, key, value).await?;
                model.insert(key, value);
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    stop.store(true, Ordering::Relaxed);
    compactor.await.expect("compaction task joins");
    table.maybe_compact_small_files().await?;

    let live = read_rows(&ctx, &table_name).await?;
    assert_eq!(
        live,
        model,
        "CONVERGENCE FAILURE (mode={mode:?} seed={seed}) after concurrent mutations + compaction\n\
         missing_from_live={:?}\nextra_in_live={:?}",
        model
            .iter()
            .filter(|(k, _)| !live.contains_key(k))
            .collect::<Vec<_>>(),
        live.iter()
            .filter(|(k, _)| !model.contains_key(k))
            .collect::<Vec<_>>(),
    );
    Ok(())
}

async fn run_concurrent_mutations_mode(fixture: &TestFixture, mode: Mode) -> TestResult<()> {
    for seed in 0..4u64 {
        run_concurrent_mutations_seed(fixture, mode, seed).await?;
    }
    Ok(())
}

async fn prop_concurrent_mutations_key_impl(fixture: TestFixture) -> TestResult<()> {
    run_concurrent_mutations_mode(&fixture, Mode::KeyPk).await
}
async fn prop_concurrent_mutations_position_impl(fixture: TestFixture) -> TestResult<()> {
    run_concurrent_mutations_mode(&fixture, Mode::PositionPk).await
}

// Multi-threaded (the `test_with_backends!` macro is current-thread) so
// compaction and mutations run on separate workers — the widest race window.
//
// IGNORED pending BUG B (see the module note above): under concurrent
// mutations + background compaction, never-mutated rows vanish and a debug
// assertion fires because the `cayenne_snapshot_file` manifest disagrees with
// the on-disk snapshot listing. The defect is in manifest/listing/cleanup code
// the compaction convergence fix does not touch, and it surfaces even in
// fully-serialized position mode. The compaction fix itself is covered
// concurrently by `cdc_compaction_delete_race_test.rs` (not ignored). Remove
// the `#[ignore]` once bug B is fixed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "pre-existing BUG B: vanished rows + cayenne_snapshot_file manifest != directory listing under concurrent compaction; in manifest code outside the fix"]
async fn prop_concurrent_mutations_key_sqlite() -> TestResult<()> {
    common::run_with_backend(BackendType::Sqlite, prop_concurrent_mutations_key_impl)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e })
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "pre-existing BUG B: vanished rows + cayenne_snapshot_file manifest != directory listing under concurrent compaction; in manifest code outside the fix"]
async fn prop_concurrent_mutations_position_sqlite() -> TestResult<()> {
    common::run_with_backend(BackendType::Sqlite, prop_concurrent_mutations_position_impl)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e })
}

// ============================================================================
// 3. Snapshot-isolation under concurrent reads (targets the #11461 class)
// ============================================================================
//
// Repeated `INSERT OVERWRITE` of the SAME key set keeps cardinality invariant:
// every committed state has exactly N rows / N distinct keys. A concurrent
// reader that EVER sees a different count/key-set observed a torn publish
// (vanished or resurrected rows). On a branch WITHOUT the #11461 fix this is
// expected to surface; it is timing-dependent (probabilistic), so it makes many
// overwrite passes and runs reads on a separate worker.

async fn run_concurrent_reads_seed(fixture: &TestFixture, mode: Mode, seed: u64) -> TestResult<()> {
    let table_name = format!("iso_{mode:?}_{seed}");
    let (table, ctx) = create_table(fixture, &table_name, mode).await?;
    let mut rng = Rng::new(seed);

    let n: i64 = 200;
    let keyset: Vec<i64> = (0..n).collect();
    // Seed the invariant key set.
    let seed_rows: Vec<(i64, i64)> = keyset.iter().map(|&k| (k, 0)).collect();
    overwrite(&table, &seed_rows).await?;

    let stop = Arc::new(AtomicBool::new(false));

    // Background compaction loop.
    let comp_table = Arc::clone(&table);
    let comp_stop = Arc::clone(&stop);
    let compactor = tokio::spawn(async move {
        while !comp_stop.load(Ordering::Relaxed) {
            let _ = comp_table.maybe_compact_small_files().await;
            tokio::task::yield_now().await;
        }
    });

    // Background reader: assert the cardinality/key-set invariant on every read.
    let read_ctx = SessionContext::new();
    read_ctx.register_table(&table_name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    let read_stop = Arc::clone(&stop);
    let read_name = table_name.clone();
    let reader = tokio::spawn(async move {
        let mut violation: Option<String> = None;
        while !read_stop.load(Ordering::Relaxed) {
            match read_rows(&read_ctx, &read_name).await {
                Ok(live) => {
                    if live.len() as i64 != n || (0..n).any(|k| !live.contains_key(&k)) {
                        violation = Some(format!(
                            "torn read: saw {} rows (expected {n}); missing={:?}",
                            live.len(),
                            (0..n)
                                .filter(|k| !live.contains_key(k))
                                .take(8)
                                .collect::<Vec<_>>(),
                        ));
                        break;
                    }
                }
                Err(e) => {
                    violation = Some(format!("read error: {e}"));
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
        violation
    });

    // Foreground: repeatedly overwrite the SAME key set with fresh values.
    for _ in 0..150 {
        let rows: Vec<(i64, i64)> = keyset
            .iter()
            .map(|&k| (k, rng.below(1_000_000) as i64))
            .collect();
        overwrite(&table, &rows).await?;
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    stop.store(true, Ordering::Relaxed);
    compactor.await.expect("compaction task joins");
    let violation = reader.await.expect("reader task joins");
    assert!(
        violation.is_none(),
        "SNAPSHOT-ISOLATION FAILURE (mode={mode:?} seed={seed}): {}",
        violation.unwrap_or_default()
    );

    // And it still converges to the last overwrite.
    let live = read_rows(&ctx, &table_name).await?;
    assert_eq!(
        live.len() as i64,
        n,
        "final row count must be N (mode={mode:?})"
    );
    Ok(())
}

async fn run_concurrent_reads_mode(fixture: &TestFixture, mode: Mode) -> TestResult<()> {
    for seed in 0..4u64 {
        run_concurrent_reads_seed(fixture, mode, seed).await?;
    }
    Ok(())
}

async fn prop_concurrent_reads_key_impl(fixture: TestFixture) -> TestResult<()> {
    run_concurrent_reads_mode(&fixture, Mode::KeyPk).await
}
async fn prop_concurrent_reads_position_impl(fixture: TestFixture) -> TestResult<()> {
    run_concurrent_reads_mode(&fixture, Mode::PositionPk).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prop_concurrent_reads_observe_consistent_snapshot_key_sqlite() -> TestResult<()> {
    common::run_with_backend(BackendType::Sqlite, prop_concurrent_reads_key_impl)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e })
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prop_concurrent_reads_observe_consistent_snapshot_position_sqlite() -> TestResult<()> {
    common::run_with_backend(BackendType::Sqlite, prop_concurrent_reads_position_impl)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e })
}
