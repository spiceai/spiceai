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

//! Randomized convergence ("property") tests for Cayenne mutations + compaction.
//!
//! These tests run random histories of insert/upsert/delete/compact against a
//! Cayenne table while maintaining a trivial in-memory model (`BTreeMap`) of the
//! rows that should be observable, then assert the table's live result set
//! always equals the model — i.e. the table *converges* to the expected state.
//!
//! Two distinct shapes, because they catch different bug classes:
//!
//! 1. [`prop_sequential_*`] — a single-threaded random walk over
//!    {upsert, delete, delete-all, compact}. This is the harness the
//!    investigation brief asked for. It is strong at finding *ordering* /
//!    *state-machine* bugs: protected-snapshot fences applied at the wrong
//!    sequence, position-capture staleness, deletion-index reload errors,
//!    upsert re-insertion vs tombstone ordering. NOTE: because compaction is
//!    driven *between* mutations (never concurrently with them), this shape
//!    canNOT reproduce a compaction-vs-delete data race — sequential
//!    compaction always materializes the deletions that exist at the moment it
//!    runs.
//!
//! 2. [`prop_concurrent_mutations_during_compaction`] — a BROAD property: a
//!    random stream of mutations (deletes + upserts) applied *concurrently*
//!    with a background compaction loop must still converge to the model. It
//!    does not hard-code any one failure mode; it asserts the invariant and
//!    lets concurrency defects surface. The audited compaction-vs-delete race
//!    is the first defect it catches: the full-snapshot rewrite path
//!    (`rewrite_current_snapshot_for_compaction`) snapshots the visible row
//!    stream, then at the end unconditionally calls `clear_all_deletion_caches()`
//!    (table.rs) with NO sequence fence, while `compaction_lock` and
//!    `write_lock` are distinct mutexes — so a delete that lands after the
//!    visible stream is captured but before the cache clear is dropped,
//!    resurrecting the row. (The *narrow*, deterministic-ish reproduction of
//!    exactly that race lives in `cdc_compaction_delete_race_test.rs`.)
//!
//! On failure both tests print the seed and the full operation history so the
//! exact sequence can be replayed deterministically.
//!
//! ## Compaction-kind coverage (KNOWN GAP — follow-up)
//!
//! Cayenne has several distinct compaction paths with *different* deletion
//! carry-forward logic, and these tests currently only drive ONE of them:
//!
//! * full-snapshot rewrite — via `maybe_compact_small_files()` (what we drive;
//!   the buggy unconditional `clear_all_deletion_caches()` path), incl. the
//!   sort-rewrite sub-variant when `sort_columns` is configured;
//! * protected-snapshot subset merge — `compact_protected_snapshots_subset()`
//!   (sequence-fenced; serializes position deletes — the *safe* pattern);
//! * seq-prefix bake — background-only (`CompactionRunner`), key-mode, prunes
//!   the deletion index;
//! * memtable/inline checkpoint flushes — `checkpoint_inlined_data()` /
//!   `checkpoint_mem_tier()`.
//!
//! Some of these are *runtime-selectable actions* (the `Compact` op should
//! eventually randomize among `maybe_compact_small_files`,
//! `compact_protected_snapshots_subset`, and a checkpoint), while others are
//! *table-config knobs* (`deletion_mode` key/position, `sort_columns`, trigger
//! thresholds) that belong in separate config *variants* of this harness. See
//! the conversation notes; expanding coverage here is a planned follow-up once
//! the first race is fixed.

#![allow(clippy::expect_used)]
#![allow(clippy::clone_on_ref_ptr)]

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{
    CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog,
};
use common::{BackendType, TestFixture};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::{Expr, col, lit};
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
        // Avoid the degenerate all-zero state.
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
// Operation model
// ============================================================================

#[derive(Clone, Debug)]
enum Op {
    Upsert { key: i64, value: i64 },
    Delete { key: i64 },
    DeleteAll,
    Compact,
}

/// Bias toward upserts/deletes so the table actually accumulates state and
/// compaction has something to consolidate; `DeleteAll` and `Compact` are
/// rarer punctuation.
fn gen_op(rng: &mut Rng, key_space: i64) -> Op {
    #[expect(clippy::cast_sign_loss, reason = "key_space is a small positive test constant")]
    let key = rng.below(key_space as u64) as i64;
    match rng.below(100) {
        0..=44 => Op::Upsert {
            key,
            value: rng.below(1_000_000) as i64,
        },
        45..=79 => Op::Delete { key },
        80..=84 => Op::DeleteAll,
        _ => Op::Compact,
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
        Op::Compact => {}
    }
}

// ============================================================================
// Table setup / IO helpers
// ============================================================================

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// Aggressive compaction config so a handful of writes is enough to trigger a
/// real rewrite. `inline_max_rows: 0` forces every write to land as a snapshot
/// file (bypassing the inline memtable) so compaction always has candidates.
/// The background scheduler is disabled; the tests drive compaction explicitly.
fn config() -> VortexConfig {
    VortexConfig {
        target_vortex_file_size_mb: 1,
        compaction_trigger_files: 4,
        compaction_background_interval_ms: 0,
        inline_max_rows: 0,
        ..VortexConfig::default()
    }
}

async fn create_table(
    fixture: &TestFixture,
    table_name: &str,
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
        vortex_config: config(),
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

async fn upsert(table: &Arc<CayenneTableProvider>, key: i64, value: i64) -> TestResult<()> {
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(vec![key])),
            Arc::new(Int64Array::from(vec![value])),
        ],
    )?;
    common::insert_batch(table.as_ref(), batch).await?;
    Ok(())
}

async fn delete(table: &Arc<CayenneTableProvider>, filter: Expr) -> TestResult<u64> {
    let ctx = SessionContext::new();
    let plan = table.delete_from(&ctx.state(), vec![filter]).await?;
    let results = datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(results
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<arrow::array::UInt64Array>())
        .and_then(|a| a.values().first())
        .copied()
        .unwrap_or(0))
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
            .expect("id column should be Int64");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value column should be Int64");
        for idx in 0..batch.num_rows() {
            // A second occurrence of a key is a hard convergence failure
            // (resurrected/duplicate row); record it as a sentinel collision so
            // the assert below catches it rather than silently overwriting.
            if model.insert(ids.value(idx), values.value(idx)).is_some() {
                model.insert(ids.value(idx), i64::MIN);
            }
        }
    }
    Ok(model)
}

/// Reopen the table fresh from catalog metadata (simulates a process restart)
/// and read it back — proves convergence is durable, not just an in-memory
/// cache artifact.
async fn reopen_and_read(fixture: &TestFixture, table_name: &str) -> TestResult<Model> {
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let provider = CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
        .open(table_name)
        .await?;
    ctx.register_table(table_name, Arc::new(provider) as Arc<dyn TableProvider>)?;
    read_rows(&ctx, table_name).await
}

// ============================================================================
// 1. Sequential random-walk convergence
// ============================================================================

async fn run_sequential_seed(
    fixture: &TestFixture,
    seed: u64,
    steps: usize,
    key_space: i64,
) -> TestResult<()> {
    let table_name = format!("seq_{seed}");
    let (table, ctx) = create_table(fixture, &table_name).await?;
    let mut rng = Rng::new(seed);
    let mut model = Model::new();
    let mut history: Vec<Op> = Vec::with_capacity(steps);

    for step in 0..steps {
        let op = gen_op(&mut rng, key_space);
        history.push(op.clone());

        match &op {
            Op::Upsert { key, value } => upsert(&table, *key, *value).await?,
            Op::Delete { key } => {
                delete(&table, col("id").eq(lit(*key))).await?;
            }
            Op::DeleteAll => {
                delete(&table, lit(true)).await?;
            }
            Op::Compact => {
                table.maybe_compact_small_files().await?;
            }
        }
        apply_model(&mut model, &op);

        let live = read_rows(&ctx, &table_name).await?;
        assert_eq!(
            live, model,
            "live state diverged after step {step} (op {op:?})\n\
             seed={seed} key_space={key_space}\n\
             history={history:?}"
        );
    }

    // Final convergence guarantee: one last compaction must not resurrect
    // anything, and the state must survive a reopen from catalog.
    table.maybe_compact_small_files().await?;
    let after_compact = read_rows(&ctx, &table_name).await?;
    assert_eq!(
        after_compact, model,
        "final compaction diverged\nseed={seed}\nhistory={history:?}"
    );
    let reopened = reopen_and_read(fixture, &table_name).await?;
    assert_eq!(
        reopened, model,
        "reopened state diverged\nseed={seed}\nhistory={history:?}"
    );
    Ok(())
}

async fn prop_sequential_impl(fixture: TestFixture) -> TestResult<()> {
    // 20 seeds × 50 steps over an 8-key space keeps overlap high (lots of
    // upsert-over / delete-then-reinsert) while staying within a reasonable
    // wall-clock budget for an integration test. This shape is expected to
    // PASS: sequential compaction always materializes the deletions that exist
    // when it runs, so it cannot reproduce the compaction-vs-delete race — its
    // value is guarding ordering / state-machine / reopen-durability paths.
    for seed in 0..20u64 {
        run_sequential_seed(&fixture, seed, 50, 8).await?;
    }
    Ok(())
}

test_with_backends!(prop_sequential_impl);

// ============================================================================
// 2. Convergence under concurrent mutations + background compaction
// ============================================================================
//
// This is a BROAD property: "a random stream of mutations (deletes + upserts)
// applied while compaction runs concurrently must still converge to the model."
// It is NOT narrowly testing "a delete during a rewrite" — it asserts the
// general invariant, and the audited compaction-vs-delete race is just the
// first way that invariant is observed to break. The narrow, named reproduction
// of the specific race lives in `cdc_compaction_delete_race_test.rs`. Keeping
// this one broad is deliberate: a property test should assert the property and
// let any concurrency defect surface, not hard-code the one we already found.

async fn run_concurrent_mutations_seed(fixture: &TestFixture, seed: u64) -> TestResult<()> {
    let table_name = format!("conc_{seed}");
    let (table, ctx) = create_table(fixture, &table_name).await?;
    let mut rng = Rng::new(seed);

    // Seed a working set across many files so a full-snapshot rewrite takes
    // long enough to overlap with the mutation stream.
    let population: i64 = 1000;
    for chunk_start in (0..population).step_by(20) {
        let ids: Vec<i64> = (chunk_start..(chunk_start + 20).min(population)).collect();
        let values: Vec<i64> = ids.iter().map(|k| k * 10).collect();
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )?;
        common::insert_batch(table.as_ref(), batch).await?;
    }
    let mut model: Model = (0..population).map(|k| (k, k * 10)).collect();

    // Background compaction loop — hammers the full-rewrite path while the
    // foreground issues deletes.
    let stop = Arc::new(AtomicBool::new(false));
    let bg_table = Arc::clone(&table);
    let bg_stop = Arc::clone(&stop);
    let compactor = tokio::spawn(async move {
        while !bg_stop.load(Ordering::Relaxed) {
            // Ignore errors here; convergence is asserted after quiesce.
            let _ = bg_table.maybe_compact_small_files().await;
            tokio::task::yield_now().await;
        }
    });

    // Foreground: a stream of random deletes and a few re-upserts. The model is
    // owned solely by this task (compaction never changes logical contents), so
    // no shared-state synchronization is needed.
    for _ in 0..250 {
        let key = rng.below(population as u64) as i64;
        if rng.below(100) < 75 {
            delete(&table, col("id").eq(lit(key))).await?;
            model.remove(&key);
        } else {
            let value = rng.below(1_000_000) as i64;
            upsert(&table, key, value).await?;
            model.insert(key, value);
        }
        // A brief pause (rather than a bare yield) so mutations are genuinely
        // interleaved with the background rewrite passes instead of draining
        // ahead of them.
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    // Quiesce: stop the background compactor, then run one more compaction to
    // ensure the final state is fully materialized.
    stop.store(true, Ordering::Relaxed);
    compactor.await.expect("compaction task joins cleanly");
    table.maybe_compact_small_files().await?;

    let live = read_rows(&ctx, &table_name).await?;
    assert_eq!(
        live, model,
        "CONVERGENCE FAILURE: live state != model after concurrent \
         deletes + compaction (resurrected/lost rows). seed={seed}\n\
         missing_from_live={:?}\nextra_in_live={:?}",
        model
            .iter()
            .filter(|(k, _)| !live.contains_key(k))
            .collect::<Vec<_>>(),
        live.iter()
            .filter(|(k, _)| !model.contains_key(k))
            .collect::<Vec<_>>(),
    );

    let reopened = reopen_and_read(fixture, &table_name).await?;
    assert_eq!(
        reopened, model,
        "CONVERGENCE FAILURE after reopen. seed={seed}"
    );
    Ok(())
}

async fn prop_concurrent_mutations_during_compaction_impl(fixture: TestFixture) -> TestResult<()> {
    for seed in 0..4u64 {
        run_concurrent_mutations_seed(&fixture, seed).await?;
    }
    Ok(())
}

// NOTE: this variant uses an explicit multi-threaded runtime (not the
// `test_with_backends!` macro, which expands to a current-thread `#[tokio::test]`)
// so compaction and the delete stream run on separate worker threads — true
// parallelism, the widest race window. This is the shape that catches the
// compaction-vs-delete convergence bug.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prop_concurrent_mutations_during_compaction_sqlite() -> TestResult<()> {
    common::run_with_backend(
        BackendType::Sqlite,
        prop_concurrent_mutations_during_compaction_impl,
    )
    .await
    .map_err(|e| -> Box<dyn std::error::Error> { e })
}

#[cfg(feature = "turso")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prop_concurrent_mutations_during_compaction_turso() -> TestResult<()> {
    common::run_with_backend(
        BackendType::Turso,
        prop_concurrent_mutations_during_compaction_impl,
    )
    .await
    .map_err(|e| -> Box<dyn std::error::Error> { e })
}
