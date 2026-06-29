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

//! Randomized fuzz (property-style) convergence + isolation tests for Cayenne
//! mutations, compaction, overwrite, and restart. Each test drives a randomized
//! operation sequence and asserts the table converges to an in-memory model —
//! i.e. it fuzzes the mutation/compaction state machine for data-loss,
//! resurrection, and torn-read defects.
//!
//! Each convergence test is one parametrized harness ([`Workload`]) run with a
//! different config — controlling deletion mode, sequential vs concurrent,
//! per-operation weights (so e.g. "no deletions" or "fewer compactions" is just
//! a weight change), batch size, op count, and the upsert conflict-detection
//! path (exact PK index vs over-budget bloom existence filter via
//! `pk_keyset_cache_mb`). All share one in-memory model (`BTreeMap<key,value>`)
//! and one convergence check that reports `missing` (loss), `extra`
//! (resurrection), and `wrong_value` separately. Beyond the row-set compare,
//! each settled state is also cross-checked through aggregate/filter/point
//! queries (`COUNT(*)`, `SUM(value)`, `WHERE value >= …`, `WHERE id = …`) so
//! defects that the deduplicated id→value map misses (phantom/duplicate rows,
//! pushdown bugs) are caught. The concurrent configs also fuzz mid-stream
//! restarts (reopen-from-catalog under a lock) racing the background compactor.
//!
//! Coverage is env-scalable for CI without code changes (see `env_scale`):
//! `CAYENNE_PROPTEST_SCALE` multiplies the seed count of every config, and
//! `CAYENNE_PROPTEST_OPS_SCALE` multiplies the per-seed op count. Both default
//! to 1 (a fast local run).
//!
//! All configs currently converge — the convergence/resurrection defects this
//! harness surfaced are fixed (see the PR description for the linked fixes). If
//! a future config exposes a new defect, `#[ignore]` it with a description of
//! the defect (not a label) until it is fixed.

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
// Deterministic PRNG (SplitMix64)
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
    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n.max(1)
    }
}

// ============================================================================
// Workload configuration
// ============================================================================

#[derive(Clone, Copy, Debug)]
enum Mode {
    /// Explicit `deletion_mode: key` — the deletion index is authoritative.
    Key,
    /// Default (`auto` resolves to position even for PK tables).
    Position,
}

#[derive(Clone, Copy, Debug)]
enum Concurrency {
    /// Single-threaded random walk; model-checked after every op and after a
    /// reopen-from-catalog at the end.
    Sequential,
    /// A foreground mutation stream concurrent with a background compaction loop;
    /// convergence checked once after quiesce + a final compaction.
    ConcurrentWithCompaction,
}

/// Relative weights for the random op generator. Set a weight to 0 to exclude
/// that op (e.g. `delete: 0` for "no deletions"). `compact` applies only to
/// [`Concurrency::Sequential`] (the concurrent path drives compaction from its
/// background loop, so a foreground `compact` is a no-op there). `restart`
/// applies to BOTH: sequential reopens inline; concurrent reopens under a lock
/// that quiesces the background compactor first.
#[derive(Clone, Copy)]
struct OpWeights {
    upsert: u32,
    delete: u32,
    delete_all: u32,
    overwrite: u32,
    compact: u32,
    restart: u32,
}

#[derive(Clone, Copy)]
struct Workload {
    mode: Mode,
    concurrency: Concurrency,
    weights: OpWeights,
    /// Initial seeded rows (and the key-space upper bound for random keys).
    population: i64,
    /// Rows written per upsert/overwrite op (1 = single-row; larger = batches).
    batch_size: i64,
    /// Number of foreground ops.
    ops: usize,
    /// Seeds to run.
    seeds: u64,
    /// `pk_keyset_cache_mb` for the table: `None` = default exact PK index,
    /// `Some(0)` = force the over-budget bloom existence-filter conflict path.
    pk_keyset_cache_mb: Option<usize>,
}

// ============================================================================
// Table + IO helpers
// ============================================================================

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn config(mode: Mode, pk_keyset_cache_mb: Option<usize>) -> VortexConfig {
    let base = VortexConfig {
        target_vortex_file_size_mb: 1,
        compaction_trigger_files: 4,
        compaction_background_interval_ms: 0,
        inline_max_rows: 0,
        // `Some(0)` forces the over-budget bloom existence-filter path for upsert
        // conflict detection (instead of the exact PK keyset); `None` keeps the
        // default exact index. Lets one harness fuzz both existence paths.
        pk_keyset_cache_mb,
        ..VortexConfig::default()
    };
    match mode {
        Mode::Key => VortexConfig {
            deletion_mode: DeletionMode::Key,
            ..base
        },
        Mode::Position => base,
    }
}

async fn create_table(
    fixture: &TestFixture,
    name: &str,
    mode: Mode,
    pk_keyset_cache_mb: Option<usize>,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext)> {
    let opts = CreateTableOptions {
        table_name: name.to_string(),
        schema: schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: config(mode, pk_keyset_cache_mb),
    };
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table =
        Arc::new(CayenneTableProvider::create_table(catalog, opts, ctx.runtime_env()).await?);
    ctx.register_table(name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    Ok((table, ctx))
}

async fn reopen_table(
    fixture: &TestFixture,
    name: &str,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext)> {
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let provider = Arc::new(
        CayenneTableProviderBuilder::new(catalog, ctx.runtime_env())
            .open(name)
            .await?,
    );
    ctx.register_table(name, Arc::clone(&provider) as Arc<dyn TableProvider>)?;
    Ok((provider, ctx))
}

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

async fn upsert(table: &Arc<CayenneTableProvider>, rows: &[(i64, i64)]) -> TestResult<()> {
    common::insert_batch(table.as_ref(), rows_to_batch(rows)).await?;
    Ok(())
}

async fn delete(table: &Arc<CayenneTableProvider>, filter: Expr) -> TestResult<()> {
    let ctx = SessionContext::new();
    let plan = table.delete_from(&ctx.state(), vec![filter]).await?;
    datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(())
}

async fn overwrite(table: &Arc<CayenneTableProvider>, rows: &[(i64, i64)]) -> TestResult<()> {
    let ctx = SessionContext::new();
    let exec = MemorySourceConfig::try_new_exec(&[vec![rows_to_batch(rows)]], schema(), None)?;
    let plan = table
        .insert_into(&ctx.state(), exec, InsertOp::Overwrite)
        .await?;
    datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(())
}

async fn read_rows(ctx: &SessionContext, name: &str) -> TestResult<Model> {
    let df = ctx
        .sql(&format!("SELECT id, value FROM {name} ORDER BY id"))
        .await?;
    let mut model = Model::new();
    for b in &df.collect().await? {
        let ids = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id");
        let vals = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value");
        for i in 0..b.num_rows() {
            if model.insert(ids.value(i), vals.value(i)).is_some() {
                model.insert(ids.value(i), i64::MIN); // duplicate sentinel
            }
        }
    }
    Ok(model)
}

fn assert_converged(live: &Model, model: &Model, ctx_msg: &str) {
    let missing: Vec<(i64, i64)> = model
        .iter()
        .filter(|(k, _)| !live.contains_key(k))
        .map(|(&k, &v)| (k, v))
        .collect();
    let extra: Vec<(i64, i64)> = live
        .iter()
        .filter(|(k, _)| !model.contains_key(k))
        .map(|(&k, &v)| (k, v))
        .collect();
    let wrong: Vec<(i64, i64, i64)> = model
        .iter()
        .filter_map(|(&k, &v)| live.get(&k).filter(|&&lv| lv != v).map(|&lv| (k, v, lv)))
        .collect();
    assert!(
        missing.is_empty() && extra.is_empty() && wrong.is_empty(),
        "{ctx_msg}\nmissing(loss)={missing:?}\nextra(resurrect)={extra:?}\nwrong_value(k,expected,got)={wrong:?}"
    );
}

/// Run a query expected to return a single `i64` scalar (a NULL aggregate maps
/// to 0). Used by the aggregate-query checks below.
async fn scalar_i64(ctx: &SessionContext, sql: &str) -> TestResult<i64> {
    // Callers use COUNT(*) (never null) or COALESCE(SUM(...), 0), so the single
    // result cell is always a non-null i64.
    let batches = ctx.sql(sql).await?.collect().await?;
    for b in &batches {
        if b.num_rows() > 0 {
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("i64 scalar result");
            return Ok(arr.value(0));
        }
    }
    Ok(0)
}

/// Cross-check the table against the model through AGGREGATE + FILTER + POINT
/// queries, not just the `SELECT id, value` row scan. These exercise different
/// execution paths (aggregation, filter pushdown, PK point-lookup pushdown over
/// the merge-on-read deletion filter), so they catch defects the row-set compare
/// can miss — e.g. a phantom/duplicate physical row that inflates `COUNT(*)` /
/// `SUM(value)` while the deduplicated id→value map still looks right.
async fn verify_aggregate_queries(
    ctx: &SessionContext,
    name: &str,
    model: &Model,
    key_space: i64,
    ctx_msg: &str,
) -> TestResult<()> {
    // Total row count must equal the number of live keys (no phantom/dup rows).
    let count = scalar_i64(ctx, &format!("SELECT COUNT(*) FROM {name}")).await?;
    assert_eq!(
        count,
        i64::try_from(model.len()).expect("model len fits i64"),
        "{ctx_msg}: COUNT(*) mismatch"
    );

    // SUM over all values — sensitive to duplicates and wrong values that a
    // per-key compare might not surface if the dup carries the same key.
    let sum = scalar_i64(ctx, &format!("SELECT COALESCE(SUM(value), 0) FROM {name}")).await?;
    let expected_sum: i64 = model.values().copied().sum();
    assert_eq!(sum, expected_sum, "{ctx_msg}: SUM(value) mismatch");

    // Filtered count exercises a value predicate + pushdown.
    const THRESH: i64 = 500_000;
    let filtered =
        scalar_i64(ctx, &format!("SELECT COUNT(*) FROM {name} WHERE value >= {THRESH}")).await?;
    let expected_filtered =
        i64::try_from(model.values().filter(|&&v| v >= THRESH).count()).expect("fits i64");
    assert_eq!(
        filtered, expected_filtered,
        "{ctx_msg}: COUNT(*) WHERE value >= {THRESH} mismatch"
    );

    // Point lookups across the key space (PK filter pushdown + deletion filter):
    // each key is present exactly once or absent, matching the model.
    let step = (key_space / 8).max(1);
    let mut k = 0;
    while k < key_space {
        let got = scalar_i64(ctx, &format!("SELECT COUNT(*) FROM {name} WHERE id = {k}")).await?;
        let expected = i64::from(model.contains_key(&k));
        assert_eq!(got, expected, "{ctx_msg}: COUNT(*) WHERE id = {k} mismatch");
        k += step;
    }
    Ok(())
}

// ============================================================================
// Op generation + model
// ============================================================================

#[derive(Clone, Debug)]
enum Op {
    Upsert { rows: Vec<(i64, i64)> },
    Delete { key: i64 },
    DeleteAll,
    Overwrite { rows: Vec<(i64, i64)> },
    Compact,
    Restart,
}

fn random_rows(rng: &mut Rng, key_space: i64, batch_size: i64) -> Vec<(i64, i64)> {
    let mut rows: Vec<(i64, i64)> = Vec::new();
    for _ in 0..batch_size.max(1) {
        let k = rng.below(key_space as u64) as i64;
        let v = rng.below(1_000_000) as i64;
        // last-writer-wins within the batch (a batch may not repeat a PK)
        if let Some(slot) = rows.iter_mut().find(|(ek, _): &&mut (i64, i64)| *ek == k) {
            slot.1 = v;
        } else {
            rows.push((k, v));
        }
    }
    rows
}

fn gen_op(rng: &mut Rng, w: &OpWeights, key_space: i64, batch_size: i64) -> Op {
    let total = w.upsert + w.delete + w.delete_all + w.overwrite + w.compact + w.restart;
    let mut pick = rng.below(u64::from(total)) as u32;
    for (weight, kind) in [
        (w.upsert, 0u8),
        (w.delete, 1),
        (w.delete_all, 2),
        (w.overwrite, 3),
        (w.compact, 4),
        (w.restart, 5),
    ] {
        if pick < weight {
            return match kind {
                0 => Op::Upsert {
                    rows: random_rows(rng, key_space, batch_size),
                },
                1 => Op::Delete {
                    key: rng.below(key_space as u64) as i64,
                },
                2 => Op::DeleteAll,
                3 => Op::Overwrite {
                    rows: random_rows(rng, key_space, batch_size),
                },
                4 => Op::Compact,
                _ => Op::Restart,
            };
        }
        pick -= weight;
    }
    Op::Compact // unreachable when total > 0
}

fn apply_model(model: &mut Model, op: &Op) {
    match op {
        Op::Upsert { rows } => {
            for (k, v) in rows {
                model.insert(*k, *v);
            }
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

// ============================================================================
// Harness
// ============================================================================

async fn run_sequential(fixture: &TestFixture, w: &Workload, seed: u64) -> TestResult<()> {
    let name = format!("seq_{:?}_{seed}", w.mode);
    let (mut table, mut ctx) = create_table(fixture, &name, w.mode, w.pk_keyset_cache_mb).await?;
    let mut rng = Rng::new(seed);
    let mut model = Model::new();
    let mut history: Vec<Op> = Vec::with_capacity(w.ops);

    for step in 0..w.ops {
        let op = gen_op(&mut rng, &w.weights, w.population, w.batch_size);
        history.push(op.clone());
        match &op {
            Op::Upsert { rows } => upsert(&table, rows).await?,
            Op::Delete { key } => delete(&table, col("id").eq(lit(*key))).await?,
            Op::DeleteAll => delete(&table, lit(true)).await?,
            Op::Overwrite { rows } => overwrite(&table, rows).await?,
            Op::Compact => {
                table.maybe_compact_small_files().await?;
            }
            Op::Restart => {
                let (t, c) = reopen_table(fixture, &name).await?;
                table = t;
                ctx = c;
            }
        }
        apply_model(&mut model, &op);
        let live = read_rows(&ctx, &name).await?;
        assert_converged(
            &live,
            &model,
            &format!(
                "seq diverged after step {step} ({op:?}) mode={:?} seed={seed}\nhistory={history:?}",
                w.mode
            ),
        );
    }

    table.maybe_compact_small_files().await?;
    let (_t, c) = reopen_table(fixture, &name).await?;
    let final_state = read_rows(&c, &name).await?;
    let msg = format!(
        "seq final compact+restart diverged mode={:?} seed={seed}\nhistory={history:?}",
        w.mode
    );
    assert_converged(&final_state, &model, &msg);
    verify_aggregate_queries(&c, &name, &model, w.population, &msg).await?;
    Ok(())
}

async fn run_concurrent(fixture: &TestFixture, w: &Workload, seed: u64) -> TestResult<()> {
    let name = format!("conc_{:?}_{seed}", w.mode);
    let (table0, ctx0) = create_table(fixture, &name, w.mode, w.pk_keyset_cache_mb).await?;
    let mut rng = Rng::new(seed);

    for start in (0..w.population).step_by(20) {
        let rows: Vec<(i64, i64)> = (start..(start + 20).min(w.population))
            .map(|k| (k, k * 10))
            .collect();
        upsert(&table0, &rows).await?;
    }
    let mut model: Model = (0..w.population).map(|k| (k, k * 10)).collect();

    // Restart-swappable table handle shared with the background compactor.
    // Foreground ops AND compaction take a READ lock (so they still run
    // concurrently — read locks are shared); a `Restart` op takes the WRITE
    // lock, which quiesces both, reopens the table from the catalog, and swaps
    // the handle. This exercises restart/recovery CONCURRENTLY with compaction
    // (the compactor holds its read lock across each pass, so a restart waits
    // for an in-flight compaction rather than tearing it). The read context is
    // tracked separately and only used for the post-quiesce assertions (the
    // mutation helpers build their own contexts), so it just follows the latest
    // reopened provider.
    let handle = Arc::new(tokio::sync::RwLock::new(Arc::clone(&table0)));
    let mut ctx = ctx0;
    drop(table0);

    let stop = Arc::new(AtomicBool::new(false));
    let bg_handle = Arc::clone(&handle);
    let bg_stop = Arc::clone(&stop);
    let compactor = tokio::spawn(async move {
        while !bg_stop.load(Ordering::Relaxed) {
            {
                // Hold the read lock across the pass so a concurrent restart
                // (write lock) waits for it instead of swapping mid-compaction.
                let t = bg_handle.read().await;
                let _ = t.maybe_compact_small_files().await;
            }
            tokio::task::yield_now().await;
        }
    });

    // Foreground stream. `compact` is driven by the background loop (a foreground
    // Compact op is a no-op here); `restart` reopens under the write lock.
    for _ in 0..w.ops {
        let op = gen_op(&mut rng, &w.weights, w.population, w.batch_size);
        match &op {
            Op::Upsert { rows } => {
                let t = handle.read().await;
                upsert(&t, rows).await?;
            }
            Op::Delete { key } => {
                let t = handle.read().await;
                delete(&t, col("id").eq(lit(*key))).await?;
            }
            Op::DeleteAll => {
                let t = handle.read().await;
                delete(&t, lit(true)).await?;
            }
            Op::Overwrite { rows } => {
                let t = handle.read().await;
                overwrite(&t, rows).await?;
            }
            Op::Restart => {
                // Exclusive: waits for any in-flight compaction, then reopens
                // from the catalog and swaps in the fresh provider + context.
                let mut guard = handle.write().await;
                let (nt, nc) = reopen_table(fixture, &name).await?;
                *guard = nt;
                ctx = nc;
            }
            Op::Compact => continue,
        }
        apply_model(&mut model, &op);
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    stop.store(true, Ordering::Relaxed);
    compactor.await.expect("compaction task joins");
    let table = Arc::clone(&*handle.read().await);
    table.maybe_compact_small_files().await?;

    let live = read_rows(&ctx, &name).await?;
    let msg = format!("concurrent convergence failed mode={:?} seed={seed}", w.mode);
    assert_converged(&live, &model, &msg);
    verify_aggregate_queries(&ctx, &name, &model, w.population, &msg).await?;
    Ok(())
}

async fn run_workload(fixture: TestFixture, w: Workload) -> TestResult<()> {
    for seed in 0..w.seeds {
        match w.concurrency {
            Concurrency::Sequential => run_sequential(&fixture, &w, seed).await?,
            Concurrency::ConcurrentWithCompaction => run_concurrent(&fixture, &w, seed).await?,
        }
    }
    Ok(())
}

// ============================================================================
// Exhaustiveness knobs (env-scalable for CI)
// ============================================================================
//
// Defaults keep a local `cargo test` fast. CI can dial up coverage without code
// changes:
//   * `CAYENNE_PROPTEST_SCALE`     — multiplies the SEED count of every config
//     (each seed is an independent random walk / fresh interleaving, so this is
//     the highest-value, roughly-linear-cost knob for finding rare races).
//   * `CAYENNE_PROPTEST_OPS_SCALE` — multiplies the per-seed OP count (longer
//     sequences = deeper histories; costlier in the concurrent configs because
//     each op carries a small real-time sleep, so scale this more gently).
// Both default to 1 and accept any positive integer; a missing/zero/unparseable
// value is treated as 1.
fn env_scale(var: &str) -> u64 {
    std::env::var(var)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(1)
}
fn scaled_seeds(base: u64) -> u64 {
    base * env_scale("CAYENNE_PROPTEST_SCALE")
}
fn scaled_ops(base: usize) -> usize {
    base * env_scale("CAYENNE_PROPTEST_OPS_SCALE") as usize
}

// ============================================================================
// Named configs
// ============================================================================

const SEQUENTIAL_MIXED: OpWeights = OpWeights {
    upsert: 40,
    delete: 25,
    delete_all: 8,
    overwrite: 12,
    compact: 8,
    restart: 7,
};
const CONCURRENT_MIXED: OpWeights = OpWeights {
    upsert: 40,
    delete: 60,
    delete_all: 0,
    overwrite: 0,
    // Compaction is driven by the background loop (foreground `compact` is a
    // no-op here); `restart` reopens the table from the catalog mid-stream, under
    // a lock, concurrently with that compaction — exercising restart/recovery
    // durability against racing compaction.
    compact: 0,
    restart: 4,
};
const CONCURRENT_UPSERT_ONLY: OpWeights = OpWeights {
    upsert: 100,
    delete: 0,
    delete_all: 0,
    overwrite: 0,
    compact: 0,
    restart: 0,
};

fn sequential(mode: Mode) -> Workload {
    Workload {
        mode,
        concurrency: Concurrency::Sequential,
        weights: SEQUENTIAL_MIXED,
        population: 6,
        batch_size: 1,
        ops: scaled_ops(50),
        seeds: scaled_seeds(24),
        pk_keyset_cache_mb: None,
    }
}
fn concurrent_mixed(mode: Mode) -> Workload {
    Workload {
        mode,
        concurrency: Concurrency::ConcurrentWithCompaction,
        weights: CONCURRENT_MIXED,
        population: 300,
        batch_size: 1,
        ops: scaled_ops(250),
        seeds: scaled_seeds(16),
        pk_keyset_cache_mb: None,
    }
}
// High-collision variant: a small key space with many ops drives repeated
// delete/re-upsert on the SAME keys while the background compactor folds, which
// is exactly the interleaving that surfaced the deletion-index lost-update race.
// `concurrent_mixed`'s large key space rarely revisits a key, so this dense
// config exercises the race far more per op. `pk_keyset_cache_mb` selects the
// upsert conflict-detection path: `None` = exact PK index, `Some(0)` = bloom
// existence filter (the over-budget path with its own re-insert-over-tombstone
// handling).
fn concurrent_mixed_dense(mode: Mode, pk_keyset_cache_mb: Option<usize>) -> Workload {
    Workload {
        mode,
        concurrency: Concurrency::ConcurrentWithCompaction,
        weights: CONCURRENT_MIXED,
        population: 16,
        batch_size: 1,
        // Long per-seed sequences (depth) on a tiny key space matter more than
        // seed count here; keep the default seed count low so a local run stays
        // quick, and let CI raise it via `CAYENNE_PROPTEST_SCALE`.
        ops: scaled_ops(400),
        seeds: scaled_seeds(6),
        pk_keyset_cache_mb,
    }
}
fn concurrent_upsert_only(mode: Mode) -> Workload {
    Workload {
        mode,
        concurrency: Concurrency::ConcurrentWithCompaction,
        weights: CONCURRENT_UPSERT_ONLY,
        population: 300,
        batch_size: 1,
        ops: scaled_ops(200),
        seeds: scaled_seeds(10),
        pk_keyset_cache_mb: None,
    }
}

// --- Sequential convergence (GREEN) ---
async fn prop_sequential_key_impl(f: TestFixture) -> TestResult<()> {
    run_workload(f, sequential(Mode::Key)).await
}
async fn prop_sequential_position_impl(f: TestFixture) -> TestResult<()> {
    run_workload(f, sequential(Mode::Position)).await
}
test_with_backends!(prop_sequential_key_impl);
test_with_backends!(prop_sequential_position_impl);

// --- Concurrent pure-upsert convergence (GREEN control: loss needs deletes) ---
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prop_concurrent_upsert_only_key_sqlite() -> TestResult<()> {
    common::run_with_backend(BackendType::Sqlite, |f| {
        run_workload(f, concurrent_upsert_only(Mode::Key))
    })
    .await
    .map_err(|e| -> Box<dyn std::error::Error> { e })
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prop_concurrent_upsert_only_position_sqlite() -> TestResult<()> {
    common::run_with_backend(BackendType::Sqlite, |f| {
        run_workload(f, concurrent_upsert_only(Mode::Position))
    })
    .await
    .map_err(|e| -> Box<dyn std::error::Error> { e })
}

// --- Concurrent mixed delete/upsert convergence ---
//
// Heavy concurrent delete+upsert against a background compaction loop. This
// previously lost a re-upserted row (pure loss, ~1 in 3 runs): a background
// compaction's deletion-index prune (`load_full` + `store`) and a concurrent
// re-upsert's insert-record publish (`load_full` + `store`) could interleave so
// the prune's store clobbered the re-insert's store — the re-inserted key lost
// the insert-record that supersedes its pending tombstone, so the row stayed
// hidden and was dropped when the holding snapshot was later consolidated. Fixed
// by publishing every deletion-index update through `ArcSwap::rcu` (no lost
// updates), so these now converge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prop_concurrent_mixed_key_sqlite() -> TestResult<()> {
    common::run_with_backend(BackendType::Sqlite, |f| {
        run_workload(f, concurrent_mixed(Mode::Key))
    })
    .await
    .map_err(|e| -> Box<dyn std::error::Error> { e })
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prop_concurrent_mixed_position_sqlite() -> TestResult<()> {
    common::run_with_backend(BackendType::Sqlite, |f| {
        run_workload(f, concurrent_mixed(Mode::Position))
    })
    .await
    .map_err(|e| -> Box<dyn std::error::Error> { e })
}

// Dense (small key space) variant of the above — many ops per key against the
// background compactor, maximizing same-key delete/re-upsert/compaction races.
// Run on BOTH conflict-detection paths: the exact PK index (default) and the
// over-budget bloom existence filter (`pk_keyset_cache_mb = Some(0)`), since the
// re-insert-over-tombstone handling differs between them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prop_concurrent_mixed_dense_key_sqlite() -> TestResult<()> {
    common::run_with_backend(BackendType::Sqlite, |f| {
        run_workload(f, concurrent_mixed_dense(Mode::Key, None))
    })
    .await
    .map_err(|e| -> Box<dyn std::error::Error> { e })
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prop_concurrent_mixed_dense_position_sqlite() -> TestResult<()> {
    common::run_with_backend(BackendType::Sqlite, |f| {
        run_workload(f, concurrent_mixed_dense(Mode::Position, None))
    })
    .await
    .map_err(|e| -> Box<dyn std::error::Error> { e })
}
// Same dense stress, but forcing the bloom existence-filter conflict path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prop_concurrent_mixed_dense_bloom_key_sqlite() -> TestResult<()> {
    common::run_with_backend(BackendType::Sqlite, |f| {
        run_workload(f, concurrent_mixed_dense(Mode::Key, Some(0)))
    })
    .await
    .map_err(|e| -> Box<dyn std::error::Error> { e })
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prop_concurrent_mixed_dense_bloom_position_sqlite() -> TestResult<()> {
    common::run_with_backend(BackendType::Sqlite, |f| {
        run_workload(f, concurrent_mixed_dense(Mode::Position, Some(0)))
    })
    .await
    .map_err(|e| -> Box<dyn std::error::Error> { e })
}

// ============================================================================
// Snapshot-isolation under concurrent reads (overwrite torn-publish class)
// ============================================================================

async fn run_concurrent_reads_seed(fixture: &TestFixture, mode: Mode, seed: u64) -> TestResult<()> {
    let name = format!("iso_{mode:?}_{seed}");
    let (table, ctx) = create_table(fixture, &name, mode, None).await?;
    let mut rng = Rng::new(seed);

    let n: i64 = 200;
    let keyset: Vec<i64> = (0..n).collect();
    let seed_rows: Vec<(i64, i64)> = keyset.iter().map(|&k| (k, 0)).collect();
    overwrite(&table, &seed_rows).await?;

    let stop = Arc::new(AtomicBool::new(false));
    let comp_table = Arc::clone(&table);
    let comp_stop = Arc::clone(&stop);
    let compactor = tokio::spawn(async move {
        while !comp_stop.load(Ordering::Relaxed) {
            let _ = comp_table.maybe_compact_small_files().await;
            tokio::task::yield_now().await;
        }
    });

    let read_ctx = SessionContext::new();
    read_ctx.register_table(&name, Arc::clone(&table) as Arc<dyn TableProvider>)?;
    let read_stop = Arc::clone(&stop);
    let read_name = name.clone();
    let reader = tokio::spawn(async move {
        let mut violation: Option<String> = None;
        while !read_stop.load(Ordering::Relaxed) {
            match read_rows(&read_ctx, &read_name).await {
                Ok(live) => {
                    if live.len() as i64 != n || (0..n).any(|k| !live.contains_key(&k)) {
                        violation =
                            Some(format!("torn read: saw {} rows (expected {n})", live.len()));
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

    let live = read_rows(&ctx, &name).await?;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prop_concurrent_reads_observe_consistent_snapshot_key_sqlite() -> TestResult<()> {
    common::run_with_backend(BackendType::Sqlite, |f| async move {
        run_concurrent_reads_mode(&f, Mode::Key).await
    })
    .await
    .map_err(|e| -> Box<dyn std::error::Error> { e })
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prop_concurrent_reads_observe_consistent_snapshot_position_sqlite() -> TestResult<()> {
    common::run_with_backend(BackendType::Sqlite, |f| async move {
        run_concurrent_reads_mode(&f, Mode::Position).await
    })
    .await
    .map_err(|e| -> Box<dyn std::error::Error> { e })
}
