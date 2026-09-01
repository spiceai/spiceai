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
//! `CAYENNE_PROPTEST_OPS_SCALE` multiplies the per-seed op count. Both accept
//! fractional values (e.g. `0.25` for a lighter per-PR pass) and default to 1
//! (the current fast local run). Scaling never drops a config below 1 seed/op,
//! so every config still runs on every PR — only its depth changes.
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
use cayenne::metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog, SlotAdvancer};
use common::{BackendType, TestFixture};
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::{Expr, col, lit};
use datafusion_expr::dml::InsertOp;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

/// Slot advancer that arms `cdc_durability: memory` deferral in tests (the
/// runtime installs the real one on the first replayable committer). Without it
/// `is_cdc_memory_mode() && has_slot_advancer()` is false and mem-mode CDC writes
/// fall back to the durable path — so the mem-tier + checkpoint path under test
/// never runs.
struct NoopSlotAdvancer;
#[async_trait::async_trait]
impl SlotAdvancer for NoopSlotAdvancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
}

/// Whether a workload drives writes through the durable path (`insert_batch` /
/// `delete_from`) or the in-memory CDC tier (`write_cdc_append_stream` /
/// `write_cdc_delete_keys_in_memory` + periodic `checkpoint_mem_tier`). Memory
/// requires key-based deletion (`is_cdc_memory_mode` excludes position deletes).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Durability {
    File,
    Memory,
}

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
    /// Uniform in `[0, n)`. Callers conceptually require `n > 0` (a key space,
    /// population, or weight total); a zero bound means a misconfigured workload,
    /// so fail fast instead of silently coercing to `below(1)` and always returning 0.
    fn below(&mut self, n: u64) -> u64 {
        debug_assert!(n > 0, "Rng::below requires a positive bound (got 0)");
        self.next_u64() % n.max(1)
    }
    /// [`Rng::below`] for the `i64` key/value space the model uses. Keeps the
    /// `u64`↔`i64` conversions in one checked place (the bound must be
    /// non-negative; the result is in `[0, n)` and so always fits `i64`).
    fn below_i64(&mut self, n: i64) -> i64 {
        let bound = u64::try_from(n).expect("Rng::below_i64 bound must be non-negative");
        i64::try_from(self.below(bound)).expect("value in [0, n) fits i64")
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
    /// Delete by a predicate over a non-PK column: the scan-and-match path,
    /// which `delete` (PK index) and `delete_all` (no matching) never reach.
    /// A `retention_sql` DELETE has this shape.
    delete_predicate: u32,
    overwrite: u32,
    compact: u32,
    restart: u32,
    /// Settle + compact the warm tier to the cold store + one GC pass (the
    /// production promotion-tick sequence, PRNG-scheduled). Requires
    /// `Workload::cold`; keep 0 in non-cold configs.
    move_to_cold_tier: u32,
}

#[derive(Clone, Copy)]
struct Workload {
    mode: Mode,
    /// Durable insert path vs in-memory CDC tier + checkpoint. `Memory` is only
    /// valid with [`Mode::Key`] (mem mode excludes position deletes).
    durability: Durability,
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
    /// `true` = cold (datalake) tier enabled on a local `file://` store
    cold: bool,
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

/// GC grace for cold fuzz configs (`cold_tier_gc_interval_ms` doubles as the
/// orphan grace) — short enough to age out within a walk.
const COLD_GC_GRACE_MS: u64 = 25;

fn config(
    mode: Mode,
    durability: Durability,
    pk_keyset_cache_mb: Option<usize>,
    cold_url: Option<String>,
) -> VortexConfig {
    let base = VortexConfig {
        target_vortex_file_size_mb: 1,
        compaction_trigger_files: 4,
        compaction_background_interval_ms: 0,
        inline_max_rows: 0,
        cdc_durability: match durability {
            Durability::File => CdcDurability::File,
            Durability::Memory => CdcDurability::Memory,
        },
        // `Some(0)` forces the over-budget bloom existence-filter path for upsert
        // conflict detection (instead of the exact PK keyset); `None` keeps the
        // default exact index. Lets one harness fuzz both existence paths.
        pk_keyset_cache_mb,
        ..VortexConfig::default()
    };
    let base = match cold_url {
        Some(cold_url) => VortexConfig {
            cold_tier_location: Some(cold_url),
            // Any durable warm file triggers promotion.
            cold_tier_warm_max_files: 1,
            // No wall-clock promoter task: a timer fires at times unrelated to
            // the op stream (destroying seed-deterministic replay), so the
            // harness re-adds promotion + GC as the explicit, PRNG-scheduled
            // `MoveToColdTier` op in the same serialization order production uses.
            cold_tier_background_interval_ms: 0,
            cold_tier_gc_interval_ms: COLD_GC_GRACE_MS,
            cold_clustering_columns: vec!["id".to_string()],
            cold_target_file_size_mb: 1,
            ..base
        },
        None => base,
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
    durability: Durability,
    pk_keyset_cache_mb: Option<usize>,
    cold: bool,
) -> TestResult<(Arc<CayenneTableProvider>, SessionContext)> {
    // Local `file://` cold store per table (no object-store config needed —
    // the default local store resolves it).
    let cold_url = if cold {
        assert!(
            matches!(mode, Mode::Key),
            "cold fuzz configs require Mode::Key (promotion no-ops in position mode)"
        );
        let cold_dir = fixture.temp_dir.path().join(format!("cold_{name}"));
        tokio::fs::create_dir_all(&cold_dir).await?;
        Some(format!("file://{}", cold_dir.to_string_lossy()))
    } else {
        None
    };
    let opts = CreateTableOptions {
        table_name: name.to_string(),
        schema: schema(),
        primary_key: vec!["id".to_string()],
        on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
            "id".to_string(),
        ]))),
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: config(mode, durability, pk_keyset_cache_mb, cold_url),
    };
    let catalog: Arc<dyn MetadataCatalog> =
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>;
    let ctx = SessionContext::new();
    let table =
        Arc::new(CayenneTableProvider::create_table(catalog, opts, ctx.runtime_env()).await?);
    if durability == Durability::Memory {
        // Arm mem-mode deferral (the runtime does this on the first replayable
        // committer); without it mem-mode CDC writes take the durable path.
        assert!(
            table.is_cdc_memory_mode(),
            "Memory durability requires an is_cdc_memory_mode-eligible table (Key mode, \
             non-partitioned); got mode={mode:?}"
        );
        table.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    }
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

/// Single-column `id` batch for the in-memory CDC delete path.
fn id_batch(keys: &[i64]) -> RecordBatch {
    let id_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(id_schema, vec![Arc::new(Int64Array::from(keys.to_vec()))])
        .expect("valid id batch")
}

fn batch_to_stream(batch: RecordBatch) -> SendableRecordBatchStream {
    let schema = batch.schema();
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter([Ok(batch)]),
    ))
}

async fn upsert(
    table: &Arc<CayenneTableProvider>,
    rows: &[(i64, i64)],
    durability: Durability,
) -> TestResult<()> {
    match durability {
        Durability::File => {
            common::insert_batch(table.as_ref(), rows_to_batch(rows)).await?;
        }
        Durability::Memory => {
            let ctx = SessionContext::new();
            let write = table
                .write_cdc_append_stream(batch_to_stream(rows_to_batch(rows)), &ctx.task_ctx())
                .await?;
            // Memory-mode appends publish through the RAM tier synchronously; a
            // spill fallback (byte-cap breach) instead stages and needs finalize.
            if write.has_pending_finalize() {
                write.finish().await?;
            }
        }
    }
    Ok(())
}

/// Delete a single key. File uses the durable `DELETE`; memory uses the in-RAM
/// CDC delete path (`write_cdc_delete_keys_in_memory`).
async fn delete_key(
    table: &Arc<CayenneTableProvider>,
    key: i64,
    durability: Durability,
) -> TestResult<()> {
    match durability {
        Durability::File => delete_filter(table, col("id").eq(lit(key))).await?,
        Durability::Memory => {
            table
                .write_cdc_delete_keys_in_memory(&id_batch(&[key]))
                .await?;
        }
    }
    Ok(())
}

async fn delete_filter(table: &Arc<CayenneTableProvider>, filter: Expr) -> TestResult<()> {
    let ctx = SessionContext::new();
    let plan = table.delete_from(&ctx.state(), vec![filter]).await?;
    datafusion_physical_plan::collect(plan, ctx.task_ctx()).await?;
    Ok(())
}

/// Delete every live key. File uses `DELETE WHERE true`; memory deletes the
/// supplied live keyset through the in-RAM CDC delete path.
async fn delete_all(
    table: &Arc<CayenneTableProvider>,
    live_keys: &[i64],
    durability: Durability,
) -> TestResult<()> {
    match durability {
        Durability::File => delete_filter(table, lit(true)).await?,
        Durability::Memory => {
            if !live_keys.is_empty() {
                table
                    .write_cdc_delete_keys_in_memory(&id_batch(live_keys))
                    .await?;
            }
        }
    }
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

/// One "settle" pass. File compacts small files; memory additionally checkpoints
/// the RAM tier to durable Vortex files and bakes the seq-prefix (the exact
/// intersection — mem-tier checkpoint + bake — that surfaced the COUNT(*) drift).
/// Delete rows whose non-PK `value` is in `[lo, hi)`.
///
/// File only: a filtered client DELETE does not remove un-checkpointed mem-tier
/// rows (spiceai/spiceai#12008; delete-all was fixed in #11987), so
/// `delete_predicate` is weighted 0 in memory configs. Once #12008 is fixed,
/// weighting it there turns this into the regression test.
async fn delete_predicate(table: &Arc<CayenneTableProvider>, lo: i64, hi: i64) -> TestResult<()> {
    delete_filter(table, col("value").gt_eq(lit(lo)).and(col("value").lt(lit(hi)))).await
}

async fn settle(table: &Arc<CayenneTableProvider>, durability: Durability) -> TestResult<()> {
    // Drain debounced post-write maintenance so the persisted stats (the maintained
    // `num_rows` delta) reflect every committed write before we read/compact.
    table.flush_pending_maintenance().await?;
    if durability == Durability::Memory {
        table.checkpoint_mem_tier().await?;
        let _ = table.bake_seq_prefix_protected_snapshots().await?;
    }
    table.maybe_compact_small_files().await?;
    Ok(())
}

/// One background maintenance pass without full current-snapshot compaction.
///
/// Cold-tier fuzz configs disable timer-driven small-file compaction
/// (`compaction_background_interval_ms = 0`) and drive cold promotion explicitly
/// through `MoveToColdTier`. Keep the concurrent cold background loop shaped like
/// production by running protected-snapshot maintenance, but not the test-only
/// full current-snapshot rewrite.
async fn settle_protected_maintenance_only(
    table: &Arc<CayenneTableProvider>,
    durability: Durability,
) -> TestResult<()> {
    table.flush_pending_maintenance().await?;
    if durability == Durability::Memory {
        table.checkpoint_mem_tier().await?;
        let _ = table.bake_seq_prefix_protected_snapshots().await?;
    }
    table.compact_protected_snapshots_subset(usize::MAX).await?;
    Ok(())
}

/// Prepare enough warm-tier files for a cold-promotion.
async fn materialize_warm_files_for_cold_promotion(
    table: &Arc<CayenneTableProvider>,
    durability: Durability,
) -> TestResult<()> {
    table.flush_pending_maintenance().await?;
    if durability == Durability::Memory {
        table.checkpoint_mem_tier().await?;
    }
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

/// Run a query expected to return a single non-null `i64` scalar. Callers
/// `COALESCE` nullable aggregates (e.g. `COALESCE(SUM(...), 0)`) so the single
/// cell is never NULL; this helper does not itself map NULL to a value. Used by
/// the aggregate-query checks below.
async fn scalar_i64(ctx: &SessionContext, sql: &str) -> TestResult<i64> {
    // Callers are single-cell aggregates (COUNT(*) / COALESCE(SUM(...), 0)) that
    // must return EXACTLY ONE non-null i64 row. Neither zero rows nor extra rows
    // are legitimate here, so fail loudly rather than masking a query/engine bug
    // (e.g. silently reading the first of several rows).
    let batches = ctx.sql(sql).await?.collect().await?;
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    if total_rows != 1 {
        return Err(
            format!("scalar query returned {total_rows} rows, expected exactly 1: {sql}").into(),
        );
    }
    let b = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("exactly one row exists");
    let arr = b
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("i64 scalar result");
    Ok(arr.value(0))
}

/// Cross-check the table against the model through AGGREGATE + FILTER + POINT
/// queries, not just the `SELECT id, value` row scan. These exercise different
/// execution paths (aggregation, filter pushdown, PK point-lookup pushdown over
/// the merge-on-read deletion filter), so they catch defects the row-set compare
/// can miss — e.g. a phantom/duplicate physical row that inflates `COUNT(*)` /
/// `SUM(value)` while the deduplicated id→value map still looks right.
async fn verify_aggregate_queries(
    ctx: &SessionContext,
    provider: &CayenneTableProvider,
    name: &str,
    model: &Model,
    key_space: i64,
    ctx_msg: &str,
) -> TestResult<()> {
    const THRESH: i64 = 500_000;

    // Total row count must equal the number of live keys (no phantom/dup rows).
    let count = scalar_i64(ctx, &format!("SELECT COUNT(*) FROM {name}")).await?;
    assert_eq!(
        count,
        i64::try_from(model.len()).expect("model len fits i64"),
        "{ctx_msg}: COUNT(*) mismatch"
    );

    // Maintained-count exactness invariant (the distributed-gate contract): the
    // count `local_executor_table_statistics` reports to the coordinator — and
    // that the coordinator folds `COUNT(*)` on ONLY when Exact — must never be
    // `Exact`-and-wrong. If it is `Exact(n)`, n must equal the live row count; a
    // drifted count must instead be served `Inexact` (so the fold declines). This
    // is what catches "drift served Exact" defects the plain `COUNT(*)` query
    // (which single-node answers from footer sums) can miss.
    if let Some(stats) = provider.optimizer_table_statistics()
        && let datafusion::common::stats::Precision::Exact(n) = stats.num_rows
    {
        assert_eq!(
            n,
            model.len(),
            "{ctx_msg}: maintained num_rows served Exact({n}) but {} rows are live — a drifted \
             count must be served Inexact so the distributed COUNT(*) fold declines",
            model.len(),
        );
    }

    // SUM over all values — sensitive to duplicates and wrong values that a
    // per-key compare might not surface if the dup carries the same key.
    let sum = scalar_i64(ctx, &format!("SELECT COALESCE(SUM(value), 0) FROM {name}")).await?;
    let expected_sum: i64 = model.values().copied().sum();
    assert_eq!(sum, expected_sum, "{ctx_msg}: SUM(value) mismatch");

    // Filtered count exercises a value predicate + pushdown.
    let filtered = scalar_i64(
        ctx,
        &format!("SELECT COUNT(*) FROM {name} WHERE value >= {THRESH}"),
    )
    .await?;
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
    /// Delete every row whose non-PK `value` falls in `[lo, hi)`.
    DeletePredicate {
        lo: i64,
        hi: i64,
    },
    Overwrite { rows: Vec<(i64, i64)> },
    Compact,
    Restart,
    MoveToColdTier,
}

/// One live value to anchor a predicate window on; `None` when the table is
/// empty.
fn sample_live_value(model: &Model, rng: &mut Rng) -> Option<i64> {
    if model.is_empty() {
        return None;
    }
    let len = u64::try_from(model.len()).expect("model len fits u64");
    let idx = usize::try_from(rng.below(len)).expect("index below len fits usize");
    model.values().nth(idx).copied()
}

/// Value domain for the non-PK `value` column; `DeletePredicate` sizes its
/// window against it.
const VALUE_SPACE: i64 = 1_000_000;

fn random_rows(rng: &mut Rng, key_space: i64, batch_size: i64) -> Vec<(i64, i64)> {
    debug_assert!(
        batch_size > 0,
        "random_rows requires a positive batch_size (an op writes >= 1 row); a \
         zero batch means a misconfigured workload that writes nothing"
    );
    let mut rows: Vec<(i64, i64)> = Vec::new();
    // `.max(1)` keeps this division-/loop-safe in release builds; the
    // debug_assert above catches the misconfiguration in tests.
    for _ in 0..batch_size.max(1) {
        let k = rng.below_i64(key_space);
        let v = rng.below_i64(VALUE_SPACE);
        // last-writer-wins within the batch (a batch may not repeat a PK)
        if let Some(slot) = rows.iter_mut().find(|(ek, _): &&mut (i64, i64)| *ek == k) {
            slot.1 = v;
        } else {
            rows.push((k, v));
        }
    }
    rows
}

fn gen_op(
    rng: &mut Rng,
    w: &OpWeights,
    key_space: i64,
    batch_size: i64,
    live_value: Option<i64>,
) -> Op {
    let total = w.upsert
        + w.delete
        + w.delete_all
        + w.overwrite
        + w.compact
        + w.restart
        + w.move_to_cold_tier;
    debug_assert!(
        total > 0,
        "OpWeights must have a positive total weight (else the workload runs no real ops)"
    );
    let mut pick = u32::try_from(rng.below(u64::from(total)))
        .expect("rng.below(total) is < total, which fits u32");
    for (weight, kind) in [
        (w.upsert, 0u8),
        (w.delete, 1),
        (w.delete_all, 2),
        (w.delete_predicate, 7),
        (w.overwrite, 3),
        (w.compact, 4),
        (w.restart, 5),
        (w.move_to_cold_tier, 6),
    ] {
        if pick < weight {
            return match kind {
                0 => Op::Upsert {
                    rows: random_rows(rng, key_space, batch_size),
                },
                1 => Op::Delete {
                    key: rng.below_i64(key_space),
                },
                2 => Op::DeleteAll,
                7 => {
                    // A window narrower than `VALUE_SPACE` matches nothing, and a
                    // blind one rarely intersects a table holding a few rows.
                    // Anchoring half on a live value reaches deletion-vector
                    // writing rather than only the scan-and-match plan.
                    let width = 1 + rng.below_i64(VALUE_SPACE / 2);
                    let lo = match live_value {
                        Some(v) if rng.below(2) == 0 => (v - rng.below_i64(width)).max(0),
                        _ => rng.below_i64(VALUE_SPACE),
                    };
                    Op::DeletePredicate {
                        lo,
                        hi: lo + width,
                    }
                }
                3 => Op::Overwrite {
                    rows: random_rows(rng, key_space, batch_size),
                },
                4 => Op::Compact,
                5 => Op::Restart,
                _ => Op::MoveToColdTier,
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
        Op::DeletePredicate { lo, hi } => model.retain(|_, v| !(*v >= *lo && *v < *hi)),
        Op::Overwrite { rows } => {
            model.clear();
            for (k, v) in rows {
                model.insert(*k, *v);
            }
        }
        Op::Compact | Op::Restart | Op::MoveToColdTier => {}
    }
}

// ============================================================================
// Harness
// ============================================================================

async fn run_sequential(
    fixture: &TestFixture,
    w: &Workload,
    seed: u64,
) -> TestResult<usize> {
    let name = format!("seq_{:?}_{:?}_{seed}", w.mode, w.durability);
    let (mut table, mut ctx) = create_table(
        fixture,
        &name,
        w.mode,
        w.durability,
        w.pk_keyset_cache_mb,
        w.cold,
    )
    .await?;
    let mut rng = Rng::new(seed);
    let mut model = Model::new();
    let mut history: Vec<Op> = Vec::with_capacity(w.ops);
    let mut predicate_deleted_rows = 0usize;

    for step in 0..w.ops {
        let live_value = sample_live_value(&model, &mut rng);
        let op = gen_op(&mut rng, &w.weights, w.population, w.batch_size, live_value);
        history.push(op.clone());
        match &op {
            Op::Upsert { rows } => upsert(&table, rows, w.durability).await?,
            Op::Delete { key } => delete_key(&table, *key, w.durability).await?,
            Op::DeleteAll => {
                // Pre-op live keys (memory deletes them by key; file uses WHERE true).
                let live_keys: Vec<i64> = model.keys().copied().collect();
                delete_all(&table, &live_keys, w.durability).await?;
            }
            Op::DeletePredicate { lo, hi } => delete_predicate(&table, *lo, *hi).await?,
            Op::Overwrite { rows } => overwrite(&table, rows).await?,
            Op::Compact => {
                settle(&table, w.durability).await?;
            }
            Op::Restart => {
                // A clean restart, not a crash. For memory durability checkpoint the
                // RAM tier first so no un-acked mem rows are lost (the model expects
                // every applied op). Then DRAIN this instance's detached maintenance:
                // a real crash would kill it, and an in-process reopen must drain it
                // or the old instance's compaction can commit against the shared
                // catalog concurrently with the reopened provider (distinct
                // `compaction_lock`s), corrupting the protected-snapshot set. See
                // `drain_in_flight_maintenance`.
                if w.durability == Durability::Memory {
                    table.checkpoint_mem_tier().await?;
                }
                table.drain_in_flight_maintenance().await?;
                let (t, c) = reopen_table(fixture, &name).await?;
                table = t;
                ctx = c;
                // Re-arm mem-mode deferral on the fresh provider instance.
                if w.durability == Durability::Memory {
                    table.install_slot_advancer(Arc::new(NoopSlotAdvancer));
                }
            }
            Op::MoveToColdTier => {
                // Durable warm files must exist for the trigger to fire.
                materialize_warm_files_for_cold_promotion(&table, w.durability).await?;
                // `Ok(false)` (warm tier below threshold) is fine.
                let _ = table.promote_warm_to_cold().await?;
                // GC serialized after promote, same task — the production
                // promotion-tick order (out-of-band GC is unsupported).
                table.run_cold_tier_gc_tick().await;
            }
        }
        let rows_before_op = model.len();
        apply_model(&mut model, &op);
        let retired_rows = model.len() < rows_before_op;
        if matches!(op, Op::DeletePredicate { .. }) {
            predicate_deleted_rows += rows_before_op - model.len();
        }
        let live = read_rows(&ctx, &name).await?;
        let step_msg = format!(
            "seq diverged after step {step} ({op:?}) mode={:?} durability={:?} seed={seed}\nhistory={history:?}",
            w.mode, w.durability,
        );
        assert_converged(&live, &model, &step_msg);

        // Retiring rows is when a maintained count drifts and when deletion
        // vectors leave holes for a stale min/max to prune around. The
        // coordinator can fold `COUNT(*)` from an `Exact` count at any time, so
        // the contract has to hold here, not only after the final settle.
        if retired_rows {
            verify_aggregate_queries(&ctx, table.as_ref(), &name, &model, w.population, &step_msg)
                .await?;
        }
    }

    // Final settle (memory: checkpoint RAM + bake; both: compact) so the reopened
    // state is durable, then drain this instance's in-flight detached maintenance
    // before reopening from the catalog (see the loop's Op::Restart).
    settle(&table, w.durability).await?;
    table.drain_in_flight_maintenance().await?;
    let (t, c) = reopen_table(fixture, &name).await?;
    let final_state = read_rows(&c, &name).await?;
    let msg = format!(
        "seq final compact+restart diverged mode={:?} durability={:?} seed={seed}\nhistory={history:?}",
        w.mode, w.durability,
    );
    assert_converged(&final_state, &model, &msg);
    verify_aggregate_queries(&c, t.as_ref(), &name, &model, w.population, &msg).await?;
    Ok(predicate_deleted_rows)
}

async fn run_concurrent(fixture: &TestFixture, w: &Workload, seed: u64) -> TestResult<()> {
    let name = format!("conc_{:?}_{:?}_{seed}", w.mode, w.durability);
    let (table0, ctx0) = create_table(
        fixture,
        &name,
        w.mode,
        w.durability,
        w.pk_keyset_cache_mb,
        w.cold,
    )
    .await?;
    let mut rng = Rng::new(seed);

    for start in (0..w.population).step_by(20) {
        let rows: Vec<(i64, i64)> = (start..(start + 20).min(w.population))
            .map(|k| (k, k * 10))
            .collect();
        upsert(&table0, &rows, w.durability).await?;
    }
    let mut model: Model = (0..w.population).map(|k| (k, k * 10)).collect();

    // Restart-swappable table handle shared with the background maintenance loop.
    // Foreground ops AND maintenance take a READ lock (so they still run
    // concurrently — read locks are shared); a `Restart` op takes the WRITE
    // lock, which quiesces both, reopens the table from the catalog, and swaps
    // the handle. This exercises restart/recovery CONCURRENTLY with maintenance
    // (the loop holds its read lock across each pass, so a restart waits for an
    // in-flight pass rather than tearing it). The read context is tracked
    // separately and only used for the post-quiesce assertions (the mutation
    // helpers build their own contexts), so it just follows the latest reopened
    // provider.
    let handle = Arc::new(tokio::sync::RwLock::new(Arc::clone(&table0)));
    let mut ctx = ctx0;
    drop(table0);

    let stop = Arc::new(AtomicBool::new(false));
    let bg_handle = Arc::clone(&handle);
    let bg_stop = Arc::clone(&stop);
    let bg_durability = w.durability;
    let bg_protected_only = w.cold;
    let compactor = tokio::spawn(async move {
        while !bg_stop.load(Ordering::Relaxed) {
            {
                // Hold the read lock across the pass so a concurrent restart
                // (write lock) waits for it instead of swapping mid-maintenance.
                // Cold configs keep this production-shaped: protected-snapshot
                // maintenance only, because timer-driven small-file compaction is
                // disabled for these tables.
                let t = bg_handle.read().await;
                if let Err(e) = if bg_protected_only {
                    settle_protected_maintenance_only(&t, bg_durability).await
                } else {
                    settle(&t, bg_durability).await
                } {
                    panic!("background maintenance pass failed: {e}");
                }
            }
            tokio::task::yield_now().await;
        }
    });

    // Foreground stream. `compact` is driven by the background loop (a foreground
    // Compact op is a no-op here); `restart` reopens under the write lock.
    let mut history: Vec<Op> = Vec::with_capacity(w.ops);
    for _ in 0..w.ops {
        let live_value = sample_live_value(&model, &mut rng);
        let op = gen_op(&mut rng, &w.weights, w.population, w.batch_size, live_value);
        history.push(op.clone());
        match &op {
            Op::Upsert { rows } => {
                let t = handle.read().await;
                upsert(&t, rows, w.durability).await?;
            }
            Op::Delete { key } => {
                let t = handle.read().await;
                delete_key(&t, *key, w.durability).await?;
            }
            Op::DeleteAll => {
                let live_keys: Vec<i64> = model.keys().copied().collect();
                let t = handle.read().await;
                delete_all(&t, &live_keys, w.durability).await?;
            }
            Op::DeletePredicate { lo, hi } => {
                let t = handle.read().await;
                delete_predicate(&t, *lo, *hi).await?;
            }
            Op::Overwrite { rows } => {
                let t = handle.read().await;
                overwrite(&t, rows).await?;
            }
            Op::Restart => {
                // Exclusive: waits for any in-flight compaction, then reopens
                // from the catalog and swaps in the fresh provider + context.
                // Drain the OLD instance's detached maintenance first so it cannot
                // commit against the shared catalog after the reopen (its
                // `compaction_lock` is distinct from the reopened provider's — the
                // background compactor's read lock does NOT gate it).
                let mut guard = handle.write().await;
                // Clean restart: for memory durability checkpoint the RAM tier
                // (persist un-acked mem rows), then drain this instance's detached
                // maintenance so it cannot commit against the shared catalog after
                // the reopen.
                if w.durability == Durability::Memory {
                    guard.checkpoint_mem_tier().await?;
                }
                guard.drain_in_flight_maintenance().await?;
                let (nt, nc) = reopen_table(fixture, &name).await?;
                if w.durability == Durability::Memory {
                    nt.install_slot_advancer(Arc::new(NoopSlotAdvancer));
                }
                *guard = nt;
                ctx = nc;
            }
            Op::MoveToColdTier => {
                // Foreground promotion under the shared read lock, racing the
                // background protected-snapshot maintenance loop. GC runs
                // serialized after promote (production order); see the
                // sequential arm.
                let t = handle.read().await;
                materialize_warm_files_for_cold_promotion(&t, w.durability).await?;
                let _ = t.promote_warm_to_cold().await?;
                t.run_cold_tier_gc_tick().await;
            }
            Op::Compact => continue,
        }
        apply_model(&mut model, &op);
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    stop.store(true, Ordering::Relaxed);
    compactor.await.expect("background maintenance task joins");
    let table = Arc::clone(&*handle.read().await);
    if w.cold {
        settle_protected_maintenance_only(&table, w.durability).await?;
    } else {
        settle(&table, w.durability).await?;
    }
    // Quiesce before the final assertion so no detached compaction from this (or
    // an earlier, since-replaced) instance commits mid-read.
    table.drain_in_flight_maintenance().await?;

    let live = read_rows(&ctx, &name).await?;
    let msg = format!(
        "concurrent convergence failed mode={:?} durability={:?} seed={seed}\nhistory={history:?}",
        w.mode, w.durability,
    );
    assert_converged(&live, &model, &msg);
    verify_aggregate_queries(&ctx, table.as_ref(), &name, &model, w.population, &msg).await?;
    Ok(())
}

async fn run_workload(fixture: TestFixture, w: Workload) -> TestResult<()> {
    let mut predicate_deleted_rows = 0usize;
    for seed in 0..w.seeds {
        match w.concurrency {
            Concurrency::Sequential => {
                predicate_deleted_rows += run_sequential(&fixture, &w, seed).await?;
            }
            Concurrency::ConcurrentWithCompaction => run_concurrent(&fixture, &w, seed).await?,
        }
    }
    // An op that never retires a row still runs the delete plan, so it would sit
    // in the weights looking like coverage. Fail loudly if the window and the
    // value domain drift apart again.
    if w.weights.delete_predicate > 0 && matches!(w.concurrency, Concurrency::Sequential) {
        assert!(
            predicate_deleted_rows > 0,
            "delete_predicate is weighted {} but retired 0 rows across {} seeds — the window no \
             longer intersects the value domain, so the op is inert",
            w.weights.delete_predicate,
            w.seeds,
        );
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
// Both default to 1 and accept any positive number, including fractions below 1
// (e.g. `0.25` for a lighter per-PR pass); a missing/non-positive/unparseable
// value is treated as 1. The scaled result is always rounded up to at least 1,
// so no config's seed/op count can be scaled away to 0 — every config still
// runs, just shallower.
fn env_scale(var: &str) -> f64 {
    std::env::var(var)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|&v| v > 0.0)
        .unwrap_or(1.0)
}
#[expect(
    clippy::cast_precision_loss,
    reason = "base seed counts are small (<1_000); exact in f64"
)]
#[expect(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    reason = "env_scale() is always positive and the result is floored at 1.0 before casting"
)]
fn scaled_seeds(base: u64) -> u64 {
    ((base as f64) * env_scale("CAYENNE_PROPTEST_SCALE"))
        .round()
        .max(1.0) as u64
}
#[expect(
    clippy::cast_precision_loss,
    reason = "base op counts are small (<1_000); exact in f64"
)]
#[expect(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    reason = "env_scale() is always positive and the result is floored at 1.0 before casting"
)]
fn scaled_ops(base: usize) -> usize {
    ((base as f64) * env_scale("CAYENNE_PROPTEST_OPS_SCALE"))
        .round()
        .max(1.0) as usize
}

// ============================================================================
// Named configs
// ============================================================================

const SEQUENTIAL_MIXED: OpWeights = OpWeights {
    upsert: 40,
    delete: 25,
    delete_all: 8,
    delete_predicate: 10,
    overwrite: 12,
    compact: 8,
    restart: 7,
    move_to_cold_tier: 0,
};
const CONCURRENT_MIXED: OpWeights = OpWeights {
    upsert: 40,
    delete: 60,
    delete_all: 0,
    delete_predicate: 10,
    overwrite: 0,
    // Compaction is driven by the background loop (foreground `compact` is a
    // no-op here); `restart` reopens the table from the catalog mid-stream, under
    // a lock, concurrently with that compaction — exercising restart/recovery
    // durability against racing compaction.
    compact: 0,
    restart: 4,
    move_to_cold_tier: 0,
};
const CONCURRENT_UPSERT_ONLY: OpWeights = OpWeights {
    upsert: 100,
    delete: 0,
    delete_all: 0,
    delete_predicate: 0,
    overwrite: 0,
    compact: 0,
    restart: 0,
    move_to_cold_tier: 0,
};

// Memory-CDC op mix. Excludes `delete_all`/`overwrite` (durable-rewrite ops with
// no mem-tier equivalent); `compact` (a "settle" here — checkpoint + bake +
// compact) is weighted UP because the mem-tier drift only surfaces once rows are
// checkpointed to durable files and the seq-prefix is baked.
const MEMORY_MIXED: OpWeights = OpWeights {
    upsert: 45,
    delete: 25,
    delete_all: 0,
    // 0 until spiceai/spiceai#12008: a filtered DELETE leaves un-checkpointed
    // mem-tier rows live.
    delete_predicate: 0,
    overwrite: 0,
    compact: 25,
    restart: 5,
    move_to_cold_tier: 0,
};

fn sequential(mode: Mode) -> Workload {
    Workload {
        mode,
        durability: Durability::File,
        concurrency: Concurrency::Sequential,
        weights: SEQUENTIAL_MIXED,
        population: 6,
        batch_size: 1,
        ops: scaled_ops(50),
        seeds: scaled_seeds(24),
        pk_keyset_cache_mb: None,
        cold: false,
    }
}
// Sequential memory-CDC: drives the mem-tier append + checkpoint + seq-prefix bake
// path (Key mode only — mem mode excludes position deletes). This is the exact
// intersection the durable-path configs never exercised, where the COUNT(*)
// over-count lived.
fn sequential_memory() -> Workload {
    Workload {
        mode: Mode::Key,
        durability: Durability::Memory,
        concurrency: Concurrency::Sequential,
        weights: MEMORY_MIXED,
        population: 8,
        batch_size: 1,
        ops: scaled_ops(60),
        seeds: scaled_seeds(16),
        pk_keyset_cache_mb: None,
        cold: false,
    }
}
fn concurrent_mixed(mode: Mode) -> Workload {
    Workload {
        mode,
        durability: Durability::File,
        concurrency: Concurrency::ConcurrentWithCompaction,
        weights: CONCURRENT_MIXED,
        population: 300,
        batch_size: 1,
        ops: scaled_ops(250),
        seeds: scaled_seeds(16),
        pk_keyset_cache_mb: None,
        cold: false,
    }
}
// Concurrent memory-CDC: foreground mem-tier upserts/deletes racing a background
// checkpoint + bake loop — the concurrency profile behind the SF-1000 gate.
fn concurrent_memory() -> Workload {
    Workload {
        mode: Mode::Key,
        durability: Durability::Memory,
        concurrency: Concurrency::ConcurrentWithCompaction,
        weights: MEMORY_MIXED,
        population: 32,
        batch_size: 1,
        ops: scaled_ops(200),
        seeds: scaled_seeds(8),
        pk_keyset_cache_mb: None,
        cold: false,
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
        durability: Durability::File,
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
        cold: false,
    }
}
fn concurrent_upsert_only(mode: Mode) -> Workload {
    Workload {
        mode,
        durability: Durability::File,
        concurrency: Concurrency::ConcurrentWithCompaction,
        weights: CONCURRENT_UPSERT_ONLY,
        population: 300,
        batch_size: 1,
        ops: scaled_ops(200),
        seeds: scaled_seeds(10),
        pk_keyset_cache_mb: None,
        cold: false,
    }
}
// Sequential cold walk: mixed ops + promotion/GC, model-checked after every op.
// Covers: dirty rewrite, carry-forward, tombstone application across tiers,
// delete/re-upsert of cold-resident keys, reopen with a cold manifest.
// Population stays small: promotion is a whole-warm-tier rewrite, so lean on op
// depth over key-space size.
fn sequential_cold() -> Workload {
    Workload {
        mode: Mode::Key,
        durability: Durability::File,
        concurrency: Concurrency::Sequential,
        weights: OpWeights {
            upsert: 40,
            delete: 25,
            delete_all: 5,
            delete_predicate: 10,
            overwrite: 10,
            compact: 5,
            restart: 5,
            move_to_cold_tier: 10,
        },
        population: 8,
        batch_size: 1,
        ops: scaled_ops(60),
        seeds: scaled_seeds(16),
        pk_keyset_cache_mb: None,
        cold: true,
    }
}
// Concurrent cold: foreground mixed delete/upsert + promotions + restarts with
// a background maintenance loop.
fn concurrent_cold() -> Workload {
    Workload {
        mode: Mode::Key,
        durability: Durability::File,
        concurrency: Concurrency::ConcurrentWithCompaction,
        weights: OpWeights {
            upsert: 40,
            delete: 55,
            delete_all: 0,
            delete_predicate: 10,
            overwrite: 0,
            compact: 0,
            restart: 3,
            move_to_cold_tier: 8,
        },
        population: 32,
        batch_size: 1,
        ops: scaled_ops(200),
        seeds: scaled_seeds(6),
        pk_keyset_cache_mb: None,
        cold: true,
    }
}

// --- Harness ---

/// The workloads below plan deeply enough that some need more stack than the
/// 2 MiB std gives a thread, so `test_with_backends!` runs every body on a
/// `common::TEST_STACK_SIZE` thread. Assert that headroom directly: it is a
/// property of the harness, not of any one workload, and without a test of its
/// own a harness change that dropped it would surface as an unrelated
/// workload's process aborting on whichever backend happens to plan deepest.
///
/// Touches 4 MiB — twice the std default, a quarter of what the harness
/// reserves — so it overflows without the harness and clears it with room to
/// spare.
async fn prop_harness_stack_headroom_impl(_f: TestFixture) -> TestResult<()> {
    /// 64 KiB per frame, kept out of the caller's frame by `inline(never)` and
    /// out of the optimizer's reach by `black_box`.
    #[inline(never)]
    fn descend(frames: u32) {
        // Consuming stack is the whole assertion, so the lint's advice to move
        // this to the heap would leave the test measuring nothing. The size is
        // deliberate, and it is bounded: 64 KiB × 64 frames against the 16 MiB
        // `common::TEST_STACK_SIZE` the harness reserves.
        #[expect(
            clippy::large_stack_arrays,
            reason = "the frame must live on the stack for this to test stack headroom"
        )]
        let mut frame = [0u8; 64 * 1024];
        std::hint::black_box(&mut frame);
        if frames > 0 {
            descend(frames - 1);
        }
    }

    descend(63);
    Ok(())
}
test_with_backends!(prop_harness_stack_headroom_impl);

// --- Sequential convergence (GREEN) ---
async fn prop_sequential_key_impl(f: TestFixture) -> TestResult<()> {
    run_workload(f, sequential(Mode::Key)).await
}
async fn prop_sequential_position_impl(f: TestFixture) -> TestResult<()> {
    run_workload(f, sequential(Mode::Position)).await
}
test_with_backends!(prop_sequential_key_impl);
test_with_backends!(prop_sequential_position_impl);

// --- Memory-CDC convergence + count-exactness (mem-tier checkpoint + bake) ---
//
// Drives writes through the in-RAM CDC tier (`write_cdc_append_stream` /
// `write_cdc_delete_keys_in_memory`) with periodic checkpoint + seq-prefix bake —
// the path the durable-path configs above never exercise. Besides row-set
// convergence, `verify_aggregate_queries` asserts the maintained count is never
// served `Exact`-and-wrong, guarding the distributed COUNT(*) gate against the
// mem-tier drift.
async fn prop_sequential_memory_impl(f: TestFixture) -> TestResult<()> {
    run_workload(f, sequential_memory()).await
}
test_with_backends!(prop_sequential_memory_impl);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prop_concurrent_memory_sqlite() -> TestResult<()> {
    common::run_with_backend(BackendType::Sqlite, |f| {
        run_workload(f, concurrent_memory())
    })
    .await
    .map_err(|e| -> Box<dyn std::error::Error> { e })
}

// --- Cold (datalake) tier convergence: promotion + GC as PRNG-scheduled ops ---
//
// `MoveToColdTier` settles the warm tier, promotes it to a local `file://` cold
// store, and runs one GC pass — the production promotion-tick sequence. The op
// is a model no-op, so every existing check does the work: `assert_converged`
// catches loss/resurrection across tiers, `verify_aggregate_queries` catches
// cross-tier double-counts (phantom rows visible in both warm and cold), and
// `Restart` reopens from the catalog — validating the persisted cold manifest
// on every walk.
async fn prop_sequential_cold_impl(f: TestFixture) -> TestResult<()> {
    run_workload(f, sequential_cold()).await
}
test_with_backends!(prop_sequential_cold_impl);

// Foreground promotions + restarts with a background compactor/bake loop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prop_concurrent_cold_sqlite() -> TestResult<()> {
    common::run_with_backend(BackendType::Sqlite, |f| run_workload(f, concurrent_cold()))
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e })
}

// --- Focused regression: re-upsert after overwrite+delete stays visible ---
//
// The minimal deterministic shape behind the sequential walks: INSERT OVERWRITE
// a key, DELETE it, then UPSERT it again — the re-upserted value must be visible
// (the delete tombstone must not outlive the later re-insert). The
// `prop_sequential_*` walks cover this shape probabilistically across seeds;
// pinning the exact three-op sequence makes a regression point straight at the
// cause instead of surfacing as a rare seed-dependent walk failure. Run in both
// deletion modes, which take different prune paths (key index vs position).
async fn reupsert_after_overwrite_delete_is_visible_impl(f: TestFixture) -> TestResult<()> {
    for mode in [Mode::Key, Mode::Position] {
        let name = format!("reupsert_min_{mode:?}");
        let (table, ctx) = create_table(&f, &name, mode, Durability::File, None, false).await?;

        overwrite(&table, &[(1, 100)]).await?;
        delete_key(&table, 1, Durability::File).await?;
        upsert(&table, &[(1, 200)], Durability::File).await?;

        let live = read_rows(&ctx, &name).await?;
        assert_eq!(
            live.get(&1).copied(),
            Some(200),
            "re-upsert after overwrite+delete was lost (mode={mode:?}, expected 200, live={live:?})"
        );
    }
    Ok(())
}
test_with_backends!(reupsert_after_overwrite_delete_is_visible_impl);

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
    let (table, ctx) = create_table(fixture, &name, mode, Durability::File, None, false).await?;
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
                    if i64::try_from(live.len()).expect("live len fits i64") != n
                        || (0..n).any(|k| !live.contains_key(&k))
                    {
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
            .map(|&k| (k, rng.below_i64(1_000_000)))
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
        i64::try_from(live.len()).expect("live len fits i64"),
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
