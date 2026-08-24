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

//! Forward regression guard for the overwrite snapshot-publish atomicity fix.
//!
//! `PreparedOverwrite::finish` publishes the new snapshot via
//! `CayenneTableProvider::publish_overwrite_snapshot`, which flips the in-memory
//! `current_snapshot_id`, clears the deletion caches, invalidates the inline cache,
//! and swaps the listing table — ALL under a single `listing_fence.write()`
//! acquisition. Before that fix these ran unfenced, so a scan holding
//! `listing_fence.read()` (which captures the deletion snapshot and the current
//! snapshot id at different points) could observe a torn state and silently vanish
//! or resurrect rows. The fenced publish mirrors the proven-correct compaction path
//! `rewrite_current_snapshot_for_compaction`.
//!
//! This test runs concurrent `INSERT OVERWRITE` (re-inserting the SAME key set, so
//! the row count is invariant) against a stream of `SELECT COUNT(*)` scans, and
//! asserts every scan observes exactly the stable count — never a torn intermediate.
//!
//! Scope note (honest): a black-box concurrency test cannot *reliably* reproduce the
//! original narrow race on trunk without a deterministic interleaving hook (and the
//! fence / snapshot-id are crate-private, unreachable from an integration test). This
//! is therefore a FORWARD GUARD that exercises the fenced publish under concurrency
//! and passes with the fix; it is not asserted to fail on the pre-fix code. The
//! fix's primary correctness argument is structural equivalence to the proven sibling.
//!
//! Tiger Style: bounded loops, `expect` with messages, the scan asserts correctness.

#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use tempfile::TempDir;
use tokio::sync::Barrier;

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Stable key set every overwrite reinserts — a correct scan always sees this count.
const KEY_COUNT: i64 = 256;

/// Bounded concurrent iterations (enough to interleave; fast enough for CI).
/// Scalable via `CAYENNE_PROPTEST_OPS_SCALE` (see `common::env_scale`) for a
/// lighter per-PR pass or a deeper nightly run; floored at 1 so the race is
/// still exercised at least once.
#[expect(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    reason = "common::env_scale() is always positive and the result is floored at 1.0 before casting"
)]
fn iterations() -> usize {
    (200.0 * common::env_scale("CAYENNE_PROPTEST_OPS_SCALE"))
        .round()
        .max(1.0) as usize
}

async fn setup() -> TestResult<(SessionContext, TempDir, TempDir)> {
    let data_dir = TempDir::new()?;
    let metadata_dir = TempDir::new()?;
    let catalog = Arc::new(CayenneCatalog::new(format!(
        "sqlite://{}/test.db",
        metadata_dir.path().display()
    ))?);
    catalog.init().await?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let table_options = CreateTableOptions {
        table_name: "t".to_string(),
        schema,
        primary_key: vec!["id".to_string()],
        on_conflict: None,
        base_path: data_dir.path().to_string_lossy().to_string(),
        partition_column: None,
        vortex_config: VortexConfig::default(),
    };

    let ctx = SessionContext::new();
    let provider = Arc::new(
        CayenneTableProvider::create_table(catalog, table_options, ctx.runtime_env()).await?,
    );
    ctx.register_table("t", Arc::clone(&provider) as Arc<dyn TableProvider>)?;
    Ok((ctx, data_dir, metadata_dir))
}

/// `KEY_COUNT` rows, ids `0..KEY_COUNT`, values salted so each overwrite is distinct
/// content but the same cardinality.
fn values_clause(salt: i64) -> String {
    (0..KEY_COUNT)
        .map(|key| format!("({key}, {})", key * 10 + salt))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Run `SELECT COUNT(*)` against table `t` and return the row count.
async fn scan_count(ctx: &SessionContext) -> i64 {
    let batches = ctx
        .sql("SELECT COUNT(*) AS n FROM t")
        .await
        .expect("scan plan must build")
        .collect()
        .await
        .expect("scan must execute");
    batches
        .first()
        .expect("count query returns one batch")
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column is Int64")
        .value(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overwrite_finish_never_exposes_a_torn_snapshot_to_a_concurrent_scan() -> TestResult<()> {
    let (ctx, _data_dir, _metadata_dir) = setup().await?;

    // Seed the stable key set.
    ctx.sql(&format!("INSERT INTO t VALUES {}", values_clause(0)))
        .await?
        .collect()
        .await?;

    // Start both loops together so they genuinely overlap — the torn-publish window
    // is narrow, and a shared barrier maximizes interleaving (cf.
    // shared_metastore_concurrency_test.rs).
    let start = Arc::new(Barrier::new(2));
    let iterations = iterations();

    // Writer: repeatedly OVERWRITE with the same KEY_COUNT keys (count-invariant).
    let writer_ctx = ctx.clone();
    let writer_start = Arc::clone(&start);
    let writer = tokio::spawn(async move {
        writer_start.wait().await;
        for iteration in 0..iterations {
            let salt = i64::try_from(iteration % 7).expect("salt fits i64");
            writer_ctx
                .sql(&format!(
                    "INSERT OVERWRITE t VALUES {}",
                    values_clause(salt)
                ))
                .await
                .expect("overwrite plan must build")
                .collect()
                .await
                .expect("overwrite must execute");
        }
    });

    // Reader: scan concurrently; a torn publish shows a count != KEY_COUNT.
    let reader_ctx = ctx.clone();
    let reader_start = Arc::clone(&start);
    let reader = tokio::spawn(async move {
        reader_start.wait().await;
        let mut torn_observations = 0_usize;
        for _ in 0..iterations {
            if scan_count(&reader_ctx).await != KEY_COUNT {
                torn_observations += 1;
            }
        }
        torn_observations
    });

    writer.await.expect("writer task panicked");
    let torn_observations = reader.await.expect("reader task panicked");

    assert_eq!(
        torn_observations, 0,
        "a concurrent scan observed {torn_observations} torn snapshot states \
         (COUNT(*) != {KEY_COUNT}); overwrite finish() must publish the snapshot-id flip, \
         deletion-cache clear, inline-cache invalidation, and listing swap atomically under \
         listing_fence.write() — see CayenneTableProvider::publish_overwrite_snapshot and the \
         proven sibling rewrite_current_snapshot_for_compaction"
    );

    // Final scan: the table is left in a consistent KEY_COUNT state.
    assert_eq!(
        scan_count(&ctx).await,
        KEY_COUNT,
        "table must settle at the stable key count"
    );
    Ok(())
}
