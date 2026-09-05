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

//! Integration test for distributed full-text search (issue #13080).
//!
//! A partitioned, accelerated dataset scatters its rows across multiple
//! executors. A `text_search(...)` query issued to the scheduler must fan out
//! to every executor that owns a matching partition, merge the per-executor
//! BM25 candidates, and return the same top-N rows *in the same order* as the
//! identical query over a single-node (non-distributed) copy of the same data.
//!
//! BM25 is collection-relative (it divides by document frequency and average
//! document length), so a naive distributed implementation that scores each
//! partition against only its *local* statistics can rank rows differently from
//! a single node that sees the whole collection. This test pins that behaviour:
//! the distributed and single-node id orderings must be identical.
//!
//! ## Declarative (Spicepod) equivalent of the in-code dataset
//!
//! The cluster harness builds `App`s programmatically rather than loading YAML,
//! so there is no fixture file on disk. The equivalent Spicepod for the
//! distributed dataset (naming rule `{connector}-{accelerator}-{test_variant}`,
//! i.e. `file-arrow-distributed_full_text_search`) is:
//!
//! ```yaml
//! version: v1
//! kind: Spicepod
//! name: file-arrow-distributed_full_text_search
//! datasets:
//!   - from: file:<tempdir>/fts_docs.csv
//!     name: fts_docs
//!     acceleration:
//!       enabled: true
//!       mode: memory          # arrow (default engine)
//!       refresh_mode: full
//!       partition_by:
//!         - "CASE WHEN id = 3 THEN 0 ELSE 1 END"
//!     columns:
//!       - name: body
//!         full_text_search:
//!           enabled: true
//!           row_id:
//!             - id
//! runtime:
//!   scheduler:
//!     state_location: s3://.../cluster-state/<test>/<run-id>/
//!     # ... s3 params, partition assignment tuning ...
//! ```

use app::AppBuilder;
use arrow::array::RecordBatch;
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::component::dataset::Dataset;
use spicepod::component::runtime::{Runtime as SpicepodRuntime, Scheduler as SchedulerConfig};
use spicepod::semantic::{Column, FullTextSearchConfig};
use spicepod::{
    acceleration::{Acceleration, Mode, RefreshMode},
    partitioning::PartitionedBy,
};
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

use crate::{
    configure_test_datafusion,
    utils::{runtime_ready_check, test_request_context, verify_env_secret_exists, wait_until_true},
};

use super::harness::ClusterHarness;

/// 12 rows. The rare term `peregrine` appears in exactly three documents, with a
/// strictly-decreasing term frequency (3x, 2x, 1x) across three similar-length
/// documents, so a correctly-merged global BM25 imposes a strict order on the
/// three hits: id 3 > id 7 > id 11. The common term `data` appears in many docs
/// and is only present to give the collection realistic document-frequency
/// statistics (so BM25 has something to normalise against).
///
/// `id = 3` (the highest term frequency, so the strongest global-stats hit) is
/// deliberately isolated onto a single-row partition (see
/// `make_fts_partitioned_dataset`), so its *local* document frequency and
/// collection size are the most extreme in the dataset. Scoring it against
/// local-only statistics (`N = 1`, `df = 1`) collapses its idf far below the
/// correctly-merged global idf, while `id = 7` and `id = 11` share the other,
/// 11-row partition and get an inflated local idf (`N = 11`, `df = 2`) instead
/// of the smaller global one. A per-partition-only (unmerged) implementation
/// therefore produces `id 7 > id 11 > id 3` — the opposite order at both ends
/// from the correct `3 > 7 > 11` — so this fixture actually exercises global
/// statistics merging rather than coincidentally matching it.
const FTS_DOCS_CSV: &str = r"id,body
1,the system stores rows of data in tables
2,queries read data from many tables quickly
3,a peregrine peregrine peregrine hunts at dawn
4,common words fill this ordinary sentence here
5,data pipelines move data between data stores
6,indexes speed up lookups across the dataset
7,a peregrine peregrine soars above the cliff
8,text about ordinary birds and their habits
9,more data flows through the busy data channels
10,nothing special appears in this plain line
11,a peregrine glides silently over the wide ridge
12,data data data everywhere in the warehouse today
";

/// The rare-term search that both the distributed and single-node paths run.
///
/// Uses the four-argument positional form `text_search(table, query, column,
/// limit)` requested by the issue. `_score` (see `SEARCH_SCORE_COLUMN_NAME`) is
/// exposed by the UDTF; ordering by it (with `id` as a tiebreak) makes the row
/// order deterministic regardless of the score-emission order.
///
// TODO(distributed-fts): every existing test applies the row cap via a SQL
// `LIMIT`, not the 4th positional `text_search(..., 5)` argument (the provider
// docstring lists `[limit]` as positional, but the parse path is unexercised
// here). If argument parsing rejects the positional limit, drop the `5` and add
// `LIMIT 5` instead — the collection is tiny and the ORDER BY already pins order.
const RARE_TERM_SEARCH_SQL: &str = "SELECT id, _score FROM text_search(fts_docs, 'peregrine', body, 5) \
     ORDER BY _score DESC, id ASC";

/// Ids of the documents that contain `peregrine`, in the BM25 order the
/// single-node collection produces. The distributed path must match this.
const EXPECTED_RARE_TERM_IDS: [i64; 3] = [3, 7, 11];

/// Verifies distributed full-text search returns the same top-N rows and order
/// as single-node search.
///
/// 1. Build a single-node accelerated copy of the data with FTS on `body` and
///    capture the id ordering `text_search('peregrine')` produces.
/// 2. Stand up a scheduler + 2 executors accelerating the *same* data,
///    deterministically partitioned so `id = 3` is alone on one partition and
///    every other row (including `id = 7` and `id = 11`) is on the other —
///    guaranteeing the two executors each own a disjoint, non-empty slice of
///    the matching rows rather than merely being likely to.
/// 3. Run the identical `text_search` query through the scheduler and assert the
///    returned ids and their order equal the single-node baseline.
#[tokio::test(flavor = "multi_thread")]
async fn distributed_full_text_search_matches_single_node_ordering() -> Result<(), anyhow::Error> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("runtime=debug,info"))
        .with_ansi(true)
        .try_init();

    // The scheduler persists cluster state to S3 (its `PartitionManager` needs
    // conditional-put / OCC, which the local filesystem object store does not
    // support), so the same credentials the other distributed-acceleration
    // tests rely on must be present.
    for env_var in ["AWS_S3_VECTORS_KEY", "AWS_S3_VECTORS_SECRET"] {
        verify_env_secret_exists(env_var)
            .await
            .map_err(anyhow::Error::msg)?;
    }

    let csv_tempdir = tempfile::tempdir().expect("csv tempdir");
    let csv_path = csv_tempdir.path().join("fts_docs.csv");
    tokio::fs::write(&csv_path, FTS_DOCS_CSV)
        .await
        .expect("write fts docs");
    let source = format!("file://{}", csv_path.display());

    test_request_context()
        .scope(async {
            configure_test_datafusion();

            // --- Single-node baseline --------------------------------------
            let baseline_ids = single_node_baseline_ids(&source).await?;
            assert_eq!(
                baseline_ids, EXPECTED_RARE_TERM_IDS,
                "single-node BM25 ordering changed; update EXPECTED_RARE_TERM_IDS \
                 or the fixture data before trusting the distributed comparison"
            );

            // --- Distributed cluster ---------------------------------------
            let app = AppBuilder::new("test_distributed_fts")
                .with_dataset(make_fts_partitioned_dataset(&source, "fts_docs"))
                .with_runtime(SpicepodRuntime {
                    scheduler: Some(make_named_scheduler_config(
                        "distributed_full_text_search_matches_single_node_ordering",
                    )),
                    ..SpicepodRuntime::default()
                })
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(app)
                .executors(2)
                .start()
                .await?;

            harness.wait_for_executors(Duration::from_secs(30)).await?;
            wait_for_row_count(&harness, "fts_docs", 12, Duration::from_mins(1)).await?;

            // The dataset's 2 partitions must land on 2 distinct executors — not
            // merely be likely to, per the deterministic `id = 3` vs. everything-else
            // split in `make_fts_partitioned_dataset` — or the "distributed" query
            // below would silently exercise only one executor's local (uncontested)
            // Tantivy index and never touch the cross-executor BM25 merge this test
            // exists to pin.
            let owner_counts = harness.partition_owner_counts("fts_docs");
            assert_eq!(
                owner_counts.len(),
                2,
                "expected fts_docs' 2 partitions to be split across 2 distinct \
                 executors, got: {owner_counts:?}"
            );
            assert!(
                owner_counts.values().all(|&count| count == 1),
                "expected each executor to own exactly 1 partition (no partition \
                 co-location), got: {owner_counts:?}"
            );

            // Wait until the distributed FTS index is queryable for all three hits,
            // not merely until the rows are counted — index build can lag row load.
            // TODO(distributed-fts): confirm whether a partition being row-count-ready
            // also guarantees its full-text index is built and registered with the
            // scheduler. If not, a dedicated readiness signal (analogous to
            // `PartitionsLoaded`) should gate this instead of polling the query.
            let indexed = wait_until_true(Duration::from_mins(1), || {
                let harness = &harness;
                async move {
                    distributed_search_ids(harness, RARE_TERM_SEARCH_SQL)
                        .await
                        .is_ok_and(|ids| ids.len() == EXPECTED_RARE_TERM_IDS.len())
                }
            })
            .await;
            anyhow::ensure!(
                indexed,
                "distributed text_search never returned all {} `peregrine` hits — \
                 the full-text index did not build on every owning executor",
                EXPECTED_RARE_TERM_IDS.len()
            );

            let distributed_ids = distributed_search_ids(&harness, RARE_TERM_SEARCH_SQL).await?;

            assert_eq!(
                distributed_ids, baseline_ids,
                "distributed full-text search returned different top-N rows/order than \
                 single-node: distributed={distributed_ids:?} single_node={baseline_ids:?}. \
                 BM25 statistics are not being merged across partitions."
            );

            harness.shutdown().await;
            Ok(())
        })
        .await
}

// ---------------------------------------------------------------------------
// Single-node baseline
// ---------------------------------------------------------------------------

/// Build a standalone (non-cluster) runtime over the same data with the same FTS
/// configuration and memory acceleration — but no partitioning — and return the
/// id ordering `text_search('peregrine')` produces.
async fn single_node_baseline_ids(source: &str) -> Result<Vec<i64>, anyhow::Error> {
    let app = AppBuilder::new("test_single_node_fts_baseline")
        .with_dataset(make_fts_single_node_dataset(source, "fts_docs"))
        .build();

    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    let load_rt = Arc::clone(&rt);
    tokio::select! {
        () = tokio::time::sleep(Duration::from_mins(1)) => {
            anyhow::bail!("timed out loading single-node baseline components");
        }
        () = load_rt.load_components() => {}
    }
    runtime_ready_check(&rt).await;

    // Index build can trail readiness; poll until all three hits are searchable.
    let indexed = wait_until_true(Duration::from_secs(30), || {
        let rt = Arc::clone(&rt);
        async move {
            single_node_search_ids(&rt, RARE_TERM_SEARCH_SQL)
                .await
                .is_ok_and(|ids| ids.len() == EXPECTED_RARE_TERM_IDS.len())
        }
    })
    .await;
    anyhow::ensure!(
        indexed,
        "single-node text_search never returned all {} `peregrine` hits",
        EXPECTED_RARE_TERM_IDS.len()
    );

    let ids = single_node_search_ids(&rt, RARE_TERM_SEARCH_SQL).await?;
    rt.shutdown().await;
    Ok(ids)
}

// ---------------------------------------------------------------------------
// Query helpers
// ---------------------------------------------------------------------------

/// Run `sql` through the scheduler and extract the `id` column.
async fn distributed_search_ids(
    harness: &ClusterHarness,
    sql: &str,
) -> Result<Vec<i64>, anyhow::Error> {
    ids_from_batches(&harness.query(sql).await?, sql)
}

/// Run `sql` on a standalone runtime and extract the `id` column.
async fn single_node_search_ids(rt: &Arc<Runtime>, sql: &str) -> Result<Vec<i64>, anyhow::Error> {
    let batches = rt
        .datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("query `{sql}` failed: {e}"))?
        .data
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| anyhow::anyhow!("collecting `{sql}` failed: {e}"))?;
    ids_from_batches(&batches, sql)
}

/// Extract an `Int64` `id` column from result batches, preserving row order.
fn ids_from_batches(batches: &[RecordBatch], sql: &str) -> Result<Vec<i64>, anyhow::Error> {
    let mut ids = Vec::new();
    for batch in batches {
        let column = batch.column_by_name("id").ok_or_else(|| {
            anyhow::anyhow!(
                "query `{sql}` returned no `id` column: {:?}",
                batch.schema()
            )
        })?;
        let values = column
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("`id` column is not Int64 for `{sql}`"))?;
        for i in 0..batch.num_rows() {
            ids.push(values.value(i));
        }
    }
    Ok(ids)
}

/// Poll `SELECT COUNT(*) FROM {table}` through the scheduler until it returns
/// `expected` rows, or time out.
async fn wait_for_row_count(
    harness: &ClusterHarness,
    table: &str,
    expected: usize,
    timeout: Duration,
) -> Result<(), anyhow::Error> {
    let start = std::time::Instant::now();
    let mut last_count: Option<usize> = None;
    loop {
        if let Ok(batches) = harness
            .query(&format!("SELECT COUNT(*) AS cnt FROM {table}"))
            .await
        {
            for batch in &batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                if let Some(arr) = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                {
                    #[expect(clippy::cast_sign_loss, reason = "COUNT(*) is always non-negative")]
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "row count fits in usize on 64-bit"
                    )]
                    let count = arr.value(0) as usize;
                    last_count = Some(count);
                    if count == expected {
                        return Ok(());
                    }
                }
            }
        }
        if start.elapsed() > timeout {
            return Err(anyhow::anyhow!(
                "Timed out waiting for {table} to have {expected} rows (last count: {last_count:?})"
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

// ---------------------------------------------------------------------------
// Dataset builders
// ---------------------------------------------------------------------------

/// A `body` column with full-text search enabled and `id` as its row id.
fn body_fts_column() -> Column {
    Column {
        r#type: Some("utf8".to_string()),
        nullable: Some(true),
        ..Column::new("body")
    }
    .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("id"))
}

/// Memory-accelerated dataset with FTS on `body`, deterministically split into
/// 2 partitions: `id = 3` alone, and every other row (including `id = 7` and
/// `id = 11`) together. A hash-based `bucket()` split would only make a
/// cross-executor placement of the matching rows *likely*; this pins it, and
/// pins which side `id = 3` lands on (see the divergence this is designed to
/// produce in the `FTS_DOCS_CSV` doc comment).
fn make_fts_partitioned_dataset(source_path: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(source_path, name);
    dataset.columns = vec![
        Column {
            r#type: Some("int64".to_string()),
            nullable: Some(true),
            ..Column::new("id")
        },
        body_fts_column(),
    ];
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Full),
        partition_by: vec![PartitionedBy {
            name: "expr0".to_string(),
            expression: "CASE WHEN id = 3 THEN 0 ELSE 1 END".to_string(),
        }],
        ..Acceleration::default()
    });
    dataset
}

/// Same data + FTS config as the partitioned dataset, but no partitioning — the
/// single-node baseline the distributed result is compared against.
fn make_fts_single_node_dataset(source_path: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(source_path, name);
    dataset.columns = vec![
        Column {
            r#type: Some("int64".to_string()),
            nullable: Some(true),
            ..Column::new("id")
        },
        body_fts_column(),
    ];
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        mode: Mode::Memory,
        refresh_mode: Some(RefreshMode::Full),
        ..Acceleration::default()
    });
    dataset
}

/// Scheduler config pointing at a per-run S3 state prefix. Mirrors the helper in
/// `distributed_acceleration.rs` (which is module-private): the `PartitionManager`
/// uses OCC and needs conditional-put support, so the shared cluster state must
/// live on S3, and a UUID suffix keeps each run's assignments isolated.
fn make_named_scheduler_config(test_name: &str) -> SchedulerConfig {
    let run_id = uuid::Uuid::new_v4();
    SchedulerConfig {
        state_location: format!(
            "s3://spiceai-integration-tests/cluster-state/{test_name}/{run_id}/"
        ),
        params: Some(spicepod::param::Params::from_string_map(
            std::collections::HashMap::from([
                ("s3_region".to_string(), "us-east-1".to_string()),
                (
                    "s3_key".to_string(),
                    "${env:AWS_S3_VECTORS_KEY}".to_string(),
                ),
                (
                    "s3_secret".to_string(),
                    "${env:AWS_S3_VECTORS_SECRET}".to_string(),
                ),
                ("s3_auth".to_string(), "key".to_string()),
            ]),
        )),
        partition_assignment_interval: "1s".to_string(),
        max_partition_assignments_per_interval:
            spicepod::component::runtime::default_max_partition_assignments_per_interval(),
        // The dataset has exactly 2 partitions (see `make_fts_partitioned_dataset`).
        // Cap at 1 per executor so the 2 executors are each forced to take one,
        // rather than one greedily owning both — forcing the query to merge BM25
        // results across executors, which is the behaviour under test.
        max_partitions_per_executor: 1,
        partition_discovery_timeout:
            spicepod::component::runtime::default_partition_discovery_timeout(),
    }
}
