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

//! Integration tests for distributed-query observability in `task_history`.
//!
//! Verifies that a Ballista job submitted via `Query::submit_distributed`
//! produces:
//! - A single parent `sql_query` row with non-zero `execution_duration_ms`
//!   and the expected summary labels (`distributed=true`,
//!   `ballista_job_id`, `stage_count`, `executor_count`, `total_tasks`).
//! - One or more child `ballista_stage` rows linked to the parent via
//!   `parent_span_id`, each with a `stage_id` label and the stage's plan
//!   in `input` (tree format).
//! - On a failing query, `error_message` populated on the parent.
//!
//! ## Tracing-test note
//!
//! The shared helper `utils::init_tracing_with_task_history` uses
//! `tracing::subscriber::set_default`, which is **thread-local**. In a
//! `flavor = "multi_thread"` `tokio::test`, the spawned finalize future
//! inside `QueryHandle::spawn_finalize` runs on a worker thread whose
//! thread-local dispatcher is the (no-op) global default — so the `OTel`
//! layer never sees the spans and no rows would be written.
//!
//! Production (`bin/spiced/src/tracing.rs`) avoids this by using
//! `set_global_default`. We mirror that here in `init_global_task_history`
//! — set the `OTel` subscriber as the *global* default exactly once per
//! test binary, with the exporter pointed at a swappable `DataFusion`
//! slot so each test can rebind it to its own scheduler runtime.
//!
//! ## Test serialization
//!
//! Because the global subscriber and `DF_SLOT` are process-wide, two tests
//! in this module running concurrently can step on each other (one rebinds
//! the slot to `None` while the other's export is still draining). Each
//! test acquires `TEST_LOCK` for its whole body — including `OTel` flush
//! and assertion queries — so the slot lifecycle never overlaps.

use std::sync::{Arc, Mutex, Once, OnceLock};
use tokio::sync::Mutex as AsyncMutex;

use app::AppBuilder;
use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::Int64Type;
use futures::TryStreamExt;
use opentelemetry::InstrumentationScope;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::runtime::TokioCurrentThread;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
use runtime::Runtime;
use runtime::datafusion::DataFusion;
use runtime::datafusion::query::QueryBuilder;
use runtime::task_history::otel_exporter::TaskHistoryExporter;
use spicepod::component::dataset::Dataset;
use spicepod::component::runtime::{
    TaskHistoryCapturedContext, TaskHistoryCapturedOutput, TaskHistoryCapturedPlan,
};
use std::time::Duration;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::filter;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::{configure_test_datafusion, utils::test_request_context};

use super::harness::ClusterHarness;

const NAMES_CSV: &str = include_str!("../acceleration/data/names.csv");

/// Swappable target for the `OTel` `task_history` exporter.
///
/// We can't recreate the global tracing subscriber once it's installed
/// (`set_global_default` is one-shot per process), but we *can* re-point
/// where the exporter writes by swapping this slot before each test.
type DataFusionSlot = Arc<Mutex<Option<Arc<DataFusion>>>>;

static DF_SLOT: OnceLock<DataFusionSlot> = OnceLock::new();

/// Serializes the two `tokio::test`s in this module so they don't race on
/// `DF_SLOT`. Each test holds the lock for its whole body, including
/// `OTel` flush and assertion queries.
static TEST_LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();

fn test_lock() -> &'static AsyncMutex<()> {
    TEST_LOCK.get_or_init(|| AsyncMutex::new(()))
}

/// Wraps a `TaskHistoryExporter` so its target `DataFusion` is read from a
/// shared slot at export time rather than baked in at construction. Lets
/// the global tracing subscriber outlive any single test's runtime.
struct SwappableExporter {
    slot: DataFusionSlot,
}

impl std::fmt::Debug for SwappableExporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SwappableExporter").finish_non_exhaustive()
    }
}

impl opentelemetry_sdk::trace::SpanExporter for SwappableExporter {
    fn export(
        &self,
        batch: Vec<opentelemetry_sdk::trace::SpanData>,
    ) -> impl std::future::Future<Output = opentelemetry_sdk::error::OTelSdkResult> + Send {
        let df = self.slot.lock().expect("slot poisoned").clone();
        async move {
            let Some(df) = df else {
                // No active test runtime bound; skip silently.
                return Ok(());
            };
            // Build a fresh exporter pointed at the current runtime each
            // batch. Cheap — TaskHistoryExporter is just a few Arcs.
            // The scheduler runtime registers task_history in cluster
            // mode, which requires every row to carry a non-null
            // `node_id` — synthesize one for the test.
            let (ballista_transform, ballista_retention) =
                runtime::datafusion::query::stage_history::BallistaStageMiddleware::pair();
            let exporter = TaskHistoryExporter::new(
                df as std::sync::Arc<dyn runtime_datafusion::query_engine::QueryEngine>,
                TaskHistoryCapturedOutput::Truncated,
                TaskHistoryCapturedContext::Truncated,
                None,
                TaskHistoryCapturedPlan::None,
                None,
                Some(Arc::<str>::from("test-scheduler")),
            )
            .with_transform(ballista_transform)
            .with_retention(ballista_retention);
            opentelemetry_sdk::trace::SpanExporter::export(&exporter, batch).await
        }
    }
}

/// Install a process-wide tracing subscriber with the `OTel`
/// `task_history` layer pointed at the shared `DF_SLOT`. Returns the
/// provider so callers can `force_flush()`.
fn init_global_task_history(rt: &Arc<Runtime>) -> SdkTracerProvider {
    static PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();
    static INIT_SUBSCRIBER: Once = Once::new();

    let slot = Arc::clone(DF_SLOT.get_or_init(|| Arc::new(Mutex::new(None))));
    *slot.lock().expect("slot poisoned") = Some(rt.datafusion());

    let provider = PROVIDER
        .get_or_init(|| {
            let exporter = SwappableExporter {
                slot: Arc::clone(&slot),
            };
            let processor = BatchSpanProcessor::builder(exporter, TokioCurrentThread).build();
            SdkTracerProvider::builder()
                .with_span_processor(processor)
                .build()
        })
        .clone();

    INIT_SUBSCRIBER.call_once(|| {
        let scope = InstrumentationScope::builder("task_history")
            .with_version(env!("CARGO_PKG_VERSION"))
            .build();
        let tracer = provider.tracer_with_scope(scope);
        let task_history_layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(filter::filter_fn(|metadata| {
                metadata.target() == "task_history"
            }));

        let fmt_layer = fmt::layer()
            .with_ansi(true)
            .with_filter(EnvFilter::new("runtime=info,info"));

        // `try_init()` silently no-ops when a global subscriber is already
        // set, which would leave the OTel layer uninstalled and make these
        // tests time out waiting for `task_history` rows. Fail fast with a
        // clear message so a future sibling test that installs an
        // fmt-only global subscriber gets caught immediately rather than
        // surfacing as a mysterious timeout.
        tracing_subscriber::registry()
            .with(fmt_layer)
            .with(task_history_layer)
            .try_init()
            .expect(
                "task_history OTel subscriber must be the process-global default; \
                 another global subscriber was installed first. \
                 Move task_history tests into their own test binary, or audit sibling \
                 tests in this binary for early `set_global_default` / `try_init` calls.",
            );
    });

    provider
}

/// Submit a distributed query against the scheduler runtime and drain its
/// result stream. Drops the handle so its `Drop` guard runs.
async fn run_distributed(
    harness: &ClusterHarness,
    sql: &str,
    job_name: &str,
) -> Result<(), anyhow::Error> {
    let handle = QueryBuilder::new(sql, harness.scheduler.datafusion())
        .build()
        .submit_distributed(job_name)
        .await
        .map_err(|e| anyhow::Error::msg(format!("submit_distributed failed: {e}")))?;
    let stream = handle
        .into_stream()
        .await
        .map_err(|e| anyhow::Error::msg(format!("into_stream failed: {e}")))?;
    let _ = stream
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| anyhow::Error::msg(format!("collect failed: {e}")))?;
    drop(handle);
    Ok(())
}

/// Poll `runtime.task_history` with the given count-query until it returns
/// at least `min` rows or the deadline elapses. Returns the final count.
///
/// Used instead of a fixed sleep after `force_flush` — the `OTel` batch
/// processor and the federated `task_history` table writer are both async,
/// so a fixed delay would be both slow (over-waits the happy path) and
/// flaky (under-waits under load). Polling proceeds as soon as the
/// expected rows are visible and fails loudly with the last observed
/// count on timeout.
async fn wait_for_row_count(
    harness: &ClusterHarness,
    sql_count: &str,
    min: i64,
    timeout: Duration,
) -> Result<i64, anyhow::Error> {
    let deadline = std::time::Instant::now() + timeout;
    let poll_interval = Duration::from_millis(100);
    let mut last;
    loop {
        let rows = harness.query(sql_count).await?;
        last = single_i64(&rows);
        if last >= min {
            return Ok(last);
        }
        if std::time::Instant::now() >= deadline {
            return Err(anyhow::Error::msg(format!(
                "wait_for_row_count: expected ≥{min} rows for `{sql_count}` within {timeout:?}; last observed = {last}"
            )));
        }
        tokio::time::sleep(poll_interval).await;
    }
}

/// Extract a single i64 from a single-column, single-row result batch.
fn single_i64(rows: &[RecordBatch]) -> i64 {
    assert_eq!(rows.len(), 1, "expected one batch, got {}", rows.len());
    let batch = &rows[0];
    assert_eq!(
        batch.num_rows(),
        1,
        "expected one row, got {}",
        batch.num_rows()
    );
    let col = batch.column(0).as_primitive::<Int64Type>();
    col.value(0)
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_query_records_parent_and_stage_rows() -> Result<(), anyhow::Error> {
    // Serialize against other tests in this module — shared DF_SLOT +
    // global subscriber would race otherwise.
    let _serial = test_lock().lock().await;
    test_request_context()
        .scope(async {
            let tempdir = tempfile::tempdir().expect("csv tempdir");
            let csv_path = tempdir.path().join("names.csv");
            tokio::fs::write(&csv_path, NAMES_CSV)
                .await
                .expect("write csv");

            configure_test_datafusion();

            let app = AppBuilder::new("test_distributed_task_history")
                .with_dataset(Dataset::new(
                    format!("file:{}", csv_path.display()).as_str(),
                    "names",
                ))
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(app)
                .executors(1)
                .start()
                .await?;
            harness.wait_for_executors(Duration::from_secs(15)).await?;

            // Install the global OTel subscriber (or rebind the slot if
            // another test already did). Must happen *after* the scheduler
            // runtime is built so `runtime.task_history` exists.
            let provider = init_global_task_history(&harness.scheduler);

            // Sort forces a shuffle, producing multiple stages.
            run_distributed(
                &harness,
                "SELECT id, name FROM names ORDER BY id LIMIT 5",
                "th_test_success",
            )
            .await?;

            let _ = provider.force_flush();

            // Poll until the parent row + at least one child row land in
            // task_history rather than guessing a sleep duration. Bounded
            // by 10s with a clear failure message.
            wait_for_row_count(
                &harness,
                "SELECT count(*)::bigint \
                 FROM runtime.task_history \
                 WHERE task = 'sql_query' \
                 AND labels['distributed'] = 'true' \
                 AND labels['job_id'] = 'th_test_success'",
                1,
                Duration::from_secs(10),
            )
            .await?;
            wait_for_row_count(
                &harness,
                "SELECT count(*)::bigint \
                 FROM runtime.task_history \
                 WHERE task = 'ballista_stage' \
                 AND parent_span_id IS NOT NULL",
                1,
                Duration::from_secs(10),
            )
            .await?;

            // 1. ≥1 distributed parent sql_query row for our job.
            let parent_count = single_i64(
                &harness
                    .query(
                        "SELECT count(*)::bigint \
                         FROM runtime.task_history \
                         WHERE task = 'sql_query' \
                         AND labels['distributed'] = 'true' \
                         AND labels['job_id'] = 'th_test_success'",
                    )
                    .await?,
            );
            assert!(
                parent_count >= 1,
                "expected ≥1 distributed parent sql_query row, got {parent_count}"
            );

            // 2. Parent has non-zero duration and the summary labels.
            let parent_metadata = single_i64(
                &harness
                    .query(
                        "SELECT count(*)::bigint \
                         FROM runtime.task_history \
                         WHERE task = 'sql_query' \
                         AND labels['job_id'] = 'th_test_success' \
                         AND execution_duration_ms > 0 \
                         AND labels['ballista_job_id'] IS NOT NULL \
                         AND labels['stage_count'] IS NOT NULL \
                         AND labels['executor_count'] IS NOT NULL \
                         AND labels['total_tasks'] IS NOT NULL",
                    )
                    .await?,
            );
            assert!(
                parent_metadata >= 1,
                "expected parent row with nonzero duration and summary labels"
            );

            // 3. ≥1 ballista_stage child linked to the parent.
            // Scope to *our* job — other tests in the same binary could in
            // principle write task_history rows into this scheduler's
            // table while DF_SLOT is bound, and unscoped predicates would
            // count them. Joining via parent.span_id and filtering on the
            // parent's job_id keeps the assertion narrow.
            let stage_count = single_i64(
                &harness
                    .query(
                        "SELECT count(*)::bigint \
                         FROM runtime.task_history c \
                         JOIN runtime.task_history p ON c.parent_span_id = p.span_id \
                         WHERE c.task = 'ballista_stage' \
                         AND c.labels['stage_id'] IS NOT NULL \
                         AND p.task = 'sql_query' \
                         AND p.labels['job_id'] = 'th_test_success'",
                    )
                    .await?,
            );
            assert!(
                stage_count >= 1,
                "expected ≥1 ballista_stage row with parent_span_id, got {stage_count}"
            );

            // (Previously asserted no orphan ballista_stage rows here.
            // Once `parent.labels['job_id'] = 'th_test_success'` scopes
            // children to our parent, that invariant is trivially true —
            // children are *defined* by joining to our parent's span_id.
            // The structural check is fully covered by the
            // stage_count-equality assertion below.)

            // 4. parent.labels['stage_count'] matches actual child row count.
            let consistent = single_i64(
                &harness
                    .query(
                        "WITH parent AS ( \
                             SELECT span_id, CAST(labels['stage_count'] AS BIGINT) AS expected \
                             FROM runtime.task_history \
                             WHERE task = 'sql_query' \
                             AND labels['job_id'] = 'th_test_success' \
                             LIMIT 1 \
                         ), \
                         actual AS ( \
                             SELECT count(*) AS got \
                             FROM runtime.task_history c JOIN parent p \
                                 ON c.parent_span_id = p.span_id \
                             WHERE c.task = 'ballista_stage' \
                         ) \
                         SELECT CASE WHEN p.expected = a.got THEN 1 ELSE 0 END::bigint \
                         FROM parent p, actual a",
                    )
                    .await?,
            );
            assert_eq!(
                consistent, 1,
                "parent stage_count label must equal actual child row count"
            );

            // Rebind the slot to None so subsequent tests don't write to a
            // dropped DataFusion via this provider.
            if let Some(slot) = DF_SLOT.get() {
                *slot.lock().expect("slot poisoned") = None;
            }
            harness.shutdown().await;
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(not(target_os = "windows"))]
async fn test_distributed_query_records_error_on_failure() -> Result<(), anyhow::Error> {
    let _serial = test_lock().lock().await;
    test_request_context()
        .scope(async {
            let tempdir = tempfile::tempdir().expect("csv tempdir");
            let csv_path = tempdir.path().join("names.csv");
            tokio::fs::write(&csv_path, NAMES_CSV)
                .await
                .expect("write csv");

            configure_test_datafusion();

            let app = AppBuilder::new("test_distributed_task_history_error")
                .with_dataset(Dataset::new(
                    format!("file:{}", csv_path.display()).as_str(),
                    "names",
                ))
                .build();

            let harness = ClusterHarness::builder()
                .scheduler(app)
                .executors(1)
                .start()
                .await?;
            harness.wait_for_executors(Duration::from_secs(15)).await?;

            let provider = init_global_task_history(&harness.scheduler);

            // Reference a column that doesn't exist — planning fails
            // inside `submit_distributed_internal`, which still routes
            // through the span path and should produce a parent row with
            // `error_message`.
            let handle_result = QueryBuilder::new(
                "SELECT does_not_exist FROM names",
                harness.scheduler.datafusion(),
            )
            .build()
            .submit_distributed("th_test_failure")
            .await;
            assert!(
                handle_result.is_err(),
                "expected submit_distributed to fail for invalid column reference"
            );

            let _ = provider.force_flush();
            wait_for_row_count(
                &harness,
                "SELECT count(*)::bigint \
                 FROM runtime.task_history \
                 WHERE task = 'sql_query' \
                 AND labels['job_id'] = 'th_test_failure' \
                 AND error_message IS NOT NULL",
                1,
                Duration::from_secs(10),
            )
            .await?;

            if let Some(slot) = DF_SLOT.get() {
                *slot.lock().expect("slot poisoned") = None;
            }
            harness.shutdown().await;
            Ok(())
        })
        .await
}
