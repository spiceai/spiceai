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

//! A distributed task waiting on a slow HTTP source lends its executor task
//! slot, so another query runs in it before the response arrives
//! (spiceai/spiceai#15049).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use app::AppBuilder;
use arrow::array::{RecordBatch, StringArray};
use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use futures::{FutureExt, TryStreamExt};
use runtime::datafusion::query::QueryBuilder;
use serde_json::{Value, json};
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::{configure_test_datafusion, init_tracing, utils::test_request_context};

use super::harness::ClusterHarness;

/// How long the fast query may take once every slot holds a waiting task.
/// Without lending it can never get a slot, because the slow responses are
/// held until it finishes.
const FAST_QUERY_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct FixtureState {
    /// Until set, `/slow` answers at once, so loading the dataset is not held.
    hold: Arc<AtomicBool>,
    /// Slow requests that have arrived and not been answered.
    slow_in_flight: Arc<AtomicUsize>,
    /// Flips to `true` when the test lets the slow responses go.
    release: watch::Receiver<bool>,
}

struct HttpFixture {
    addr: SocketAddr,
    hold: Arc<AtomicBool>,
    slow_in_flight: Arc<AtomicUsize>,
    release: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

impl HttpFixture {
    async fn start() -> Result<Self, anyhow::Error> {
        let slow_in_flight = Arc::new(AtomicUsize::new(0));
        let hold = Arc::new(AtomicBool::new(false));
        let (release, release_rx) = watch::channel(false);
        let state = FixtureState {
            hold: Arc::clone(&hold),
            slow_in_flight: Arc::clone(&slow_in_flight),
            release: release_rx,
        };
        let app = Router::new()
            .route("/health", get(|| async { "ok" }))
            .route("/slow", get(slow_rows))
            .route(
                "/fast",
                get(|| async { Json(vec![json!({"id": 1, "label": "fast"})]) }),
            )
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Ok(Self {
            addr,
            hold,
            slow_in_flight,
            release,
            handle,
        })
    }

    /// Polls until `n` slow requests are in flight, or fails with the last
    /// count seen.
    async fn wait_for_slow_in_flight(
        &self,
        n: usize,
        timeout: Duration,
    ) -> Result<(), anyhow::Error> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let seen = self.slow_in_flight.load(Ordering::SeqCst);
            if seen >= n {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!(
                    "only {seen} of {n} slow requests reached the origin within {timeout:?}"
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

impl Drop for HttpFixture {
    fn drop(&mut self) {
        let _ = self.release.send(true);
        self.handle.abort();
    }
}

/// Answers once the test releases it, holding its caller's task on the
/// origin until then.
async fn slow_rows(State(state): State<FixtureState>) -> Json<Vec<Value>> {
    let rows = Json(vec![json!({"id": 2, "label": "slow"})]);
    if !state.hold.load(Ordering::SeqCst) {
        return rows;
    }
    state.slow_in_flight.fetch_add(1, Ordering::SeqCst);
    let mut release = state.release.clone();
    let _ = release.wait_for(|released| *released).await;
    state.slow_in_flight.fetch_sub(1, Ordering::SeqCst);
    rows
}

fn http_dataset(addr: SocketAddr, path: &str, name: &str) -> Dataset {
    let mut dataset = Dataset::new(format!("http://{addr}/{path}"), name);
    dataset.params = Some(Params::from_string_map(HashMap::from([
        ("file_format".to_string(), "json".to_string()),
        // A dynamic JSON API dataset, not a static file.
        ("request_query_filters".to_string(), "enabled".to_string()),
        ("health_probe".to_string(), "/health".to_string()),
        ("client_timeout".to_string(), "120".to_string()),
        ("max_retries".to_string(), "0".to_string()),
        ("response_cache_max_size_bytes".to_string(), "0".to_string()),
    ])));
    dataset
}

async fn distributed_query(
    harness: &ClusterHarness,
    sql: &str,
    job_name: &str,
) -> Result<Vec<RecordBatch>, anyhow::Error> {
    let handle = QueryBuilder::new(sql, harness.scheduler.datafusion())
        .build()
        .submit_distributed(job_name)
        .await
        .map_err(|error| anyhow::anyhow!("submit distributed query {job_name}: {error}"))?;
    handle
        .into_stream()
        .await
        .map_err(|error| anyhow::anyhow!("open distributed query stream {job_name}: {error}"))?
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|error| anyhow::anyhow!("collect distributed query {job_name}: {error}"))
}

fn content(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("content")
                .expect("result should contain content")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("content should be Utf8")
                .iter()
                .map(|value| value.expect("content should be non-null").to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn run_with_harness<F>(harness: ClusterHarness, f: F) -> Result<(), anyhow::Error>
where
    F: for<'a> FnOnce(
        &'a ClusterHarness,
    )
        -> Pin<Box<dyn std::future::Future<Output = Result<(), anyhow::Error>> + 'a>>,
{
    let result = AssertUnwindSafe(f(&harness)).catch_unwind().await;
    harness.shutdown().await;
    match result {
        Ok(inner) => inner,
        Err(panic_payload) => std::panic::resume_unwind(panic_payload),
    }
}

/// Fills every executor slot with a query whose task waits on `/slow`, then
/// runs a fast query, which can only find a slot if a waiting task lends one.
/// `slow_sql(i)` is the i-th slow query; each is distinct, so none is answered
/// from a cache instead of reaching the origin.
async fn lends_slots_while_waiting(slow_sql: fn(usize) -> String) -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async move {
            configure_test_datafusion();
            let fixture = Arc::new(HttpFixture::start().await?);

            let harness = ClusterHarness::builder()
                .scheduler(
                    AppBuilder::new("io_wait_lending")
                        .with_dataset(http_dataset(fixture.addr, "slow", "slow_api"))
                        .with_dataset(http_dataset(fixture.addr, "fast", "fast_api"))
                        .build(),
                )
                .executors(1)
                .start()
                .await?;
            run_with_harness(harness, |harness| {
                let fixture = Arc::clone(&fixture);
                Box::pin(async move {
                    harness.wait_for_executors(Duration::from_secs(30)).await?;
                    fixture.hold.store(true, Ordering::SeqCst);
                    // Read only once the runtimes have installed the CPU budget.
                    let slots = cpu_budget::cpu_budget().cluster_executor_concurrent_tasks();

                    // One slow query per slot: once all their requests are at
                    // the origin, every slot holds a task waiting on HTTP.
                    let slow: Vec<(String, String)> = (0..slots)
                        .map(|i| (slow_sql(i), format!("io_wait_slow_{i}")))
                        .collect();
                    let slow_queries = slow
                        .iter()
                        .map(|(sql, name)| distributed_query(harness, sql, name))
                        .collect::<Vec<_>>();
                    let slow_queries = futures::future::join_all(slow_queries);
                    tokio::pin!(slow_queries);

                    let fast = async {
                        fixture
                            .wait_for_slow_in_flight(slots, Duration::from_secs(60))
                            .await?;
                        let fast = tokio::time::timeout(
                            FAST_QUERY_TIMEOUT,
                            distributed_query(harness, "SELECT content FROM fast_api", "io_wait_fast"),
                        )
                        .await
                        .map_err(|_| {
                            anyhow::anyhow!(
                                "the fast query found no free slot within {FAST_QUERY_TIMEOUT:?} while {} slow requests waited on the origin: tasks waiting on HTTP did not lend their slots",
                                fixture.slow_in_flight.load(Ordering::SeqCst)
                            )
                        })??;
                        // Still held: the fast query ran in a lent slot, not
                        // one a finished slow query gave back.
                        assert_eq!(fixture.slow_in_flight.load(Ordering::SeqCst), slots);
                        let _ = fixture.release.send(true);
                        Ok::<_, anyhow::Error>(fast)
                    };

                    // Drive the slow queries alongside, so their tasks are
                    // submitted and polled while the fast one runs.
                    let (fast, slow) = tokio::join!(fast, &mut slow_queries);
                    let fast = fast?;
                    assert_eq!(content(&fast), [r#"{"id":1,"label":"fast"}"#]);
                    for result in slow {
                        assert_eq!(content(&result?), [r#"{"id":2,"label":"slow"}"#]);
                    }
                    Ok(())
                })
            })
            .await?;
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn a_task_waiting_on_http_lends_its_slot_to_another_query() -> Result<(), anyhow::Error> {
    // The scan is polled on tasks the stage's shuffle writer spawns.
    lends_slots_while_waiting(|i| format!("SELECT content, {i} AS q FROM slow_api")).await
}
