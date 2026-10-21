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

//! Integration tests for declared-schema deferred dataset
//! initialization on the HTTPS connector.
//!
//! Two coverage paths:
//!
//! 1. **Eager** (no declared columns): the runtime contacts the source
//!    at startup to infer schema, so the request counter is non-zero
//!    *before* any user query runs.
//! 2. **Deferred** (declared columns + `ready_state: on_registration`):
//!    the runtime registers a placeholder at startup with the declared
//!    schema and does not contact the source. The first user query
//!    triggers `ensure_ready`, which calls the connector and swaps the
//!    real provider into the catalog. A second query reuses the swapped
//!    provider without re-contacting the source.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use app::AppBuilder;
use arrow::array::RecordBatch;
use axum::{Router, routing::get};
use futures::TryStreamExt;
use runtime::Runtime;
use spicepod::component::dataset::{Dataset, ReadyState};
use spicepod::param::Params as DatasetParams;
use spicepod::semantic::Column;
use tokio::net::TcpListener;

use crate::utils::{register_test_connectors, runtime_ready_check, test_request_context};
use crate::{configure_test_datafusion, init_tracing};

const COUNTED_ITEMS_CSV: &str = "id,name,price\n1,Widget,9.99\n2,Gadget,19.99\n3,Doohickey,4.99\n";

/// Spin up a tiny axum server that serves a single CSV at
/// `/data/items.csv` and counts requests. Returns the bind address,
/// the request counter, and a shutdown handle.
async fn start_counted_csv_server() -> Result<
    (
        tokio::sync::oneshot::Sender<()>,
        SocketAddr,
        Arc<AtomicUsize>,
    ),
    String,
> {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_route = Arc::clone(&counter);

    let app = Router::new().route(
        "/data/items.csv",
        get(move || {
            let counter = Arc::clone(&counter_route);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                ([("content-type", "text/csv")], COUNTED_ITEMS_CSV)
            }
        }),
    );

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| format!("bind: {e}"))?;
    let addr = listener.local_addr().map_err(|e| format!("addr: {e}"))?;

    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                rx.await.ok();
            })
            .await
            .unwrap_or_default();
    });

    Ok((tx, addr, counter))
}

async fn build_runtime(app: app::App) -> Result<Arc<Runtime>, String> {
    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    let rt_for_load = Arc::clone(&rt);
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
            return Err("Timed out waiting for components to load".to_string());
        }
        () = rt_for_load.load_components() => {}
    }
    runtime_ready_check(&rt).await;
    Ok(rt)
}

async fn run_count_query(rt: &Runtime, sql: &str) -> Result<i64, String> {
    let result = rt
        .datafusion()
        .query_builder(sql)
        .build()
        .run()
        .await
        .map_err(|e| format!("query `{sql}`: {e}"))?;
    let batches: Vec<RecordBatch> = result
        .data
        .try_collect()
        .await
        .map_err(|e| format!("collect `{sql}`: {e}"))?;
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    Ok(i64::try_from(total).unwrap_or(i64::MAX))
}

/// Without declared columns, the dataset takes the eager path and the
/// HTTP source is contacted at startup so `DataFusion` can infer the
/// CSV schema.
#[tokio::test]
async fn http_eager_contacts_source_at_startup_without_columns() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let (tx, addr, counter) = start_counted_csv_server().await?;

            let mut dataset = Dataset::new(format!("http://{addr}/data/items.csv"), "items_eager");
            dataset.params = Some(DatasetParams::from_string_map(HashMap::from([(
                "file_format".to_string(),
                "csv".to_string(),
            )])));

            let app = AppBuilder::new("http_eager_no_columns")
                .with_dataset(dataset)
                .build();
            let rt = build_runtime(app).await?;

            let after_startup = counter.load(Ordering::SeqCst);
            assert!(
                after_startup >= 1,
                "eager path must contact the source at startup; \
                 expected counter >= 1, got {after_startup}"
            );

            // Functional sanity: the table is queryable.
            let rows = run_count_query(&rt, "SELECT COUNT(*) FROM items_eager").await?;
            assert_eq!(rows, 1, "COUNT(*) should return one row");

            tx.send(()).map_err(|()| "shutdown".to_string())?;
            Ok(())
        })
        .await
}

/// With declared columns + `ready_state: on_registration`, the dataset
/// takes the deferred path. The HTTP source is **not** contacted at
/// startup; the first query triggers materialisation, and subsequent
/// queries reuse the swapped-in provider without further factory
/// invocations.
#[tokio::test]
async fn http_deferred_does_not_contact_source_until_first_query() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let (tx, addr, counter) = start_counted_csv_server().await?;

            let mut dataset =
                Dataset::new(format!("http://{addr}/data/items.csv"), "items_deferred");
            dataset.params = Some(DatasetParams::from_string_map(HashMap::from([(
                "file_format".to_string(),
                "csv".to_string(),
            )])));
            dataset.ready_state = ReadyState::OnRegistration;
            dataset.columns = vec![
                Column::new("id").with_type("bigint"),
                Column::new("name").with_type("text"),
                Column::new("price").with_type("double precision"),
            ];

            let app = AppBuilder::new("http_deferred_with_columns")
                .with_dataset(dataset)
                .build();
            let rt = build_runtime(app).await?;

            let after_startup = counter.load(Ordering::SeqCst);
            assert_eq!(
                after_startup, 0,
                "deferred path must NOT contact the source at startup; \
                 got counter = {after_startup}"
            );
            assert!(
                rt.datafusion().has_pending_initializations(),
                "runtime should report a pending deferred initialisation"
            );

            // First query: triggers ensure_ready, which calls the
            // connector. The exact request count depends on how the
            // listing connector primes itself, but it must be > 0.
            let rows = run_count_query(&rt, "SELECT COUNT(*) FROM items_deferred").await?;
            assert_eq!(rows, 1, "COUNT(*) should return one row");
            let after_first = counter.load(Ordering::SeqCst);
            assert!(
                after_first >= 1,
                "first query must contact the source; \
                 counter went {after_startup} -> {after_first}"
            );
            assert!(
                !rt.datafusion().has_pending_initializations(),
                "pending initialisation counter should drop to zero after first query"
            );

            // Second query: the placeholder has been swapped for the
            // real provider, so we should NOT see another bring-up.
            // We can't assert "0 new requests" because DataFusion may
            // still re-read the file for the actual scan, but we can
            // assert that a brand-new connector construction (which
            // would re-run schema inference / object-store probing on
            // the listing path) does not happen \u2014 the pending
            // initialisation counter must stay at zero.
            let rows =
                run_count_query(&rt, "SELECT id, name FROM items_deferred ORDER BY id").await?;
            assert_eq!(rows, 3, "select-all should return three rows");
            assert!(
                !rt.datafusion().has_pending_initializations(),
                "second query must not reintroduce a pending initialisation"
            );

            tx.send(()).map_err(|()| "shutdown".to_string())?;
            Ok(())
        })
        .await
}

/// Schema-mismatch contract: when declared columns disagree with the
/// real source schema, the first query fails fast with a structured
/// error and the dataset stays unusable rather than silently exposing
/// the source's schema (which would diverge from what was already
/// published to the catalog at registration time).
#[tokio::test]
async fn http_deferred_schema_mismatch_fails_fast_on_first_query() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let (tx, addr, _counter) = start_counted_csv_server().await?;

            let mut dataset =
                Dataset::new(format!("http://{addr}/data/items.csv"), "items_mismatch");
            dataset.params = Some(DatasetParams::from_string_map(HashMap::from([(
                "file_format".to_string(),
                "csv".to_string(),
            )])));
            dataset.ready_state = ReadyState::OnRegistration;
            // Wrong type for `price` (declared as bigint, real is float64).
            dataset.columns = vec![
                Column::new("id").with_type("bigint"),
                Column::new("name").with_type("text"),
                Column::new("price").with_type("bigint"),
            ];

            let app = AppBuilder::new("http_deferred_mismatch")
                .with_dataset(dataset)
                .build();
            let rt = build_runtime(app).await?;

            let result = rt
                .datafusion()
                .query_builder("SELECT COUNT(*) FROM items_mismatch")
                .build()
                .run()
                .await;

            let err = match result {
                Ok(stream) => {
                    // Some errors only surface during streaming.
                    let collected: Result<Vec<RecordBatch>, _> = stream.data.try_collect().await;
                    match collected {
                        Ok(_) => return Err("schema mismatch must fail the query".to_string()),
                        Err(e) => e.to_string(),
                    }
                }
                Err(e) => e.to_string(),
            };

            assert!(
                err.contains("Declared schema does not match source schema")
                    || err.contains("schema"),
                "expected schema-mismatch error, got: {err}"
            );

            tx.send(()).map_err(|()| "shutdown".to_string())?;
            Ok(())
        })
        .await
}
