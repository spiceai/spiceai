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

//! A full-text search over a caching-mode dataset must not answer one principal with rows
//! another principal cached, after a restart as much as before one.
//!
//! A caching-mode acceleration stores every principal's rows in one table, scoped only by its
//! hidden `__spice_cache_namespace` column, and a full-text index carries no such scope. A
//! `text_search` projection that reads the index alone (the row id, a count) is answered
//! without the acceleration's namespace filter, so the startup rebuild of an empty index from
//! the acceleration (#14618) has to leave caching-mode datasets alone.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::Context as _;
use app::AppBuilder;
use app::spicepod::component::runtime::ApiKey;
use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use axum::{Router, routing::get};
use runtime::Runtime;
use runtime_auth::{AuthPrincipalRef, AuthRequestContext};
use runtime_request_context::{Protocol, RequestContext, UserAgent};
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;
use spicepod::semantic::{Column, FullTextSearchConfig};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::utils::{register_test_connectors, run_query, runtime_ready_check};
use crate::{configure_test_datafusion, init_tracing};

const LOAD_TIMEOUT: Duration = Duration::from_mins(2);

/// Serves `[{"word":"zebra","n":<n>}]`, `n` counting the requests served so far.
async fn start_mock() -> (oneshot::Sender<()>, SocketAddr, Arc<AtomicUsize>) {
    let counter = Arc::new(AtomicUsize::new(0));
    let served = Arc::clone(&counter);
    let (tx, rx) = oneshot::channel::<()>();
    let app = Router::new().route(
        "/api/data",
        get(move || {
            let served = Arc::clone(&served);
            async move {
                let n = served.fetch_add(1, Ordering::SeqCst) + 1;
                (
                    [("content-type", "application/json")],
                    format!("[{{\"word\":\"zebra\",\"n\":{n}}}]"),
                )
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock listener");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                rx.await.ok();
            })
            .await
            .unwrap_or_default();
    });
    (tx, addr, counter)
}

/// A request context whose only principal is `api_key`, so its cache namespace is its own.
fn principal(api_key: &str) -> Arc<RequestContext> {
    let ctx = Arc::new(
        RequestContext::builder(Protocol::Http)
            .with_user_agent(UserAgent::from_ua_str("spiceci/fts-caching-restart"))
            .build(),
    );
    let principal: AuthPrincipalRef = Arc::new(ApiKey::ReadOnly {
        key: api_key.to_string(),
    });
    ctx.set_auth_principal(principal)
        .expect("set_auth_principal");
    ctx
}

fn caching_dataset(base_url: &str, db_path: &str) -> Dataset {
    let mut dataset = Dataset::new(base_url, "http_data");
    dataset.params = Some(Params::from_string_map(
        [
            ("file_format".to_string(), "json".to_string()),
            ("allowed_request_paths".to_string(), "/api/data".to_string()),
        ]
        .into_iter()
        .collect(),
    ));
    dataset.columns = vec![
        Column::new("content")
            .with_full_text_search(FullTextSearchConfig::enabled().with_row_id("request_path")),
    ];
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("duckdb".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Caching),
        params: Some(Params::from_string_map(
            [("duckdb_file".to_string(), db_path.to_string())]
                .into_iter()
                .collect(),
        )),
        ..Acceleration::default()
    });
    dataset
}

async fn start(base_url: &str, db_path: &str) -> anyhow::Result<Arc<Runtime>> {
    let mut app = AppBuilder::new("fts_caching_restart")
        .with_dataset(caching_dataset(base_url, db_path))
        .build();
    // Measure the acceleration and the index, not the SQL results cache.
    app.runtime
        .caching
        .sql_results
        .get_or_insert_with(spicepod::component::caching::SQLResultsCacheConfig::default)
        .enabled = false;

    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    let load_rt = Arc::clone(&rt);
    tokio::select! {
        () = tokio::time::sleep(LOAD_TIMEOUT) => anyhow::bail!("timed out loading components"),
        () = load_rt.load_components() => {}
    }
    runtime_ready_check(&rt).await;
    Ok(rt)
}

/// The batches `sql` returns when run as `ctx`, through the query path that attaches `ctx` to
/// the session, which is where the caching scan reads its namespace from.
async fn query_as(
    rt: &Arc<Runtime>,
    ctx: &Arc<RequestContext>,
    sql: &str,
) -> anyhow::Result<Vec<RecordBatch>> {
    Arc::clone(ctx).scope(run_query(rt, sql)).await
}

fn strings(batches: &[RecordBatch], column: &str) -> anyhow::Result<Vec<String>> {
    let mut values = Vec::new();
    for batch in batches {
        let array = batch
            .column_by_name(column)
            .with_context(|| format!("no `{column}` column"))?;
        let array = array
            .as_any()
            .downcast_ref::<StringArray>()
            .with_context(|| format!("`{column}` is not Utf8"))?;
        values.extend((0..array.len()).map(|row| array.value(row).to_string()));
    }
    Ok(values)
}

const SELECT: &str = "SELECT content FROM http_data WHERE request_path = '/api/data'";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn caching_mode_full_text_search_stays_per_principal_after_a_restart() -> anyhow::Result<()> {
    let _tracing = init_tracing(None);
    register_test_connectors().await;
    // Dropping the sender at the end of the test shuts the mock down.
    let (_server, addr, upstream) = start_mock().await;
    let base_url = format!("http://{addr}");
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("cache.db").to_string_lossy().to_string();

    // Dataset registration and refresh run as the runtime does, without a principal; only the
    // queries carry alice's or bob's.
    let admin = Arc::new(
        RequestContext::builder(Protocol::Internal)
            .with_user_agent(UserAgent::from_ua_str("spiceci/fts-caching-restart-admin"))
            .build(),
    );
    let alice = principal("alice-key-aaaaaaaaaaaaaaaaaaaa");
    let bob = principal("bob-key-bbbbbbbbbbbbbbbbbbbbbb");

    Arc::clone(&admin)
        .scope(async {
            let first = start(&base_url, &db_path).await?;
            let cached = strings(&query_as(&first, &alice, SELECT).await?, "content")?;
            assert_eq!(cached, vec![r#"{"word":"zebra","n":1}"#.to_string()]);
            // Cache writes reach the acceleration through a background flush that signals
            // nothing a test can wait on, and polling as alice would refetch on a miss.
            tokio::time::sleep(Duration::from_secs(1)).await;
            first.shutdown().await;
            drop(first);

            let restarted = start(&base_url, &db_path).await?;
            // Answered from the row alice cached before the restart, not from the source: the
            // acceleration held it when the restarted dataset started, so a rebuild would have
            // had it to index.
            assert_eq!(
                strings(&query_as(&restarted, &alice, SELECT).await?, "content")?,
                cached,
                "alice's cached row should survive the restart"
            );
            assert_eq!(
                upstream.load(Ordering::SeqCst),
                1,
                "alice's second read should be served from her cached row"
            );

            // The row id alone is answered from the index; every column is not.
            for sql in [
                "SELECT request_path FROM text_search(http_data, 'zebra', content)",
                "SELECT * FROM text_search(http_data, 'zebra', content)",
            ] {
                let batches = query_as(&restarted, &bob, sql).await?;
                assert_eq!(
                    batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                    0,
                    "bob's `{sql}` must not return the row alice cached:\n{}",
                    arrow::util::pretty::pretty_format_batches(&batches)?
                );
            }

            let counts = query_as(
                &restarted,
                &bob,
                "SELECT count(*) AS n FROM text_search(http_data, 'zebra', content)",
            )
            .await?;
            let count = counts
                .first()
                .and_then(|batch| batch.column(0).as_any().downcast_ref::<Int64Array>())
                .map(|n| n.value(0))
                .context("no count row")?;
            assert_eq!(count, 0, "bob's search must not count the row alice cached");

            restarted.shutdown().await;
            Ok(())
        })
        .await
}
