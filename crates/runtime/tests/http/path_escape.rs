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

//! A `request_path` that matches `allowed_request_paths` as written must not reach a
//! different path on the origin once URL parsing normalizes it — neither a path outside the
//! allowed patterns nor one outside the path in the dataset's `from` URL.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use app::AppBuilder;
use arrow::array::RecordBatch;
use axum::{Router, extract::State, http::Uri};
use futures::TryStreamExt;
use runtime::{Runtime, datafusion::query::write_to_json_value};
use spicepod::{component::dataset::Dataset, param::Params as DatasetParams};
use tokio::net::TcpListener;

use super::load_runtime;
use crate::init_tracing;
use crate::utils::{register_test_connectors, test_request_context};

type RequestedPaths = Arc<Mutex<Vec<String>>>;

/// An origin that answers every path and records each one it was asked for, so the test
/// sees the path the request actually went to rather than the one the query named.
async fn start_recording_server()
-> Result<(tokio::sync::oneshot::Sender<()>, SocketAddr, RequestedPaths), String> {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let requested: RequestedPaths = Arc::new(Mutex::new(Vec::new()));

    let app = Router::new()
        .fallback(
            |State(requested): State<RequestedPaths>, uri: Uri| async move {
                let path = uri.path().to_string();
                requested
                    .lock()
                    .expect("requested-paths lock is not poisoned")
                    .push(path.clone());
                (
                    [("content-type", "application/json")],
                    serde_json::json!({ "served_path": path }).to_string(),
                )
            },
        )
        .with_state(Arc::clone(&requested));

    let tcp_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| e.to_string())?;
    let addr = tcp_listener.local_addr().map_err(|e| e.to_string())?;
    tokio::spawn(async move {
        axum::serve(tcp_listener, app)
            .with_graceful_shutdown(async {
                rx.await.ok();
            })
            .await
            .unwrap_or_default();
    });

    Ok((tx, addr, requested))
}

async fn query(rt: &Runtime, request_path: &str) -> Result<Vec<RecordBatch>, String> {
    let sql = format!(
        "SELECT content FROM shows WHERE request_path = '{}'",
        request_path.replace('\'', "''")
    );
    let result = rt
        .datafusion()
        .query_builder(&sql)
        .build()
        .run()
        .await
        .map_err(|e| e.to_string())?;
    result
        .data
        .try_collect::<Vec<RecordBatch>>()
        .await
        .map_err(|e| e.to_string())
}

/// The paths requested since the last call, without the connector's own availability
/// probes, which run in the background and are not driven by any query.
fn take_requested(requested: &RequestedPaths) -> Vec<String> {
    std::mem::take(
        &mut *requested
            .lock()
            .expect("requested-paths lock is not poisoned"),
    )
    .into_iter()
    .filter(|path| !path.starts_with("/__spice_health_check"))
    .collect()
}

#[tokio::test]
async fn test_http_request_path_cannot_escape_allowed_paths() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let (tx, addr, requested) = start_recording_server().await?;

            let mut dataset = Dataset::new(format!("http://{addr}/api/v1"), "shows");
            dataset.params = Some(DatasetParams::from_string_map(HashMap::from([
                ("file_format".to_string(), "json".to_string()),
                ("allowed_request_paths".to_string(), "/shows/**".to_string()),
            ])));
            let rt = load_runtime(AppBuilder::new("http_path_escape").with_dataset(dataset).build())
                .await?;
            take_requested(&requested);

            // The allowed path is fetched, under the base path: the origin is reachable and
            // the rejections below are the validation, not a broken dataset.
            let batches = query(&rt, "/shows/1").await?;
            let rows = write_to_json_value(&batches).map_err(|e| e.to_string())?;
            assert_eq!(
                rows,
                serde_json::json!([{ "content": "{\"served_path\":\"/api/v1/shows/1\"}" }]),
            );
            assert_eq!(take_requested(&requested), vec!["/api/v1/shows/1"]);

            // Asserted in full: the error reaches logs and query history, so it must not
            // repeat the path, which can carry identifiers or tokens, or a newline from it.
            let dot_segment = "Failed to execute query: Error during planning: The 'request_path' value contains a '.' or '..' segment, including a percent-encoded one such as '%2e%2e', which is not allowed for security reasons. Remove the segment from the path.";
            let rewritten = "Failed to execute query: Error during planning: The 'request_path' value would be changed before the request is sent, because URLs cannot contain tabs or newlines and treat '\\' as '/'. Remove those characters, using '/' to separate path segments.";
            // Each matches `/shows/**` as written. Without the check, URL parsing sends the
            // first six to `/api/v1/people/1` and the last to `/admin`, outside the base path.
            let escapes = [
                ("/shows/%2e%2e/people/1", dot_segment),
                ("/shows/%2E%2E/people/1", dot_segment),
                ("/shows/.%2e/people/1", dot_segment),
                ("/shows/%2e./people/1", dot_segment),
                ("/shows/.\t./people/1", rewritten),
                ("/shows/.\n./people/1", rewritten),
                ("/shows/%2e%2e/%2e%2e/%2e%2e/admin", dot_segment),
            ];
            for (raw, expected) in escapes {
                match query(&rt, raw).await {
                    Err(message) => assert_eq!(message, expected, "request_path {raw:?}"),
                    Ok(batches) => panic!(
                        "request_path {raw:?} must be rejected, got {:?}",
                        write_to_json_value(&batches)
                    ),
                }
            }
            assert_eq!(
                take_requested(&requested),
                Vec::<String>::new(),
                "a rejected request_path must not reach the origin"
            );

            tx.send(())
                .map_err(|()| "Failed to send shutdown signal".to_string())?;
            Ok(())
        })
        .await
}
