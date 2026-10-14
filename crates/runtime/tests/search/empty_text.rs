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

//! HTTP-level regression: `/v1/search` rejects empty and whitespace-only `text`
//! with exactly HTTP 400 and body `Search text must not be empty`.
//!
//! Validation runs before the search engine is touched, so a minimal runtime
//! (no embedding model) is enough.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use app::AppBuilder;
use http::header::{ACCEPT, CONTENT_TYPE};
use reqwest::{Client, StatusCode, header::HeaderMap, header::HeaderValue};
use runtime::{Runtime, auth::EndpointAuth, config::Config};
use serde_json::json;

use crate::utils::{
    register_test_connectors, runtime_ready_check, test_request_context, wait_until_true,
};
use crate::{configure_test_datafusion, init_tracing};

fn create_api_bindings_config() -> Config {
    let localhost = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let http_listener =
        std::net::TcpListener::bind(SocketAddr::new(localhost, 0)).expect("bind http listener");
    let http_port = http_listener
        .local_addr()
        .expect("http listener addr")
        .port();
    drop(http_listener);

    let flight_listener =
        std::net::TcpListener::bind(SocketAddr::new(localhost, 0)).expect("bind flight listener");
    let flight_port = flight_listener
        .local_addr()
        .expect("flight listener addr")
        .port();
    drop(flight_listener);

    Config::new()
        .with_http_bind_address(SocketAddr::new(localhost, http_port))
        .with_flight_bind_address(SocketAddr::new(localhost, flight_port))
}

async fn start_runtime_http_server(rt: Arc<Runtime>) -> Result<String, String> {
    let api_config = create_api_bindings_config();
    let http_base_url = format!("http://{}", api_config.http_bind_address);
    let health_url = format!("{http_base_url}/health");
    let server_rt = Arc::clone(&rt);

    tokio::spawn(async move {
        if let Err(e) = server_rt
            .start_servers(api_config, None, EndpointAuth::no_auth())
            .await
        {
            tracing::error!("Test runtime server failed to start: {e}");
        }
    });

    let client = Client::new();
    let is_started = wait_until_true(Duration::from_secs(10), || {
        let client = client.clone();
        let health_url = health_url.clone();
        async move {
            client
                .get(&health_url)
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
        }
    })
    .await;

    if is_started {
        Ok(http_base_url)
    } else {
        Err("Timed out waiting for runtime HTTP server to start".to_string())
    }
}

async fn post_search(base_url: &str, body: serde_json::Value) -> (StatusCode, String) {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

    let response = Client::new()
        .post(format!("{base_url}/v1/search"))
        .headers(headers)
        .body(body.to_string())
        .send()
        .await
        .expect("request reaches /v1/search");

    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    (status, text)
}

#[tokio::test]
async fn search_rejects_empty_and_whitespace_text_with_http_400() -> Result<(), String> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    register_test_connectors().await;
    configure_test_datafusion();

    test_request_context()
        .scope(async {
            let rt = Arc::new(
                Runtime::builder()
                    .with_app(AppBuilder::new("search_empty_text").build())
                    .build()
                    .await,
            );
            Arc::clone(&rt).load_components().await;
            runtime_ready_check(&rt).await;

            let base_url = start_runtime_http_server(Arc::clone(&rt)).await?;

            for text in ["", "   ", "\t\n "] {
                let (status, body) = post_search(
                    &base_url,
                    json!({
                        "datasets": ["products"],
                        "text": text,
                        "limit": 3
                    }),
                )
                .await;
                assert_eq!(
                    status,
                    StatusCode::BAD_REQUEST,
                    "text={text:?} status={status} body={body}"
                );
                assert_eq!(
                    body, "Search text must not be empty",
                    "text={text:?} body={body}"
                );
            }

            rt.shutdown().await;
            Ok(())
        })
        .await
}
