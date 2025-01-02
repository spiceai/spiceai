/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use crate::{
    init_tracing,
    utils::{test_request_context, wait_until_true},
};
use app::AppBuilder;
use rand::Rng;
use runtime::{auth::EndpointAuth, config::Config, Runtime};
use spicepod::component::runtime::CorsConfig;

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

#[tokio::test]
async fn test_enabled_cors_endpoints() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context()
        .scope(async {
            let span = tracing::info_span!("test_cors_endpoints");
            let _span_guard = span.enter();

            let mut rng = rand::thread_rng();
            let http_port: u16 = rng.gen_range(50000..60000);
            let flight_port: u16 = http_port + 1;
            let otel_port: u16 = http_port + 2;
            let metrics_port: u16 = http_port + 3;

            tracing::debug!(
                "CORS Ports: http: {http_port}, flight: {flight_port}, otel: {otel_port}, metrics: {metrics_port}"
            );

            let api_config = Config::new()
                .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
                .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port))
                .with_open_telemetry_bind_address(SocketAddr::new(LOCALHOST, otel_port));

            let rt = Runtime::builder()
                .with_app(
                    AppBuilder::new("test")
                        .with_cors_config(CorsConfig {
                            enabled: true,
                            allowed_origins: vec!["*".to_string()],
                        })
                        .build(),
                )
                .build()
                .await;

            // Start the servers
            tokio::spawn(async move {
                Box::pin(Arc::new(rt).start_servers(api_config, None, EndpointAuth::no_auth())).await
            });

            let http_client = reqwest::Client::builder().build()?;

            // Wait for the servers to start
            tracing::info!("Waiting for servers to start...");
            wait_until_true(Duration::from_secs(10), || async {
                http_client
                    .get(format!("http://localhost:{http_port}/health"))
                    .send()
                    .await
                    .is_ok()
            })
            .await;

            let http_url = format!("http://127.0.0.1:{http_port}/health");
            let request_builder = http_client.request(http::Method::OPTIONS, &http_url);
            let response = request_builder.send().await.expect("valid response");
            assert!(response.status().is_success());
            tracing::info!("HTTP health check passed");

            // Verify that the CORS headers are set
            let cors_origin_header = response
                .headers()
                .get("access-control-allow-origin")
                .expect("cors header is present");
            assert_eq!(cors_origin_header, "*");

            let cors_allow_methods_header = response
                .headers()
                .get("access-control-allow-methods")
                .expect("cors header is present");
            assert_eq!(cors_allow_methods_header, "GET,POST,PATCH");

            Ok(())
        }).await
}

#[tokio::test]
async fn test_disabled_cors_endpoints() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context().scope(async {
        let span = tracing::info_span!("test_disabled_cors_endpoints");
        let _span_guard = span.enter();

        let mut rng = rand::thread_rng();
        let http_port: u16 = rng.gen_range(50000..60000);
        let flight_port: u16 = http_port + 1;
        let otel_port: u16 = http_port + 2;
        let metrics_port: u16 = http_port + 3;

        tracing::debug!(
            "CORS Ports: http: {http_port}, flight: {flight_port}, otel: {otel_port}, metrics: {metrics_port}"
        );

        let api_config = Config::new()
            .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
            .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port))
            .with_open_telemetry_bind_address(SocketAddr::new(LOCALHOST, otel_port));

        // Default cors config is disabled
        let rt = Runtime::builder()
            .with_app(AppBuilder::new("test").build())
            .build()
            .await;

        // Start the servers
        tokio::spawn(async move {
            Box::pin(Arc::new(rt).start_servers(api_config, None, EndpointAuth::no_auth())).await
        });

        let http_client = reqwest::Client::builder().build()?;

        // Wait for the servers to start
        tracing::info!("Waiting for servers to start...");
        wait_until_true(Duration::from_secs(10), || async {
            http_client
                .get(format!("http://localhost:{http_port}/health"))
                .send()
                .await
                .is_ok()
        })
        .await;

        let http_url = format!("http://127.0.0.1:{http_port}/health");
        let request_builder = http_client.request(http::Method::OPTIONS, &http_url);
        let response = request_builder.send().await.expect("valid response");
        assert!(response.status().is_success());
        tracing::info!("HTTP health check passed");

        // Verify that the CORS headers are set
        assert!(response
            .headers()
            .get("access-control-allow-origin")
            .is_none());

        assert!(response
            .headers()
            .get("access-control-allow-methods")
            .is_none());

        Ok(())
    }).await
}
