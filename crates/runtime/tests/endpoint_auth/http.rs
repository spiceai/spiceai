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
use rand::Rng;
use runtime::{auth::EndpointAuth, config::Config, Runtime};
use runtime_auth::{api_key::ApiKeyAuth, HttpAuth};
use spicepod::component::runtime::ApiKey;

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

#[tokio::test]
async fn test_http_auth() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );

    test_request_context().scope(async {
        let span = tracing::info_span!("test_http_auth");
        let _span_guard = span.enter();

        let mut rng = rand::thread_rng();
        let http_port: u16 = rng.gen_range(50000..60000);
        let flight_port: u16 = http_port + 1;
        let otel_port: u16 = http_port + 2;
        let metrics_port: u16 = http_port + 3;

        tracing::debug!(
            "Ports: http: {http_port}, flight: {flight_port}, otel: {otel_port}, metrics: {metrics_port}"
        );

        let api_config = Config::new()
            .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
            .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port))
            .with_open_telemetry_bind_address(SocketAddr::new(LOCALHOST, otel_port));

        let registry = prometheus::Registry::new();

        let rt = Runtime::builder()
            .with_metrics_server(SocketAddr::new(LOCALHOST, metrics_port), registry)
            .build()
            .await;

        let api_key_auth =
            Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str("valid")])) as Arc<dyn HttpAuth + Send + Sync>;

        // Start the servers
        tokio::spawn(async move {
            Box::pin(Arc::new(rt).start_servers(
                api_config,
                None,
                EndpointAuth::default().with_http_auth(api_key_auth),
            ))
            .await
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

        // HTTP is not authenticated
        let http_url = format!("http://127.0.0.1:{http_port}/health");
        let response = http_client
            .get(&http_url)
            .send()
            .await
            .expect("valid response");
        assert!(response.status().is_success());
        tracing::info!("HTTP health check passed");

        // Ready API is not authenticated
        let http_url = format!("http://127.0.0.1:{http_port}/v1/ready");
        let response = http_client
            .get(&http_url)
            .send()
            .await
            .expect("valid response");
        assert!(response.status().is_success());
        tracing::info!("HTTP health check passed");

        // Metrics API is not authenticated
        let metrics_url = format!("http://127.0.0.1:{metrics_port}/");
        let response = http_client
            .get(&metrics_url)
            .send()
            .await
            .expect("valid response");
        assert!(response.status().is_success());
        tracing::info!("Metrics health check passed");

        // v1/status is authenticated
        let status_url = format!("http://127.0.0.1:{http_port}/v1/status?format=json");
        let response = http_client
            .get(&status_url)
            .send()
            .await
            .expect("valid response");
        assert!(!response.status().is_success());
        assert_eq!(response.status().as_u16(), 401);

        // Test valid API key
        let response = http_client
            .get(&status_url)
            .header("x-api-key", "valid")
            .send()
            .await
            .expect("valid response");
        assert!(response.status().is_success());
        assert_eq!(response.status().as_u16(), 200);

        // Test invalid API key
        let response = http_client
            .get(&status_url)
            .header("x-api-key", "invalid")
            .send()
            .await
            .expect("valid response");
        assert!(!response.status().is_success());
        assert_eq!(response.status().as_u16(), 401);

        Ok(())
    }).await
}
