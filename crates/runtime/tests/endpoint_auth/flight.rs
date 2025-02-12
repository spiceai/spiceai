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
use arrow_flight::{error::FlightError, flight_service_client::FlightServiceClient};
use flightrepl::cache_control;
use rand::Rng;
use runtime::{auth::EndpointAuth, config::Config, Runtime};
use runtime_auth::{api_key::ApiKeyAuth, FlightBasicAuth};
use spicepod::component::runtime::ApiKey;
use tonic::transport::Channel;

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

#[tokio::test]
async fn test_flight_auth() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context().scope(async {
        let span = tracing::info_span!("test_flight_auth");
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

        let api_key_auth = Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str("valid")]))
            as Arc<dyn FlightBasicAuth + Send + Sync>;

        // Start the servers
        tokio::spawn(async move {
            Box::pin(Arc::new(rt).start_servers(
                api_config,
                None,
                EndpointAuth::default().with_flight_basic_auth(api_key_auth),
            ))
            .await
        });

        // Wait for the servers to start
        tracing::info!("Waiting for servers to start...");
        wait_until_true(Duration::from_secs(10), || async {
            reqwest::get(format!("http://localhost:{http_port}/health"))
                .await
                .is_ok()
        })
        .await;

        let channel = Channel::from_shared(format!("http://localhost:{flight_port}"))?
            .connect()
            .await
            .expect("to connect to flight endpoint");
        let client = FlightServiceClient::new(channel);

        let result = flightrepl::get_records(
            client.clone(),
            "SELECT 1",
            Some(&"valid".to_string()),
            &format!("spiceci/{}", env!("CARGO_PKG_VERSION")),
            cache_control::CacheControl::Cache,
        )
        .await;
        assert!(result.is_ok());

        let Err(e) = flightrepl::get_records(
            client,
            "SELECT 1",
            None,
            &format!("spiceci/{}", env!("CARGO_PKG_VERSION")),
            cache_control::CacheControl::Cache,
        )
        .await
        else {
            panic!("expected error");
        };
        match e {
            FlightError::Tonic(status) => {
                assert_eq!(status.code(), tonic::Code::Unauthenticated);
            }
            _ => panic!("expected tonic error"),
        }

        Ok(())
    }).await
}
