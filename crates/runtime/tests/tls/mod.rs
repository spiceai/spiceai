/*
Copyright 2024 The Spice.ai OSS Authors

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
};

use crate::init_tracing;
use arrow_flight::{
    flight_service_client::FlightServiceClient,
    sql::{CommandStatementQuery, ProstMessageExt},
    FlightDescriptor,
};
use prost::Message;
use rand::Rng;
use runtime::{config::Config, tls::TlsConfig, Runtime};
use rustls::crypto::{self, CryptoProvider};
use tonic::transport::Channel;
use tonic_health::pb::health_client::HealthClient;

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

#[tokio::test]
async fn test_tls_endpoints() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    CryptoProvider::install_default(crypto::aws_lc_rs::default_provider())
        .expect("valid crypto provider");

    let span = tracing::info_span!("test_tls_endpoints");
    let _span_guard = span.enter();

    let mut rng = rand::thread_rng();
    let http_port: u16 = rng.gen_range(50000..60000);
    let flight_port: u16 = http_port + 1;
    let otel_port: u16 = http_port + 2;
    let metrics_port: u16 = http_port + 3;

    tracing::debug!(
        "TLS Ports: http: {http_port}, flight: {flight_port}, otel: {otel_port}, metrics: {metrics_port}"
    );

    let cert_bytes = include_bytes!("../../../../test/tls/spiced_cert.pem").to_vec();
    let key_bytes = include_bytes!("../../../../test/tls/spiced_key.pem").to_vec();

    let api_config = Config::new()
        .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
        .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port))
        .with_open_telemetry_bind_address(SocketAddr::new(LOCALHOST, otel_port));
    let tls_config = TlsConfig::try_new(cert_bytes.clone(), key_bytes).expect("valid TlsConfig");

    let registry = prometheus::Registry::new();

    let rt = Runtime::builder()
        .with_metrics_server(SocketAddr::new(LOCALHOST, metrics_port), registry)
        .build()
        .await;

    // Start the servers
    tokio::spawn(async move {
        rt.start_servers(api_config, Some(Arc::new(tls_config)))
            .await
    });

    // Wait for the servers to start
    tracing::info!("Waiting for servers to start...");
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Connect to the servers with TLS
    let root_cert_bytes = include_bytes!("../../../../test/tls/spiced_root_cert.pem").to_vec();
    let root_cert_reqwest =
        reqwest::tls::Certificate::from_pem(&root_cert_bytes).expect("valid certificate");
    let http_client = reqwest::Client::builder()
        .use_rustls_tls()
        .tls_built_in_root_certs(false)
        .add_root_certificate(root_cert_reqwest)
        .build()?;

    // HTTP
    let http_url = format!("https://127.0.0.1:{http_port}/health");
    let response = http_client
        .get(&http_url)
        .send()
        .await
        .expect("valid response");
    assert!(response.status().is_success());
    tracing::info!("HTTP health check passed");

    // METRICS
    let metrics_url = format!("https://127.0.0.1:{metrics_port}/health");
    let response = http_client
        .get(&metrics_url)
        .send()
        .await
        .expect("valid response");
    assert!(response.status().is_success());
    tracing::info!("Metrics health check passed");

    // FLIGHT (GRPC)
    let root_cert_tonic = tonic::transport::Certificate::from_pem(&root_cert_bytes);
    let channel = Channel::from_shared(format!("https://127.0.0.1:{flight_port}"))?
        .tls_config(
            tonic::transport::ClientTlsConfig::new().ca_certificate(root_cert_tonic.clone()),
        )
        .expect("valid tls config")
        .connect()
        .await
        .expect("to connect to flight port");

    let mut client = FlightServiceClient::new(channel);
    let sql_command = CommandStatementQuery {
        query: "show tables".to_string(),
        transaction_id: None,
    };
    let sql_command_bytes = sql_command.as_any().encode_to_vec();

    let request = FlightDescriptor::new_cmd(sql_command_bytes);
    let _ = client
        .get_flight_info(request)
        .await
        .expect("valid response");
    tracing::info!("Flight (GRPC) health check passed");

    // OpenTelemetry (GRPC)
    let root_cert_tonic = tonic::transport::Certificate::from_pem(&root_cert_bytes);
    let otel_channel =
        tonic::transport::Channel::from_shared(format!("https://127.0.0.1:{otel_port}"))?
            .tls_config(tonic::transport::ClientTlsConfig::new().ca_certificate(root_cert_tonic))
            .expect("valid tls config")
            .connect()
            .await
            .expect("to connect to otel port");
    let mut health_client = HealthClient::new(otel_channel);
    health_client
        .check(tonic_health::pb::HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("valid response");
    tracing::info!("OpenTelemetry (GRPC) health check passed");

    Ok(())
}
