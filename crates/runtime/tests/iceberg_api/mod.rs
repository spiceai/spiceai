/*
Copyright 2025 The Spice.ai OSS Authors

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

use rand::Rng;
use runtime::{Runtime, auth::EndpointAuth, config::Config};
use spicepod::component::dataset::Dataset;

use crate::{
    init_tracing,
    utils::{test_request_context, wait_until_true},
};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

pub fn get_s3_dictionary_dataset(name: &str) -> Dataset {
    Dataset::new(
        "s3://spiceai-public-datasets/dictionary_example/dictionary_example.parquet",
        name,
    )
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn test_iceberg_api_get_table_schema() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );

    test_request_context()
        .scope(async {
            let span = tracing::info_span!("test_iceberg_api_get_table_schema");
            let _span_guard = span.enter();

            let mut rng = rand::rng();
            let http_port: u16 = rng.random_range(50000..60000);
            let flight_port: u16 = http_port + 1;
            let otel_port: u16 = http_port + 2;

            tracing::debug!(
                "Iceberg API Ports: http: {http_port}, flight: {flight_port}, otel: {otel_port}"
            );

            let api_config = Config::new()
                .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
                .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port))
                .with_open_telemetry_bind_address(SocketAddr::new(LOCALHOST, otel_port));

            let app = app::AppBuilder::new("test_app")
                .with_dataset(get_s3_dictionary_dataset("dictionary_example"))
                .build();

            let rt = Arc::new(Runtime::builder().with_app(app).build().await);
            let cloned_rt = Arc::clone(&rt);

            // Start the servers
            tokio::spawn(async move {
                Box::pin(cloned_rt.start_servers(api_config, None, EndpointAuth::no_auth()))
                    .await
            });

            tokio::select! {
                () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                    return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
                }
                () = Arc::clone(&rt).load_components() => {}
            }

            // Connect to the server
            let http_client = reqwest::Client::builder().build()?;

            tracing::info!("Waiting for servers to start...");
            wait_until_true(Duration::from_secs(10), || async {
                http_client
                    .get(format!("http://127.0.0.1:{http_port}/ready"))
                    .send()
                    .await
                    .is_ok()
            })
            .await;

            // Get the table schema
            let http_url =
                format!("http://127.0.0.1:{http_port}/v1/namespaces/spice%1Fpublic/tables/dictionary_example");
            let response = http_client
                .get(&http_url)
                .send()
                .await
                .expect("valid response");
            assert!(
                response.status().is_success(),
                "HTTP health check failed: {}",
                response.status()
            );
            let dictionary_example_snapshot = response.text().await?;
            let dictionary_example_snapshot = serde_json::from_str::<serde_json::Value>(&dictionary_example_snapshot)?;
            assert_eq!(dictionary_example_snapshot["metadata"]["location"], "spice.ai/spice.public.dictionary_example");
            let schemas = dictionary_example_snapshot["metadata"]["schemas"].as_array().expect("schemas is an array");
            insta::assert_json_snapshot!(schemas);

            Ok(())
        })
        .await
}
