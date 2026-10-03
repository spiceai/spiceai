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

use rand::RngExt;
use runtime::{Runtime, auth::EndpointAuth, config::Config};
use spicepod::component::dataset::Dataset;

use crate::{
    init_tracing,
    utils::{register_test_connectors, test_request_context, wait_until_true},
};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

pub fn get_s3_dictionary_dataset(name: &str) -> Dataset {
    Dataset::new(
        "s3://spiceai-public-datasets/dictionary_example/dictionary_example.parquet",
        name,
    )
}

/// A dataset that does not read from an Iceberg table cannot be handed to an
/// Iceberg client as one: `loadTable` refuses it with the reason and the fix,
/// rather than describing it with metadata the client would read as an empty
/// table.
#[tokio::test]
async fn test_iceberg_api_refuses_a_table_that_is_not_iceberg() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let _ = rustls::crypto::CryptoProvider::install_default(
        rustls::crypto::aws_lc_rs::default_provider(),
    );
    register_test_connectors().await;

    test_request_context()
        .scope(async {
            let span = tracing::info_span!("test_iceberg_api_refuses_a_table_that_is_not_iceberg");
            let _span_guard = span.enter();

            let mut rng = rand::rng();
            let http_port: u16 = rng.random_range(50000..60000);
            let flight_port: u16 = http_port + 1;

            tracing::debug!(
                "Iceberg API Ports: http: {http_port}, flight: {flight_port}"
            );

            let api_config = Config::new()
                .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
                .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port));

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
                () = tokio::time::sleep(std::time::Duration::from_mins(1)) => {
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

            let http_url =
                format!("http://127.0.0.1:{http_port}/v1/namespaces/spice%1Fpublic/tables/dictionary_example");
            let response = http_client
                .get(&http_url)
                .send()
                .await
                .expect("valid response");
            assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
            let body = serde_json::from_str::<serde_json::Value>(&response.text().await?)?;
            assert_eq!(body["error"]["type"], "BadRequestException");
            assert_eq!(body["error"]["code"], 400);
            let message = body["error"]["message"].as_str().expect("the error has a message");
            assert!(
                message.starts_with(
                    "Failed to load table 'spice.public.dictionary_example' as an Iceberg table: it does not read from an Iceberg table"
                ),
                "{message}"
            );
            assert!(message.contains("Query it with SQL through Spice instead"), "{message}");

            let missing = http_client
                .get(format!("http://127.0.0.1:{http_port}/v1/namespaces/spice%1Fpublic/tables/missing"))
                .send()
                .await
                .expect("valid response");
            assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
            let body = serde_json::from_str::<serde_json::Value>(&missing.text().await?)?;
            assert_eq!(body["error"]["type"], "NoSuchTableException");

            Ok(())
        })
        .await
}
