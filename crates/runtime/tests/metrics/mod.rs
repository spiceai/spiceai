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

use app::{App, AppBuilder};
use futures::StreamExt;
use opentelemetry::global;
use opentelemetry_sdk::{
    metrics::{PeriodicReader, SdkMeterProvider},
    runtime::Tokio,
    Resource,
};
use otel_arrow::OtelArrowExporter;
use rand::Rng;
use reqwest::Client;
use runtime::{auth::EndpointAuth, config::Config, spice_metrics, status, Runtime};
use spicepod::component::{
    dataset::Dataset,
    params::Params,
    runtime::{Runtime as SpicepodRuntime, TelemetryConfig},
};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use crate::{get_test_datafusion, init_tracing};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

pub fn get_s3_dataset() -> Dataset {
    let mut dataset = Dataset::new("s3://spiceai-demo-datasets/taxi_trips/2024/", "taxi_trips");
    dataset.params = Some(Params::from_string_map(
        vec![
            ("file_format".to_string(), "parquet".to_string()),
            ("client_timeout".to_string(), "120s".to_string()),
        ]
        .into_iter()
        .collect(),
    ));
    dataset
}

async fn run_test(app: App) -> Result<String, anyhow::Error> {
    let status = status::RuntimeStatus::new();
    let df = get_test_datafusion(Arc::clone(&status));

    let mut rng = rand::thread_rng();
    let http_port: u16 = rng.gen_range(50000..60000);
    let flight_port: u16 = http_port + 1;
    let otel_port: u16 = http_port + 2;
    let metrics_port: u16 = http_port + 3;

    let api_config = Config::new()
        .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
        .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port))
        .with_open_telemetry_bind_address(SocketAddr::new(LOCALHOST, otel_port));

    let registry = prometheus::Registry::new();

    let resource = Resource::default();

    let prometheus_exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .without_scope_info()
        .without_units()
        .without_counter_suffixes()
        .without_target_info()
        .build()?;

    let spice_metrics_exporter =
        OtelArrowExporter::new(spice_metrics::SpiceMetricsExporter::new(Arc::clone(&df)));

    let periodic_reader = PeriodicReader::builder(spice_metrics_exporter, Tokio)
        .with_interval(Duration::from_secs(30))
        .with_timeout(Duration::from_secs(10))
        .build();

    let provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(prometheus_exporter)
        .with_reader(periodic_reader)
        .build();
    global::set_meter_provider(provider);

    let rt = Arc::new(
        Runtime::builder()
            .with_app(app)
            .with_metrics_server(SocketAddr::new(LOCALHOST, metrics_port), registry)
            .with_datafusion(df)
            .build()
            .await,
    );

    // Set a timeout for the test
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
            return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
        }
        () = rt.load_components() => {}
    }

    let rt_clone = Arc::clone(&rt);
    tokio::spawn(async move {
        Box::pin(rt_clone.start_servers(api_config, None, EndpointAuth::no_auth(), true)).await
    });

    // We don't really care about the result, just that the query ran successfully
    let mut query_result = rt
        .datafusion()
        .query_builder("SELECT * FROM taxi_trips LIMIT 10")
        .with_telemetry_context(crate::get_telemetry_context("user_agent_metrics"))
        .build()
        .run()
        .await
        .map_err(|e| anyhow::anyhow!(e))?;
    let mut batches = vec![];
    while let Some(batch) = query_result.data.next().await {
        batches.push(batch?);
    }

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 10);

    let client = Client::new();
    let response = client
        .get(format!("http://localhost:{metrics_port}/metrics"))
        .send()
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    response.text().await.map_err(|e| anyhow::anyhow!(e))
}

#[tokio::test]
async fn user_agent_metrics() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let app = AppBuilder::new("user_agent_metrics")
        .with_dataset(get_s3_dataset())
        .build();

    let response_text = run_test(app).await?;
    assert!(response_text.contains("client_name=\"integration\""));

    Ok(())
}

#[tokio::test]
async fn test_disabled_user_agent() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let mut runtime = SpicepodRuntime::default();
    runtime.telemetry = Some(TelemetryConfig {
        enabled: true,
        user_agent_collection: Some(
            spicepod::component::runtime::UserAgentCollectionType::Disabled,
        ),
    });

    let app = AppBuilder::new("user_agent_metrics")
        .with_dataset(get_s3_dataset())
        .with_runtime(runtime)
        .build();

    let response_text = run_test(app).await?;

    assert!(!response_text.contains("client_name=\"integration\""));

    Ok(())
}
