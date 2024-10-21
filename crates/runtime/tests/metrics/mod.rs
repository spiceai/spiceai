use app::AppBuilder;
use opentelemetry::global;
use opentelemetry_sdk::{
    metrics::{PeriodicReader, SdkMeterProvider},
    runtime::Tokio,
    Resource,
};
use otel_arrow::OtelArrowExporter;
use prometheus::Registry;
use reqwest::Client;
use runtime::{config::Config, datafusion::DataFusion, spice_metrics, Runtime};
use std::time::Duration;
use std::{net::SocketAddr, sync::Arc};

use crate::init_tracing;

fn init_metrics(df: Arc<DataFusion>, registry: prometheus::Registry) -> Result<(), anyhow::Error> {
    let resource = Resource::default();

    let prometheus_exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry)
        .without_scope_info()
        .without_units()
        .without_counter_suffixes()
        .without_target_info()
        .build()?;

    let spice_metrics_exporter =
        OtelArrowExporter::new(spice_metrics::SpiceMetricsExporter::new(df));

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

    Ok(())
}

#[tokio::test]
#[allow(clippy::clone_on_ref_ptr)]
async fn test_http_metrics() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("debug"));

    let app = AppBuilder::new("test_http_metrics").build();

    let socket_addr: SocketAddr = SocketAddr::from(([0, 0, 0, 0], 9090));
    let registry = Some(Registry::new());

    let rt = Runtime::builder()
        .with_metrics_server_opt(Some(socket_addr), registry.clone())
        .with_app(app)
        .build()
        .await;
    tracing::info!("Starting runtime");
    let rt_arc = Arc::new(rt);

    let rt_arc_ref = Arc::clone(&rt_arc);
    tokio::spawn(async move { rt_arc_ref.start_servers(Config::default(), None).await });
    tracing::info!("Runtime started");
    init_metrics(rt_arc.datafusion(), registry.clone().expect("No registry"))?;

    let client = Client::new();
    let response = client
        .post("http://localhost:8090/v1/sql")
        .header("x-spice-user-agent", "specifictestclient 0.0.0 (DummyOS)")
        .body("show tables;")
        .send()
        .await?
        .error_for_status()?;
    assert!(response.status().is_success());

    let mresponse = client
        .get("http://localhost:9090/metrics")
        .send()
        .await?
        .error_for_status()?;
    assert!(mresponse.status().is_success());
    let response_text = mresponse.text().await?;

    assert!(
        response_text.contains("specifictestclient"),
        "Response does not contain user agent"
    );

    Ok(())
}
