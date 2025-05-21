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
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow_flight::{
    FlightClient, FlightDescriptor, PutResult, encode::FlightDataEncoderBuilder, error::FlightError,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::sql::TableReference;
use futures::{Stream, TryStreamExt as _};
use rand::Rng as _;
use runtime::{
    Runtime, accelerated_table::refresh::Refresh, auth::EndpointAuth,
    component::dataset::acceleration::Acceleration, config::Config, datafusion::DataFusion,
    flight::RateLimits, internal_table::create_internal_accelerated_table, secrets::Secrets,
};
use runtime_auth::FlightBasicAuth;
use spicepod::component::dataset::Dataset;
use tokio::{sync::RwLock, time::sleep};
use tonic::transport::Channel;

use crate::{
    configure_test_datafusion,
    utils::{runtime_ready_check, wait_until_true},
};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

mod do_get;
mod do_put;
mod prepared_statements;

async fn start_spice_test_app(
    flight_auth: Option<Arc<dyn FlightBasicAuth + Send + Sync>>,
    rate_limits: Option<RateLimits>,
    test_dataset: Option<Dataset>,
) -> Result<(Channel, Arc<DataFusion>), anyhow::Error> {
    let mut rng = rand::rng();
    let http_port: u16 = rng.random_range(50000..60000);
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

    let mut rt_builder = Runtime::builder()
        .with_metrics_server(SocketAddr::new(LOCALHOST, metrics_port), registry)
        .with_datafusion_configuration_fn(configure_test_datafusion);

    if let Some(rate_limits) = rate_limits {
        rt_builder = rt_builder.with_rate_limits(rate_limits);
    }

    let app = if let Some(dataset) = test_dataset {
        app::AppBuilder::new("test_app")
            .with_dataset(dataset)
            .build()
    } else {
        app::AppBuilder::new("test_app").build()
    };
    let rt = Arc::new(rt_builder.with_app(app).build().await);

    let cloned_rt = Arc::clone(&rt);
    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
            return Err(anyhow::anyhow!("Timed out waiting for datasets to load"));
        }
        () = cloned_rt.load_components() => {}
    };

    runtime_ready_check(&rt).await;

    let df = rt.datafusion();

    let test_record_batch = test_record_batch()?;

    register_test_table(
        &df,
        test_record_batch.schema(),
        TableReference::parse_str("public.my_table"),
        Arc::clone(&rt),
    )
    .await?;

    let mut auth = EndpointAuth::default();

    if let Some(flight_auth) = flight_auth {
        auth = auth.with_flight_basic_auth(flight_auth);
    }

    // Start the servers
    tokio::spawn(async move { Box::pin(rt.start_servers(api_config, None, auth)).await });

    // Wait for the servers to start
    tracing::info!("Waiting for servers to start...");
    wait_until_true(Duration::from_secs(10), || async {
        reqwest::get(format!("http://localhost:{http_port}/health"))
            .await
            .is_ok()
    })
    .await;

    // HTTP server readiness doesn't essentially mean the flight server is ready
    // Validate the flight server readiness by sending a handshake request
    let start_time = std::time::Instant::now();
    let channel = loop {
        if start_time.elapsed() > std::time::Duration::from_secs(30) {
            return Err(anyhow::anyhow!(
                "Flight server not ready within 30 seconds timeout"
            ));
        }

        // Attempt to connect
        match Channel::from_shared(format!("http://localhost:{flight_port}"))
            .map_err(anyhow::Error::from)?
            .connect()
            .await
        {
            Ok(channel) => {
                break channel;
            }
            Err(_) => {
                // Wait before next attempt
                sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    };

    Ok((channel, df))
}

fn test_record_batch() -> Result<RecordBatch, anyhow::Error> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .map_err(anyhow::Error::from)
}

fn create_flight_client(
    channel: Channel,
    api_key: Option<&str>,
) -> Result<FlightClient, anyhow::Error> {
    let mut client = FlightClient::new(channel);

    if let Some(api_key) = api_key {
        client
            .add_header("authorization", &format!("Bearer {api_key}"))
            .map_err(anyhow::Error::from)?;
    }

    Ok(client)
}

fn large_test_record_batch() -> Result<RecordBatch, anyhow::Error> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Utf8, false),
    ]));

    // Generate 35,000 rows of data
    let int_column = (1..=35_000).collect::<Vec<i32>>();
    let string_column = (1..=35_000)
        .map(|i| format!("row_{i}"))
        .collect::<Vec<String>>();

    RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(int_column)),
            Arc::new(StringArray::from(string_column)),
        ],
    )
    .map_err(anyhow::Error::from)
}

async fn register_test_table(
    datafusion: &Arc<DataFusion>,
    schema: SchemaRef,
    table_name: TableReference,
    runtime: Arc<Runtime>,
) -> Result<(), anyhow::Error> {
    let table = create_internal_accelerated_table(
        datafusion.runtime_status(),
        table_name.clone(),
        schema,
        None,
        Acceleration::default(),
        Refresh::default(),
        None,
        Arc::new(RwLock::new(Secrets::default())),
        runtime,
    )
    .await
    .map_err(anyhow::Error::from)?;

    datafusion
        .register_table_as_writable_and_with_schema(table_name, table)
        .map_err(anyhow::Error::from)?;

    Ok(())
}

async fn write_record_batches(
    client: &mut FlightClient,
    batches: impl IntoIterator<Item = RecordBatch>,
) -> Result<Vec<PutResult>, FlightError> {
    let flight_descriptor = FlightDescriptor::new_path(vec!["my_table".to_string()]);
    let flight_data_stream = FlightDataEncoderBuilder::new()
        .with_flight_descriptor(Some(flight_descriptor))
        .build(futures::stream::iter(
            batches.into_iter().map(Ok).collect::<Vec<_>>(),
        ));

    let response: Vec<PutResult> = client
        .do_put(flight_data_stream)
        .await?
        .try_collect()
        .await?;

    Ok(response)
}

struct RepeatingStream {
    batch: RecordBatch,
}

impl Stream for RepeatingStream {
    type Item = Result<RecordBatch, FlightError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(Some(Ok(self.batch.clone())))
    }
}
