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
    time::Duration,
};

use crate::{
    init_tracing,
    utils::{test_request_context, wait_until_true},
};
use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow_flight::{encode::FlightDataEncoderBuilder, FlightClient, FlightDescriptor, PutResult};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::sql::TableReference;
use futures::stream::TryStreamExt;
use rand::Rng;
use runtime::{
    accelerated_table::refresh::Refresh, auth::EndpointAuth,
    component::dataset::acceleration::Acceleration, config::Config, datafusion::DataFusion,
    internal_table::create_internal_accelerated_table, secrets::Secrets, Runtime,
};
use tokio::sync::RwLock;
use tonic::transport::Channel;

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

#[tokio::test]
async fn test_flight_do_put_basic() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    test_request_context().scope(async {
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

        let df = rt.datafusion();

        let test_record_batch = test_record_batch()?;

        register_test_table(&df, test_record_batch.schema(), TableReference::parse_str("public.my_table")).await?;

        // Start the servers
        tokio::spawn(async move {
            Box::pin(Arc::new(rt).start_servers(
                api_config,
                None,
                EndpointAuth::default(),
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

        let mut client = FlightClient::new(channel);

        let flight_descriptor = FlightDescriptor::new_path(vec!["my_table".to_string()]);
        let flight_data_stream = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(flight_descriptor))
            .build(futures::stream::iter(
                // simulate two record batches / FlightData messages
                [test_record_batch.clone(), test_record_batch].into_iter().map(Ok).collect::<Vec<_>>(),
            ));

        let _response: Vec<PutResult> = client
            .do_put(flight_data_stream)
            .await
            .map_err(anyhow::Error::from)?
            .try_collect()
            .await
            .map_err(anyhow::Error::from)?;


        let query = df
            .query_builder(
                "SELECT * from my_table",
            )
            .build()
            .run()
            .await?;

        let results: Vec<RecordBatch> = query.data.try_collect::<Vec<RecordBatch>>().await?;
        let results_str =
            arrow::util::pretty::pretty_format_batches(&results).expect("pretty batches");
        insta::assert_snapshot!(results_str);

        Ok(())
    }).await
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

async fn register_test_table(
    datafusion: &Arc<DataFusion>,
    schema: SchemaRef,
    table_name: TableReference,
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
    )
    .await
    .map_err(anyhow::Error::from)?;

    datafusion
        .register_table_as_writable_and_with_schema(table_name, table)
        .map_err(anyhow::Error::from)?;

    Ok(())
}
