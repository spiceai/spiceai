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

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use crate::init_tracing;
use app::AppBuilder;
use arrow_flight::{flight_service_client::FlightServiceClient, Action};
use prost::Message;
use runtime::flight::actions::datasets::ActionAcceleratedDatasetRefreshRequest;
use runtime::{config::Config, Runtime};
use spicepod::component::dataset::acceleration::Acceleration;
use spicepod::component::dataset::Dataset;
use tonic::transport::Channel;

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

#[tokio::test]
async fn test_grpc_action_accelerated_dataset_refresh() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    // 50000..60000 range is reserved for tls integration tests
    let http_port: u16 = 60001;
    let flight_port: u16 = http_port + 1;
    let otel_port: u16 = http_port + 2;

    let api_config = Config::new()
        .with_http_bind_address(SocketAddr::new(LOCALHOST, http_port))
        .with_flight_bind_address(SocketAddr::new(LOCALHOST, flight_port))
        .with_open_telemetry_bind_address(SocketAddr::new(LOCALHOST, otel_port));

    let app = AppBuilder::new("test_app")
        .with_dataset(make_spiceai_dataset("eth.recent_blocks", "recent_blocks"))
        .build();

    let rt = Runtime::builder().with_app(app).build().await;
    rt.load_components().await;

    tokio::spawn(async move { rt.start_servers(api_config, None).await });

    tracing::info!("Waiting for servers to start...");
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let channel = Channel::from_shared(format!("http://127.0.0.1:{flight_port}"))?
        .connect()
        .await
        .expect("to connect to flight port");

    let mut client = FlightServiceClient::new(channel);

    tracing::info!("Execute refresh action");

    assert!(
        execute_action(&mut client, create_refresh_action("recent_blocks")?,)
            .await
            .is_ok()
    );

    assert!(
        execute_action(&mut client, create_refresh_action("does_not_exist")?,)
            .await
            .is_err(),
        "Refresh for not existing dataset should fail"
    );

    Ok(())
}

fn create_refresh_action(dataset_name: &str) -> Result<Action, prost::EncodeError> {
    let refresh_request = ActionAcceleratedDatasetRefreshRequest {
        dataset_name: dataset_name.to_string(),
    };

    let mut buf = Vec::new();
    refresh_request.encode(&mut buf)?;
    Ok(Action {
        r#type: "AcceleratedDatasetRefresh".to_string(),
        body: buf.into(),
    })
}

async fn execute_action(
    client: &mut FlightServiceClient<Channel>,
    action: Action,
) -> Result<(), anyhow::Error> {
    let mut response_stream = client.do_action(action).await?.into_inner();
    response_stream.message().await?;
    Ok(())
}

fn make_spiceai_dataset(path: &str, name: &str) -> Dataset {
    let mut ds = Dataset::new(format!("spiceai:{path}"), name.to_string());
    ds.acceleration = Some(Acceleration {
        enabled: true,
        ..Default::default()
    });
    ds
}
