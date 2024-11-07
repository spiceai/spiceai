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

use app::AppBuilder;
use rand::Rng;
use runtime::{config::Config, Runtime};
use runtime_auth::EndpointAuth;
use spicepod::component::model::Model;

use crate::{
    init_tracing,
    models::{get_taxi_trips_dataset, send_nsql_request},
    utils::{runtime_ready_check, verify_env_secret_exists},
};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

#[tokio::test]
async fn openai_nsql_test() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);

    verify_env_secret_exists("SPICE_OPENAI_API_KEY")
        .await
        .map_err(anyhow::Error::msg)?;

    let app = AppBuilder::new("text-to-sql")
        .with_dataset(get_taxi_trips_dataset())
        .with_model(get_openai_model("gpt-4o-mini", "nql"))
        .with_model(get_openai_model("gpt-4o-mini", "nql-2"))
        .build();

    let http_port = rand::thread_rng().gen_range(50000..60000);

    tracing::debug!("Running Spice runtime with http port: {http_port}");

    let api_config = Config::new().with_http_bind_address(SocketAddr::new(LOCALHOST, http_port));
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);

    let rt_ref_copy = Arc::clone(&rt);
    tokio::spawn(async move {
        Box::pin(rt_ref_copy.start_servers(api_config, None, EndpointAuth::no_auth())).await
    });

    tokio::select! {
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            return Err(anyhow::anyhow!("Timed out waiting for components to load"));
        }
        () = rt.load_components() => {}
    }

    runtime_ready_check(&rt).await;

    let base_url = format!("http://localhost:{http_port}");

    // example responses for the test query: '[{"count(*)":10}]', '[{"record_count":10}]'

    tracing::info!("/v1/nsql: Ensure default request succeeds");
    assert!(send_nsql_request(
        base_url.as_str(),
        "how many records in taxi_trips dataset?",
        None,
        Some(false),
        None,
    )
    .await?
    .contains(":10}"));

    tracing::info!("/v1/nsql: Ensure model selection works");
    assert!(send_nsql_request(
        base_url.as_str(),
        "how many records in taxi_trips dataset?",
        Some("nql-2"),
        Some(false),
        None,
    )
    .await?
    .contains(":10}"));

    tracing::info!("/v1/nsql: Ensure error when invalid dataset name is provided");
    assert!(send_nsql_request(
        base_url.as_str(),
        "how many records in taxi_trips dataset?",
        Some("nql"),
        Some(false),
        Some(vec!["dataset_not_in_spice".to_string()]),
    )
    .await
    .is_err());

    tracing::info!("/v1/nsql: Ensure error when invalid model name is provided");
    assert!(send_nsql_request(
        base_url.as_str(),
        "how many records in taxi_trips dataset?",
        Some("model_not_in_spice"),
        Some(false),
        None,
    )
    .await
    .is_err());

    Ok(())
}

fn get_openai_model(model: impl Into<String>, name: impl Into<String>) -> Model {
    let mut model = Model::new(format!("openai:{}", model.into()), name);
    model.params.insert(
        "openai_api_key".to_string(),
        "${ secrets:SPICE_OPENAI_API_KEY }".into(),
    );
    model
}
