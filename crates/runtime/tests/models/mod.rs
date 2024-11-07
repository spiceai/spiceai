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

use reqwest::Client;
use serde_json::json;
use spicepod::component::{
    dataset::{acceleration::Acceleration, Dataset},
    params::Params,
};

mod openai;

fn get_taxi_trips_dataset() -> Dataset {
    let mut dataset = Dataset::new("s3://spiceai-demo-datasets/taxi_trips/2024/", "taxi_trips");
    dataset.params = Some(Params::from_string_map(
        vec![
            ("file_format".to_string(), "parquet".to_string()),
            ("client_timeout".to_string(), "120s".to_string()),
        ]
        .into_iter()
        .collect(),
    ));
    dataset.acceleration = Some(Acceleration {
        enabled: true,
        refresh_sql: Some("SELECT * FROM taxi_trips LIMIT 10".to_string()),
        ..Default::default()
    });
    dataset
}

async fn send_nsql_request(
    base_url: &str,
    query: &str,
    model: Option<&str>,
    sample_data_enabled: Option<bool>,
    datasets: Option<Vec<String>>,
) -> Result<String, reqwest::Error> {
    let mut request_body = json!({
        "query": query,
    });

    if let Some(m) = model {
        request_body["model"] = json!(m);
    }
    if let Some(sde) = sample_data_enabled {
        request_body["sample_data_enabled"] = json!(sde);
    }
    if let Some(ds) = datasets {
        request_body["datasets"] = json!(ds);
    }
    Client::new()
        .post(format!("{base_url}/v1/nsql"))
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await
}
