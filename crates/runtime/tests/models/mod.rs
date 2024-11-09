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

use async_openai::types::EmbeddingInput;
use rand::Rng;
use reqwest::Client;
use runtime::config::Config;
use spicepod::component::{
    dataset::{acceleration::Acceleration, Dataset},
    params::Params,
};

use serde_json::{json, Value};
mod hf;
mod openai;

fn create_api_bindings_config() -> Config {
    let mut rng = rand::thread_rng();
    let http_port: u16 = rng.gen_range(50000..60000);
    let flight_port: u16 = http_port + 1;
    let otel_port: u16 = http_port + 2;
    let metrics_port: u16 = http_port + 3;

    let localhost: IpAddr = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));

    let api_config = Config::new()
        .with_http_bind_address(SocketAddr::new(localhost, http_port))
        .with_flight_bind_address(SocketAddr::new(localhost, flight_port))
        .with_open_telemetry_bind_address(SocketAddr::new(localhost, otel_port));

    tracing::debug!(
        "Created api bindings configuration: http: {http_port}, flight: {flight_port}, otel: {otel_port}, metrics: {metrics_port}"
    );

    api_config
}

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

fn get_tpcds_dataset(ds_name: &str) -> Dataset {
    let mut dataset = Dataset::new(
        format!("s3://spiceai-public-datasets/tpcds/{ds_name}/"),
        ds_name,
    );
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
        refresh_sql: Some(format!("SELECT * FROM {ds_name} LIMIT 20")),
        ..Default::default()
    });
    dataset
}

fn json_is_single_row_with_value(json: &Value, expected_value: i64) -> bool {
    json.as_array()
        .filter(|array| array.len() == 1)
        .and_then(|array| array.first())
        .map_or(false, |item| {
            item.as_object().map_or(false, |map| {
                map.values()
                    .any(|value| value == &Value::from(expected_value))
            })
        })
}

async fn send_nsql_request(
    base_url: &str,
    query: &str,
    model: Option<&str>,
    sample_data_enabled: Option<bool>,
    datasets: Option<Vec<String>>,
) -> Result<Value, reqwest::Error> {
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
    let response = Client::new()
        .post(format!("{base_url}/v1/nsql"))
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;

    Ok(response)
}

async fn send_search_request(
    base_url: &str,
    text: &str,
    limit: Option<usize>,
    datasets: Option<Vec<String>>,
    where_cond: Option<&str>,
    additional_columns: Option<Vec<String>>,
) -> Result<Value, reqwest::Error> {
    let mut request_body = json!({
        "text": text,
    });

    if let Some(limit) = limit {
        request_body["limit"] = json!(limit);
    }

    if let Some(ds) = datasets {
        request_body["datasets"] = json!(ds);
    }

    if let Some(where_cond) = where_cond {
        request_body["where_cond"] = json!(where_cond);
    }

    if let Some(columns) = additional_columns {
        request_body["additional_columns"] = json!(columns);
    }

    let response = Client::new()
        .post(format!("{base_url}/v1/search"))
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;

    Ok(response)
}

fn normalize_search_response(mut json: Value) -> String {
    if let Some(matches) = json.get_mut("matches").and_then(|m| m.as_array_mut()) {
        for m in matches {
            if let Some(score) = m.get_mut("score") {
                *score = json!("score_val");
            }
        }
    }

    if let Some(duration) = json.get_mut("duration_ms") {
        *duration = json!("duration_ms_val");
    }

    json.to_string()
}

/// Normalizes embeddings response for consistent snapshot testing by replacing actual embedding arrays with a placeholder,
fn normalize_embeddings_response(mut json: Value) -> String {
    if let Some(data) = json.get_mut("data").and_then(|d| d.as_array_mut()) {
        for entry in data {
            if let Some(embedding) = entry.get_mut("embedding") {
                if let Some(embedding_array) = embedding.as_array_mut() {
                    let num_elements = embedding_array.len();
                    *embedding = json!(format!("array_{}_items", num_elements));
                }
            }
        }
    }

    json.to_string()
}

/// Normalizes semantic search response for consistent snapshot testing by replacing dynamic values
/// (such as scores and durations) with placeholders.
async fn send_embeddings_request(
    base_url: &str,
    model: &str,
    input: EmbeddingInput,
    // The format to return the embeddings in. Can be either `float` or [`base64`](https://pypi.org/project/pybase64/). Defaults to float
    encoding_format: Option<&str>,
    // OpenAI only: A unique identifier representing your end-user, [Learn more](https://platform.openai.com/docs/usage-policies/end-user-ids).
    user: Option<&str>,
    // The number of dimensions the resulting output embeddings should have. Only supported in `text-embedding-3` and later models.
    dimensions: Option<u32>,
) -> Result<Value, reqwest::Error> {
    let mut request_body = json!({
        "model": model,
        "input": input,
    });

    if let Some(ef) = encoding_format {
        request_body["encoding_format"] = json!(ef);
    }

    if let Some(u) = user {
        request_body["user"] = json!(u);
    }

    if let Some(d) = dimensions {
        request_body["dimensions"] = json!(d);
    }

    let response = Client::new()
        .post(format!("{base_url}/v1/embeddings"))
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;

    Ok(response)
}
