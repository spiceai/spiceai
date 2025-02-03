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

use arrow::{array::StringArray, util::pretty::pretty_format_batches};
use async_openai::types::EmbeddingInput;
use futures::TryStreamExt;
use rand::Rng;
use reqwest::{header::HeaderMap, Client};
use runtime::{config::Config, get_params_with_secrets, Runtime};
use secrecy::SecretString;
use snafu::ResultExt;
use spicepod::component::{
    dataset::{acceleration::Acceleration, Dataset},
    params::Params,
};
use std::sync::Arc;
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

use serde_json::{json, Value};
mod embedding;
mod hf;
mod local;
mod openai;

mod nsql {
    use chrono::{DateTime, Utc};
    use http::{
        header::{ACCEPT, CONTENT_TYPE},
        HeaderMap, HeaderValue,
    };
    use opentelemetry_sdk::trace::TracerProvider;

    use crate::models::http_post;

    pub struct TestCase {
        pub name: &'static str,
        pub body: serde_json::Value,
    }

    pub async fn run_nsql_test(
        base_url: &str,
        ts: &TestCase,
        trace_provider: &TracerProvider,
    ) -> Result<(), anyhow::Error> {
        tracing::info!("Running test cases {}", ts.name);
        let task_start_time = std::time::SystemTime::now();

        // Call /v1/nsql, check response
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let response = match http_post(
            format!("{base_url}/v1/nsql").as_str(),
            &ts.body.to_string(),
            headers,
        )
        .await
        {
            Ok(response) => response,
            Err(e) => {
                tracing::error!("run_nsql_test error: {:?}", e);
                insta::assert_snapshot!(format!("{}_error", ts.name), e.to_string());
                return Ok(());
            }
        };
        insta::assert_snapshot!(format!("{}_response", ts.name), &response);

        // ensure all spans are exported into task_history
        let _ = trace_provider.force_flush();

        // Check task_history table for expected rows.
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("text/plain"));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let response = http_post(
            format!("{base_url}/v1/sql").as_str(),
            format!(
                "SELECT task, input
                        FROM runtime.task_history
                        WHERE task NOT IN ('ai_completion', 'health', 'accelerated_refresh')
                        AND start_time > '{}'
                        ORDER BY start_time, task;",
                Into::<DateTime<Utc>>::into(task_start_time).to_rfc3339()
            )
            .as_str(),
            headers,
        )
        .await
        .map_err(anyhow::Error::msg)?;

        insta::assert_snapshot!(format!("{}_tasks", ts.name), response,);

        Ok(())
    }
}

mod search {
    use http::{
        header::{ACCEPT, CONTENT_TYPE},
        HeaderMap, HeaderValue,
    };

    use crate::models::{http_post, normalize_search_response};

    pub struct TestCase {
        pub name: &'static str,
        pub body: serde_json::Value,
    }

    pub async fn run_search_test(base_url: &str, ts: &TestCase) -> Result<(), anyhow::Error> {
        tracing::info!("Running test cases {}", ts.name);

        // Call /v1/search, check response
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let response_str = http_post(
            &format!("{base_url}/v1/search").to_string(),
            &ts.body.to_string(),
            headers,
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to execute HTTP POST: {}", e))?;

        let response = serde_json::from_str(&response_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse HTTP response: {}", e))?;

        insta::assert_snapshot!(
            format!("{}_response", ts.name),
            normalize_search_response(response)
        );
        Ok(())
    }
}

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

fn get_tpcds_dataset(
    ds_name: &str,
    spice_name: Option<&str>,
    refresh_sql: Option<&str>,
) -> Dataset {
    let mut dataset = Dataset::new(
        format!("s3://spiceai-public-datasets/tpcds/{ds_name}/"),
        spice_name.unwrap_or(ds_name),
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
        refresh_sql: Some(
            refresh_sql
                .unwrap_or(&format!(
                    "SELECT * FROM {} LIMIT 20",
                    spice_name.unwrap_or(ds_name)
                ))
                .to_string(),
        ),
        ..Default::default()
    });
    dataset
}

/// Normalizes vector similarity search response for consistent snapshot testing by replacing dynamic
/// values such as duration with placeholder.
fn normalize_search_response(mut json: Value) -> String {
    if let Some(matches) = json.get_mut("matches").and_then(|m| m.as_array_mut()) {
        for m in matches {
            if let Some(obj) = m.as_object_mut() {
                obj.remove("score");
            }
        }
    }

    if let Some(duration) = json.get_mut("duration_ms") {
        *duration = json!("duration_ms_val");
    }

    sort_json_keys(&mut json);

    serde_json::to_string_pretty(&json).unwrap_or_default()
}

/// Normalizes embeddings response for consistent snapshot testing by replacing actual embedding arrays with a placeholder,
fn normalize_embeddings_response(mut json: Value) -> String {
    if let Some(data) = json.get_mut("data").and_then(|d| d.as_array_mut()) {
        for entry in data {
            if let Some(embedding) = entry.get_mut("embedding") {
                if let Some(embedding_array) = embedding.as_array_mut() {
                    let num_elements = embedding_array.len();
                    *embedding = json!(format!("array_{}_items", num_elements));
                } else if let Some(embedding_str) = embedding.as_str() {
                    *embedding = json!(format!("str_len_{}", embedding_str.len()));
                }
            }
        }
    }

    sort_json_keys(&mut json);

    serde_json::to_string_pretty(&json).unwrap_or_default()
}

/// Normalizes chat completion response for consistent snapshot testing by replacing dynamic values
fn normalize_chat_completion_response(mut json: Value, normalize_message_content: bool) -> String {
    // Replace `content`
    if normalize_message_content {
        if let Some(choices) = json.get_mut("choices").and_then(|c| c.as_array_mut()) {
            for choice in choices {
                if let Some(message) = choice.get_mut("message") {
                    if let Some(content) = message.get_mut("content") {
                        *content = json!("content_val");
                    }
                }
            }
        }
    }

    if let Some(created) = json.get_mut("created") {
        *created = json!("created_val");
    }

    // Replace `completion_tokens`, `prompt_tokens`, and `total_tokens` fields in `usage`
    if let Some(usage) = json.get_mut("usage") {
        if let Some(completion_tokens) = usage.get_mut("completion_tokens") {
            *completion_tokens = json!("completion_tokens_val");
        }
        if let Some(completion_tokens_details) = usage.get_mut("completion_tokens_details") {
            *completion_tokens_details = json!("completion_tokens_details_val");
        }
        if let Some(prompt_tokens_details) = usage.get_mut("prompt_tokens_details") {
            *prompt_tokens_details = json!("prompt_tokens_details_val");
        }
        if let Some(prompt_tokens) = usage.get_mut("prompt_tokens") {
            *prompt_tokens = json!("prompt_tokens_val");
        }
        if let Some(total_tokens) = usage.get_mut("total_tokens") {
            *total_tokens = json!("total_tokens_val");
        }
    }

    if let Some(system_fingerprint) = json.get_mut("system_fingerprint") {
        *system_fingerprint = json!("system_fingerprint_val");
    }

    if let Some(id) = json.get_mut("id") {
        *id = json!("id_val");
    }

    sort_json_keys(&mut json);

    serde_json::to_string_pretty(&json).unwrap_or_default()
}

/// Sorts the keys of a JSON object in place for consistent snapshot testing
fn sort_json_keys(value: &mut Value) {
    match value {
        Value::Object(map) => {
            let mut sorted_map = serde_json::Map::new();
            let mut keys: Vec<_> = map.keys().cloned().collect();
            keys.sort();

            for key in keys {
                if let Some(mut val) = map.remove(&key) {
                    sort_json_keys(&mut val); // Recurse into nested objects
                    sorted_map.insert(key, val);
                }
            }

            *map = sorted_map;
        }
        Value::Array(array) => {
            for element in array.iter_mut() {
                sort_json_keys(element);
            }
        }
        _ => {}
    }
}

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
        request_body["user"] = Value::String(u.to_string());
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

async fn send_chat_completions_request(
    base_url: &str,
    messages: Vec<(String, String)>,
    model: &str,
    stream: bool,
) -> Result<Value, reqwest::Error> {
    let request_body = json!({
        "messages": messages.iter().map(|(role, content)| {
            json!({
                "role": role,
                "content": content,
            })
        }).collect::<Vec<_>>(),
        "model": model,
        "stream": stream,
    });

    let response = Client::new()
        .post(format!("{base_url}/v1/chat/completions"))
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;

    Ok(response)
}

/// Generic function to send a POST request, returning the response as a String.
pub async fn http_post(url: &str, body: &str, headers: HeaderMap) -> Result<String, anyhow::Error> {
    let response = Client::new()
        .post(url)
        .headers(headers)
        .body(body.to_string())
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Request error: {e}"))?;

    if !response.status().is_success() {
        let status = response.status();
        let message = response
            .text()
            .await
            .unwrap_or_else(|_| "No error message".to_string());
        return Err(anyhow::anyhow!("HTTP error: {status} - {message}"));
    }

    response
        .text()
        .await
        .map_err(|e| anyhow::anyhow!("Error reading response body: {e}")) // Map body read error to anyhow
}

/// Returns a human-readable representation of the SQL query result against a [`Runtime`].
async fn sql_to_display(
    rt: &Arc<Runtime>,
    query: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let data = rt
        .datafusion()
        .query_builder(query)
        .build()
        .run()
        .await
        .boxed()?
        .data
        .try_collect::<Vec<_>>()
        .await
        .boxed()?;
    pretty_format_batches(&data).map(|d| format!("{d}")).boxed()
}

#[allow(clippy::expect_used)]
async fn sql_to_single_json_value(rt: &Arc<Runtime>, query: &str) -> Value {
    let data = rt
        .datafusion()
        .query_builder(query)
        .build()
        .run()
        .await
        .boxed()
        .expect("Failed to collect data")
        .data
        .try_collect::<Vec<_>>()
        .await
        .boxed()
        .expect("Failed to collect data");
    assert_eq!(data.len(), 1);
    assert_eq!(data[0].columns().len(), 1);
    serde_json::from_str(
        data[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("column is a StringArray")
            .value(0),
    )
    .expect("value is a JSON string")
}

async fn get_params_with_secrets_value(
    params: &HashMap<String, Value>,
    rt: &Runtime,
) -> HashMap<String, SecretString> {
    let params = params
        .clone()
        .iter()
        .map(|(k, v)| {
            let k = k.clone();
            match v.as_str() {
                Some(s) => (k, s.to_string()),
                None => (k, v.to_string()),
            }
        })
        .collect::<HashMap<_, _>>();

    get_params_with_secrets(rt.secrets(), &params).await
}
