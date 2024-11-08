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
use spicepod::component::{
    dataset::{acceleration::Acceleration, Dataset},
    params::Params,
};

use serde_json::{json, Value};
mod hf;
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
