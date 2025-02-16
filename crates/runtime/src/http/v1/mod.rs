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

pub mod catalogs;
pub mod chat;
pub mod datasets;
pub mod embeddings;
pub mod eval;
pub mod iceberg;
pub mod inference;
pub mod models;
pub mod nsql;
pub mod packages;
pub mod query;
pub mod ready;
pub mod search;
pub mod spicepods;
pub mod status;
pub mod tools;

use std::sync::Arc;

use crate::{
    component::dataset::Dataset,
    datafusion::{query::QueryBuilder, DataFusion},
    status::ComponentStatus,
};
use arrow::{array::RecordBatch, util::pretty::pretty_format_batches};
use axum::{
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use axum_extra::TypedHeader;
use cache::QueryResultsCacheStatus;
use csv::Writer;
use headers_accept::Accept;
use http::HeaderValue;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use futures::TryStreamExt;

#[cfg(feature = "openapi")]
use utoipa::{
    openapi::{
        path::{Parameter, ParameterBuilder, ParameterIn},
        Required,
    },
    schema,
};

#[derive(Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub enum Format {
    /// JSON format
    #[default]
    Json,

    /// CSV format
    Csv,
}

#[cfg(feature = "openapi")]
impl utoipa::IntoParams for Format {
    fn into_params(parameter_in_provider: impl Fn() -> Option<ParameterIn>) -> Vec<Parameter> {
        vec![ParameterBuilder::new()
            .description(Some(""))
            .name("format")
            .required(Required::True)
            .parameter_in(parameter_in_provider().unwrap_or_default())
            .schema(Some(schema!(Format)))
            .build()]
    }
}

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
/// The various formats that the Arrow data can be converted and returned from HTTP requests.
pub enum ArrowFormat {
    #[default]
    Json,
    Csv,
    Plain,
}

/// Gets all possible media types from a `Accept` header.
pub(crate) fn accept_header_types(accept: &TypedHeader<Accept>) -> Vec<String> {
    accept.0.media_types().map(ToString::to_string).collect()
}

impl ArrowFormat {
    pub fn from_accept_header(accept: Option<&TypedHeader<Accept>>) -> ArrowFormat {
        accept.map_or(ArrowFormat::default(), |header| {
            accept_header_types(header)
                .iter()
                .find_map(|h| match h.as_str() {
                    "application/json" => Some(ArrowFormat::Json),
                    "text/csv" => Some(ArrowFormat::Csv),
                    "text/plain" => Some(ArrowFormat::Plain),
                    _ => None,
                })
                .unwrap_or(ArrowFormat::default())
        })
    }
}

fn convert_entry_to_csv<T: Serialize>(entries: &[T]) -> Result<String, Box<dyn std::error::Error>> {
    let mut w = Writer::from_writer(vec![]);
    for e in entries {
        w.serialize(e)?;
    }
    w.flush()?;
    Ok(String::from_utf8(w.into_inner()?)?)
}

fn dataset_status(df: &DataFusion, ds: &Dataset) -> ComponentStatus {
    if df.table_exists(ds.name.clone()) {
        ComponentStatus::Ready
    } else {
        ComponentStatus::Error
    }
}

// Runs query and converts query results to HTTP response (as JSON).
pub async fn sql_to_http_response(df: Arc<DataFusion>, sql: &str, format: ArrowFormat) -> Response {
    let query = QueryBuilder::new(sql, Arc::clone(&df)).build();

    let (data, results_cache_status) = match query.run().await {
        Ok(query_result) => match query_result.data.try_collect::<Vec<RecordBatch>>().await {
            Ok(batches) => (batches, query_result.results_cache_status),
            Err(e) => {
                tracing::debug!("Error executing query: {e}");
                return (
                    StatusCode::BAD_REQUEST,
                    format!("Error processing batch: {e}"),
                )
                    .into_response();
            }
        },
        Err(e) => {
            tracing::debug!("Error executing query: {e}");
            return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
        }
    };

    let res = match format {
        ArrowFormat::Json => arrow_to_json(&data),
        ArrowFormat::Csv => arrow_to_csv(&data),
        ArrowFormat::Plain => arrow_to_plain(&data),
    };

    let body = match res {
        Ok(body) => body,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let mut headers = HeaderMap::new();

    attach_cache_headers(&mut headers, results_cache_status);

    (StatusCode::OK, headers, body).into_response()
}

fn attach_cache_headers(headers: &mut HeaderMap, results_cache_status: QueryResultsCacheStatus) {
    if let Some(val) = status_to_x_cache_value(results_cache_status) {
        headers.insert("X-Cache", val);
    }

    if let Some(val) = status_to_results_cache_value(results_cache_status) {
        headers.insert("Results-Cache-Status", val);
    }
}

/// This is the legacy cache header, preserved for backwards compatibility.
fn status_to_x_cache_value(results_cache_status: QueryResultsCacheStatus) -> Option<HeaderValue> {
    match results_cache_status {
        QueryResultsCacheStatus::CacheHit => "Hit from spiceai".parse().ok(),
        QueryResultsCacheStatus::CacheMiss => "Miss from spiceai".parse().ok(),
        QueryResultsCacheStatus::CacheDisabled | QueryResultsCacheStatus::CacheBypass => None,
    }
}

fn status_to_results_cache_value(
    results_cache_status: QueryResultsCacheStatus,
) -> Option<HeaderValue> {
    match results_cache_status {
        QueryResultsCacheStatus::CacheHit => "HIT".parse().ok(),
        QueryResultsCacheStatus::CacheMiss => "MISS".parse().ok(),
        QueryResultsCacheStatus::CacheBypass => "BYPASS".parse().ok(),
        QueryResultsCacheStatus::CacheDisabled => None,
    }
}

/// Converts a vector of `RecordBatch` to a JSON string.
fn arrow_to_json(data: &[RecordBatch]) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(buf);

    writer
        .write_batches(data.iter().collect::<Vec<&RecordBatch>>().as_slice())
        .boxed()?;
    writer.finish().boxed()?;

    String::from_utf8(writer.into_inner()).boxed()
}

/// Converts a vector of `RecordBatch` to a CSV string.
fn arrow_to_csv(data: &[RecordBatch]) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let output = Vec::new();
    let mut writer = arrow_csv::Writer::new(output);

    for d in data {
        writer.write(d).boxed()?;
    }

    String::from_utf8(writer.into_inner()).boxed()
}

/// Converts a vector of `RecordBatch` to a pretty formatted string.
/// This is equivalent to [`datafusion::dataframe::DataFrame::show`].
fn arrow_to_plain(
    data: &[RecordBatch],
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    pretty_format_batches(data).map(|d| format!("{d}")).boxed()
}
