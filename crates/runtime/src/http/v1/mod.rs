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
pub mod responses;
pub mod snapshots;

pub mod models;
pub mod nsql;
pub mod packages;
pub mod queries;
pub mod query;
pub mod ready;
pub mod search;
pub mod spicepods;
pub mod status;
pub mod tools;
pub mod workers;

use std::sync::Arc;

use crate::{
    component::dataset::Dataset,
    datafusion::{DataFusion, query::QueryBuilder},
    status::ComponentStatus,
};
use arrow::{array::RecordBatch, util::pretty::pretty_format_batches};
use axum::{
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use axum_extra::TypedHeader;
use cache::result::CacheStatus;
use csv::Writer;
use datafusion::common::ParamValues;
use headers_accept::Accept;
use http::{
    HeaderValue,
    header::{CACHE_CONTROL, CONTENT_TYPE},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use snafu::ResultExt;

use futures::TryStreamExt;

use runtime_request_context::{AsyncMarker, RequestContext};

use crate::datafusion::request_context_extension::DataFusionContextExtension;
#[cfg(feature = "openapi")]
use utoipa::{
    openapi::{
        Required,
        path::{Parameter, ParameterBuilder, ParameterIn},
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
        vec![
            ParameterBuilder::new()
                .description(Some(""))
                .name("format")
                .required(Required::True)
                .parameter_in(parameter_in_provider().unwrap_or_default())
                .schema(Some(schema!(Format)))
                .build(),
        ]
    }
}

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
/// The various formats that the Arrow data can be converted and returned from HTTP requests.
pub enum ResponseMimeType {
    #[default]
    Json,
    Csv,
    Plain,
    VndNsqlJsonV1,
    VndSqlJsonV1,
}

/// Represents additional metadata to produce a response, such as the SQL query used, etc.
#[derive(Debug)]
pub struct ResponseMetadata {
    pub sql: Option<String>,
}

impl ResponseMetadata {
    /// Creates an empty `ResponseMetadata`
    pub fn empty() -> Self {
        Self { sql: None }
    }

    pub fn with_sql(mut self, sql: impl Into<String>) -> Self {
        self.sql = Some(sql.into());
        self
    }
}

/// Gets all possible media types from a `Accept` header.
pub(crate) fn accept_header_types(accept: &TypedHeader<Accept>) -> Vec<String> {
    accept.0.media_types().map(ToString::to_string).collect()
}

impl ResponseMimeType {
    pub fn to_accept_header(self) -> Option<http::HeaderValue> {
        let media_type = match self {
            Self::Json => "application/json",
            Self::Csv => "text/csv",
            Self::Plain => "text/plain",
            Self::VndNsqlJsonV1 => "application/vnd.spiceai.nsql.v1+json",
            Self::VndSqlJsonV1 => "application/vnd.spiceai.sql.v1+json",
        };
        HeaderValue::from_str(media_type).ok()
    }

    pub fn from_accept_header(accept: Option<&TypedHeader<Accept>>) -> ResponseMimeType {
        accept.map_or(ResponseMimeType::default(), |header| {
            accept_header_types(header)
                .iter()
                .find_map(|h| match h.as_str() {
                    "application/json" => Some(ResponseMimeType::Json),
                    "application/vnd.spiceai.nsql.v1+json" => Some(ResponseMimeType::VndNsqlJsonV1),
                    "application/vnd.spiceai.sql.v1+json" => Some(ResponseMimeType::VndSqlJsonV1),
                    "text/csv" => Some(ResponseMimeType::Csv),
                    "text/plain" => Some(ResponseMimeType::Plain),
                    _ => None,
                })
                .unwrap_or(ResponseMimeType::default())
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
    // First check the runtime status which tracks the actual component state
    // (Initializing, Refreshing, Ready, Error, etc.)
    let dataset_statuses = df.runtime_status().get_dataset_statuses();
    if let Some(status) = dataset_statuses.get(&ds.name) {
        return *status;
    }

    // Fallback: if not in runtime status, check if table exists
    if df.table_exists(ds.name.clone()) {
        ComponentStatus::Ready
    } else {
        ComponentStatus::Error
    }
}

// Runs query and converts query results to HTTP response (as JSON).
pub async fn sql_to_http_response(
    df: Arc<DataFusion>,
    sql: &str,
    parameters: Option<ParamValues>,
    format: ResponseMimeType,
) -> Response {
    let (data, results_cache_status) = match run_sql(df, sql, parameters).await {
        Ok((data, results_cache_status)) => (data, results_cache_status),
        Err(e) => {
            tracing::debug!("Error executing query: {e}");
            return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
        }
    };

    to_http_response(
        data,
        results_cache_status,
        format,
        ResponseMetadata::empty(),
    )
    .await
    .into_response()
}

// Runs query and returns the results as a vector of `RecordBatch`.
pub async fn run_sql(
    df: Arc<DataFusion>,
    sql: &str,
    parameters: Option<ParamValues>,
) -> Result<(Vec<RecordBatch>, CacheStatus), Box<dyn std::error::Error + Send + Sync>> {
    let query_res = QueryBuilder::new(sql, df)
        .parameters(parameters)
        .build()
        .run()
        .await?;

    Ok((
        query_res.data.try_collect::<Vec<RecordBatch>>().await?,
        query_res.cache_status,
    ))
}

// Converts query result to HTTP response (as JSON).
pub async fn to_http_response(
    data: Vec<RecordBatch>,
    cache_status: CacheStatus,
    format: ResponseMimeType,
    meta: ResponseMetadata,
) -> (StatusCode, HeaderMap, String) {
    let mut headers = HeaderMap::new();

    let res = match format {
        ResponseMimeType::Json => arrow_to_json(&data),
        ResponseMimeType::Csv => arrow_to_csv(&data),
        ResponseMimeType::Plain => arrow_to_plain(&data),
        ResponseMimeType::VndSqlJsonV1 | ResponseMimeType::VndNsqlJsonV1 => {
            arrow_to_vnd_sql_json_v1(&data, meta)
        }
    };

    let body = match res {
        Ok(body) => body,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, headers, e.to_string());
        }
    };

    let request_context = RequestContext::current(AsyncMarker::new().await);

    if let Some(header_value) = format.to_accept_header() {
        headers.insert(CONTENT_TYPE, header_value);
    }

    attach_cache_headers(
        &mut headers,
        cache_status,
        request_context.client_supplied_cache_key().is_some(),
        &request_context,
    );

    (StatusCode::OK, headers, body)
}

fn attach_cache_headers(
    headers: &mut HeaderMap,
    results_cache_status: CacheStatus,
    user_key_specified: bool,
    request_context: &RequestContext,
) {
    if let Some(val) = status_to_x_cache_value(results_cache_status) {
        headers.insert("X-Cache", val);
    }

    if let Some(val) = results_cache_status
        .to_header_string()
        .and_then(|v| v.parse().ok())
    {
        headers.insert("Results-Cache-Status", val);
    }

    // Tell CDN entry is unique per user cache key
    if user_key_specified {
        headers.insert("Vary", HeaderValue::from_static("Spice-Cache-Key"));
    }

    // Add Cache-Control response header with stale-while-revalidate if configured
    // Access the DataFusion instance to get the pre-parsed cache configuration
    if let Some(df_ext) = request_context.extension::<DataFusionContextExtension>() {
        let df = df_ext.datafusion();
        if let Some(cache_provider) = df.results_cache_provider()
            && let Some(stale_duration) = cache_provider.stale_while_revalidate_ttl()
        {
            // When serving stale content, set max-age=0 to indicate the response is not fresh
            // The Results-Cache-Status header will indicate STALE
            let max_age = if results_cache_status == CacheStatus::CacheStaleWhileRevalidate {
                0
            } else {
                cache_provider.ttl().as_secs()
            };

            let cache_control_value = format!(
                "max-age={}, stale-while-revalidate={}",
                max_age,
                stale_duration.as_secs()
            );

            if let Ok(header_value) = HeaderValue::from_str(&cache_control_value) {
                headers.insert(CACHE_CONTROL, header_value);
            }
        }
    }
}

/// This is the legacy cache header, preserved for backwards compatibility.
fn status_to_x_cache_value(results_cache_status: CacheStatus) -> Option<HeaderValue> {
    match results_cache_status {
        CacheStatus::CacheHit | CacheStatus::CacheStaleWhileRevalidate => {
            "Hit from spiceai".parse().ok()
        }
        CacheStatus::CacheMiss => "Miss from spiceai".parse().ok(),
        CacheStatus::CacheDisabled | CacheStatus::CacheBypass => None,
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

/// Converts a vector of `RecordBatch` to an application/vnd.spiceai.sql.v1+json / application/vnd.spiceai.nsql.v1+json format
fn arrow_to_vnd_sql_json_v1(
    data: &[RecordBatch],
    meta: ResponseMetadata,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(buf);

    // Convert manually instead of reusing arrow_to_json
    // to avoid an extra serialization-deserialization cycle
    writer
        .write_batches(data.iter().collect::<Vec<&RecordBatch>>().as_slice())
        .boxed()?;
    writer.finish().boxed()?;

    // Calculate total row count across all batches
    let row_count = data.iter().map(RecordBatch::num_rows).sum::<usize>();

    let schema_json = if let Some(batch) = data.first() {
        // Use built-in Arrow JSON schema representation: https://github.com/apache/arrow/blob/main/docs/source/format/Integration.rst#json-test-data-format
        serde_json::to_value(batch.schema())?
    } else {
        serde_json::json!({})
    };

    let mut result = json!({
        "row_count": row_count,
        "schema": schema_json,
        "data": serde_json::from_slice::<serde_json::Value>(&writer.into_inner()).boxed()?,
    });

    if let Some(sql) = meta.sql {
        result["sql"] = serde_json::Value::String(sql);
    }

    serde_json::to_string(&result).boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_arrow_to_vnd_json_v1() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, false),
            Field::new("total_sales", DataType::Int64, false),
        ]));

        let customer_ids = StringArray::from(vec!["12345", "67890"]);
        let total_sales = Int64Array::from(vec![150_000, 125_000]);

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(customer_ids), Arc::new(total_sales)])
                .expect("to create batch");

        // Test conversion without SQL
        let result_without_sql =
            arrow_to_vnd_sql_json_v1(std::slice::from_ref(&batch), ResponseMetadata::empty())
                .expect("to convert");
        insta::assert_json_snapshot!(
            "vnd_json_v1_without_sql",
            serde_json::from_str::<serde_json::Value>(&result_without_sql).expect("to parse")
        );

        // Test conversion with SQL
        let metadata = ResponseMetadata::empty()
            .with_sql("SELECT customer_id, total_sales FROM sales_summary LIMIT 2;");
        let result_with_sql = arrow_to_vnd_sql_json_v1(&[batch], metadata).expect("to convert");
        insta::assert_json_snapshot!(
            "vnd_json_v1_with_sql",
            serde_json::from_str::<serde_json::Value>(&result_with_sql).expect("to parse")
        );
    }
}
