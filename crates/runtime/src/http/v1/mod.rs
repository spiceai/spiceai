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
pub mod catalogs;
pub mod chat;
pub mod datasets;
pub mod embeddings;
pub mod inference;
pub mod models;
pub mod nsql;
pub mod query;
pub mod ready;
pub mod search;
pub mod spicepods;
pub mod status;

use std::sync::Arc;

use crate::{
    component::dataset::Dataset,
    datafusion::query::{Protocol, QueryBuilder},
};
use arrow::{
    array::RecordBatch,
    util::pretty::pretty_format_batches,
};
use axum::{
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use axum_extra::TypedHeader;
use csv::Writer;
use headers_accept::Accept;
use serde::{Deserialize, Serialize};

use crate::{datafusion::DataFusion, status::ComponentStatus};

use futures::TryStreamExt;

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    #[default]
    Json,
    Csv,
}

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
/// The various formats that the Arrow data can be converted and returned from HTTP requests.
pub enum ArrowFormat {
    #[default]
    Json,
    Csv,
    Shell,
}

impl ArrowFormat {
    pub fn from_accept_header(accept: &Option<TypedHeader<Accept>>) -> ArrowFormat {
        accept.as_ref().map_or(ArrowFormat::default(), |header| {
            header
                .0
                .media_types()
                .find_map(|h| match h.to_string().as_str() {
                    "application/json" => Some(ArrowFormat::Json),
                    "text/csv" => Some(ArrowFormat::Csv),
                    "text/plain" => Some(ArrowFormat::Shell),
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
pub async fn sql_to_http_response(
    df: Arc<DataFusion>,
    sql: &str,
    nsql: Option<&str>,
    format: ArrowFormat,
) -> Response {
    let query = QueryBuilder::new(sql, Arc::clone(&df), Protocol::Http)
        .use_restricted_sql_options()
        .nsql(nsql)
        .protocol(Protocol::Http)
        .build();

    let (data, is_data_from_cache) = match query.run().await {
        Ok(query_result) => match query_result.data.try_collect::<Vec<RecordBatch>>().await {
            Ok(batches) => (batches, query_result.from_cache),
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
        ArrowFormat::Shell => arrow_to_shell(&data),
    };

    let body = match res {
        Ok(body) => body,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let mut headers = HeaderMap::new();

    match is_data_from_cache {
        Some(true) => {
            if let Ok(value) = "Hit from spiceai".parse() {
                headers.insert("X-Cache", value);
            }
        }
        Some(false) => {
            if let Ok(value) = "Miss from spiceai".parse() {
                headers.insert("X-Cache", value);
            }
        }
        None => {}
    };
    (StatusCode::OK, headers, body).into_response()
}

/// Converts a vector of `RecordBatch` to a JSON string.
fn arrow_to_json(data: &[RecordBatch]) -> Result<String, Box<dyn std::error::Error>> {
    let buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(buf);

    if let Err(e) = writer.write_batches(data.iter().collect::<Vec<&RecordBatch>>().as_slice()) {
        tracing::debug!("Error converting results to JSON: {e}");
        return Err(Box::new(e));
    }
    if let Err(e) = writer.finish() {
        tracing::debug!("Error finishing JSON conversion: {e}");
        return Err(Box::new(e));
    }

    match String::from_utf8(writer.into_inner()) {
        Ok(res) => Ok(res),
        Err(e) => {
            tracing::debug!("Error converting JSON buffer to string: {e}");
            Err(Box::new(e))
        }
    }
}

/// Converts a vector of `RecordBatch` to a CSV string.
fn arrow_to_csv(data: &[RecordBatch]) -> Result<String, Box<dyn std::error::Error>> {
    let output = Vec::new();
    let mut writer = arrow_csv::Writer::new(output);

    for d in data {
        if let Err(e) = writer.write(d) {
            tracing::debug!("Error converting arrow records to CSV: {e}");
            return Err(Box::new(e));
        }
    }

    match String::from_utf8(writer.into_inner()) {
        Ok(res) => Ok(res),
        Err(e) => {
            tracing::debug!("Error converting CSV buffer to string: {e}");
            Err(Box::new(e))
        }
    }
}

/// Converts a vector of `RecordBatch` to a pretty formatted string.
/// This is equivalent to [`datafusion::dataframe::DataFrame::show`].
fn arrow_to_shell(data: &[RecordBatch]) -> Result<String, Box<dyn std::error::Error>> {
    match pretty_format_batches(data) {
        Ok(tbl) => Ok(format!("{tbl}")),
        Err(e) => {
            tracing::debug!("Error pretty formatting batches: {e}");
            Err(Box::new(e))
        }
    }
}
