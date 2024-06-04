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

pub mod datasets;
pub mod embed;
pub mod inference;
pub mod models;
pub mod nsql;
pub mod oai;
pub mod query;
pub mod spicepods;
pub mod status;

use std::sync::Arc;

use crate::{
    component::dataset::Dataset,
    datafusion::query::{Protocol, QueryBuilder},
};
use arrow::array::RecordBatch;
use axum::{
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use csv::Writer;
use datafusion::execution::context::SQLOptions;
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
    restricted_sql_options: Option<SQLOptions>,
    nsql: Option<String>,
) -> Response {
    let query = QueryBuilder::new(sql.to_string(), Arc::clone(&df), Protocol::Http)
        .restricted_sql_options(restricted_sql_options)
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
    let buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(buf);

    if let Err(e) = writer.write_batches(data.iter().collect::<Vec<&RecordBatch>>().as_slice()) {
        tracing::debug!("Error converting results to JSON: {e}");
        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }
    if let Err(e) = writer.finish() {
        tracing::debug!("Error finishing JSON conversion: {e}");
        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }

    let buf = writer.into_inner();
    let res = match String::from_utf8(buf) {
        Ok(res) => res,
        Err(e) => {
            tracing::debug!("Error converting JSON buffer to string: {e}");
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
    (StatusCode::OK, headers, res).into_response()
}
