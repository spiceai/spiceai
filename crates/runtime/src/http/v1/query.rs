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
use std::sync::Arc;

use axum::{
    body::Bytes,
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension,
};
use axum_extra::TypedHeader;
use headers_accept::Accept;

use crate::datafusion::DataFusion;

use super::{sql_to_http_response, ArrowFormat};

/// SQL Query
///
/// Execute a SQL query and return the results.
///
/// This endpoint allows users to execute SQL queries directly from an HTTP request. The SQL query is sent as plain text in the request body.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/sql",
    operation_id = "post_sql",
    tag = "SQL",
    params(
        ("Accept" = String, Header, description = "The format of the response, one of 'application/json' (default), 'text/csv' or 'text/plain'."),
    ),
    request_body(
        description = "SQL query to execute",
        content((
            String = "text/plain",
            example = "SELECT avg(total_amount), avg(tip_amount), count(1), passenger_count FROM my_table GROUP BY passenger_count ORDER BY passenger_count ASC LIMIT 3"
        ))
    ),
    responses(
        (status = 200, description = "SQL query executed successfully (JSON format)", content((
            Vec<serde_json::Value> = "application/json",
            example = json!([
                {
                    "AVG(my_table.tip_amount)": 3.072_259_971_396_793,
                    "AVG(my_table.total_amount)": 25.327_816_939_456_525,
                    "COUNT(Int64(1))": 31_465,
                    "passenger_count": 0
                },
                {
                    "AVG(my_table.tip_amount)": 3.371_262_288_468_005_7,
                    "AVG(my_table.total_amount)": 26.205_230_445_474_996,
                    "COUNT(Int64(1))": 2_188_739,
                    "passenger_count": 1
                },
                {
                    "AVG(my_table.tip_amount)": 3.717_130_211_329_085_4,
                    "AVG(my_table.total_amount)": 29.520_659_930_930_304,
                    "COUNT(Int64(1))": 405_103,
                    "passenger_count": 2
                }
            ])
        ),
        (
        String = "text/csv", example = r#""AVG(my_table.tip_amount)","AVG(my_table.total_amount)","COUNT(Int64(1))","passenger_count"
3.072259971396793,25.327816939456525,31465,0
3.3712622884680057,26.205230445474996,2188739,1
3.7171302113290854,29.520659930930304,405103,2"#
        ),
        (
            String = "text/plain",
            example = r#"
            +----------------------------+----------------------------+----------------+---------------------+
            | "AVG(my_table.tip_amount)"  | "AVG(my_table.total_amount)" | "COUNT(Int64(1))" | "passenger_count"   |
            +----------------------------+----------------------------+----------------+---------------------+
            | 3.072259971396793           | 25.327816939456525         | 31465          | 0                   |
            +----------------------------+----------------------------+----------------+---------------------+
            | 3.3712622884680057          | 26.205230445474996         | 2188739        | 1                   |
            +----------------------------+----------------------------+----------------+---------------------+
            | 3.7171302113290854          | 29.520659930930304         | 405103         | 2                   |
            +----------------------------+----------------------------+----------------+---------------------+"#
                )
        )),
        (status = 400, description = "Invalid SQL query or malformed input", content((
            String,
            example = "Error reading query: invalid UTF-8 sequence"
        ))),
        (status = 500, description = "Internal server error", content((
            String,
            example = "Unexpected internal server error occurred"
        )))
    )
))]
pub(crate) async fn post(
    Extension(df): Extension<Arc<DataFusion>>,
    accept: Option<TypedHeader<Accept>>,
    body: Bytes,
) -> Response {
    let query = match String::from_utf8(body.to_vec()) {
        Ok(query) => query,
        Err(e) => {
            tracing::debug!("Error reading query: {e}");
            return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
        }
    };

    sql_to_http_response(df, &query, ArrowFormat::from_accept_header(accept.as_ref())).await
}
