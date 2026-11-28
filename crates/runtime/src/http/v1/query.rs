/*
Copyright 2024-2026 The Spice.ai OSS Authors

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
};
use axum_extra::TypedHeader;
use headers_accept::Accept;
use http::header::CONTENT_TYPE;
use serde::Deserialize;

use crate::datafusion::{param_utils, request_context_extension::get_current_datafusion};
use crate::http::server_timing::ServerTiming;
use datafusion::common::ParamValues;
use runtime_request_context::{AsyncMarker, RequestContext};

use super::{ResponseMimeType, current_principal_requires_read_only, sql_to_http_response};

/// SQL Query
///
/// Execute a SQL query and return the results.
///
/// This endpoint allows users to execute SQL queries directly from an HTTP request. Send the SQL as `text/plain`, or as JSON `{"sql": "..."}` with optional `parameters`.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/sql",
    operation_id = "post_sql",
    tag = "SQL",
    params(
        ("Accept" = String, Header, description = "The format of the response, one of 'application/json' (default), 'application/vnd.spiceai.sql.v1+json', 'text/csv' or 'text/plain'."),
    ),
    request_body(
        description = "SQL query to execute. For `application/json`, `parameters` is optional: omit it or send `null` when the SQL has no placeholders.",
        content(
            (
                String = "text/plain",
                example = "SELECT avg(total_amount), avg(tip_amount), count(1), passenger_count FROM my_table GROUP BY passenger_count ORDER BY passenger_count ASC LIMIT 3"
            ),
            (
                serde_json::Value = "application/json",
                examples(
                    ("SQL only" = (value = json!({
                        "sql": "SELECT * FROM my_table LIMIT 10"
                    }))),
                    ("Positional parameters" = (value = json!({
                        "sql": "SELECT avg(total_amount), avg(tip_amount), count($1), passenger_count FROM my_table GROUP BY passenger_count ORDER BY passenger_count ASC LIMIT $2",
                        "parameters": [1, 3]
                    }))),
                    ("Named parameters" = (value = json!({
                        "sql": "SELECT :foo + 1 AS the_answer",
                        "parameters": {"foo": 41}
                    })))
                )
            )
        )
    ),
    responses(
        (status = 200, description = "SQL query executed successfully", headers(
            ("Server-Timing" = String, description = "The time the server spent on the query, in milliseconds: `total;dur=<ms>` (W3C Server-Timing). Sent in the head of every response whose body is complete before its head is sent: `text/plain` and every error. A streamed `application/json`, `application/vnd.spiceai.sql.v1+json` or `text/csv` body is not complete until it ends, so it sends the field as a trailer instead (declared by `Trailer: Server-Timing`) when the request allows trailers: HTTP/2, or `TE: trailers` on HTTP/1.1.")
        ), content((
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
        ),
        (
            serde_json::Value = "application/vnd.spiceai.sql.v1+json",
            example = json!({
                "schema": {
                    "fields": [
                    {
                        "name": "AVG(my_table.tip_amount)",
                        "data_type": "Float64",
                        "nullable": false,
                        "dict_id": 0,
                        "dict_is_ordered": false
                    },
                    {
                        "name": "AVG(my_table.total_amount)",
                        "data_type": "Float64",
                        "nullable": false,
                        "dict_id": 0,
                        "dict_is_ordered": false
                    },
                    {
                        "name": "COUNT(Int64(1))",
                        "data_type": "Int64",
                        "nullable": false,
                        "dict_id": 0,
                        "dict_is_ordered": false
                    },
                    {
                        "name": "passenger_count",
                        "data_type": "Int64",
                        "nullable": false,
                        "dict_id": 0,
                        "dict_is_ordered": false
                    }
                    ]
                },
                "data": [
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
                ],
                "row_count": 3
            })
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
    headers: axum::http::HeaderMap,
    accept: Option<TypedHeader<Accept>>,
    body: Bytes,
) -> Response {
    let mut response = respond(&headers, accept.as_ref(), &body).await;
    // Every response this handler builds is complete before its head is sent —
    // a query error, a rejected body, a buffered format — except a streamed
    // body, which marked itself for a trailer.
    if response.extensions().get::<ServerTiming>().is_none() {
        response.extensions_mut().insert(ServerTiming::Header);
    }
    response
}

#[derive(Deserialize)]
struct ParameterizedQuery {
    sql: String,
    #[serde(default)]
    parameters: Option<serde_json::Value>,
}

/// `Content-Type` is JSON when its type/subtype is `application/json`, ignoring
/// case and parameters such as `charset`.
fn content_type_is_json(content_type: &str) -> bool {
    let essence = content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    essence == "application/json"
}

/// Parse the `/v1/sql` body: JSON when `Content-Type` is `application/json`,
/// otherwise UTF-8 SQL text. Missing or null `parameters` means no parameters.
fn parse_sql_request(
    content_type: Option<&str>,
    body: &[u8],
) -> Result<(Arc<str>, Option<ParamValues>), (StatusCode, String)> {
    if content_type.is_some_and(content_type_is_json) {
        let ParameterizedQuery { sql, parameters } = serde_json::from_slice(body)
            .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid JSON: {e}")))?;
        let parameters = match parameters {
            None | Some(serde_json::Value::Null) => None,
            Some(value) => Some(
                param_utils::convert_json_to_param_values(value)
                    .map_err(|e| (StatusCode::BAD_REQUEST, format!("Invalid JSON: {e}")))?,
            ),
        };
        // Move the deserialized String into Arc<str> without a second copy.
        Ok((Arc::<str>::from(sql), parameters))
    } else {
        // Decode once into Arc<str> so QueryBuilder does not re-copy the SQL body.
        let sql =
            std::str::from_utf8(body).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
        Ok((Arc::<str>::from(sql), None))
    }
}

async fn respond(
    headers: &axum::http::HeaderMap,
    accept: Option<&TypedHeader<Accept>>,
    body: &Bytes,
) -> Response {
    let context = RequestContext::current(AsyncMarker::new().await);
    let df = get_current_datafusion(&context);

    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok());

    let (sql, parameters) = match parse_sql_request(content_type, body) {
        Ok(parsed) => parsed,
        Err((status, message)) => {
            tracing::debug!("{message}");
            return (status, message).into_response();
        }
    };

    sql_to_http_response(
        df,
        sql,
        parameters,
        ResponseMimeType::from_accept_header(accept),
        current_principal_requires_read_only().await,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::{content_type_is_json, parse_sql_request};
    use axum::http::StatusCode;
    use datafusion::common::ParamValues;

    fn parse_ok(content_type: Option<&str>, body: &[u8]) -> (String, Option<ParamValues>) {
        let (sql, parameters) =
            parse_sql_request(content_type, body).expect("request should parse");
        (sql.to_string(), parameters)
    }

    fn parse_err(content_type: Option<&str>, body: &[u8]) -> (StatusCode, String) {
        parse_sql_request(content_type, body).expect_err("request should be rejected")
    }

    #[test]
    fn json_content_type_ignores_case_and_charset() {
        for value in [
            "application/json",
            "application/json; charset=utf-8",
            "application/json;charset=utf-8",
            "Application/JSON",
            "APPLICATION/JSON; Charset=UTF-8",
            " application/json ",
        ] {
            assert!(content_type_is_json(value), "{value}");
        }
        for value in [
            "text/plain",
            "application/x-www-form-urlencoded",
            "application/vnd.spiceai.sql.v1+json",
            "application/jsonx",
            "",
        ] {
            assert!(!content_type_is_json(value), "{value}");
        }
    }

    #[test]
    fn json_body_without_parameters_is_sql_only() {
        let (sql, parameters) = parse_ok(Some("application/json"), br#"{"sql":"select 1 as one"}"#);
        assert_eq!(sql, "select 1 as one");
        assert!(parameters.is_none());
    }

    #[test]
    fn json_body_with_null_parameters_is_sql_only() {
        let (sql, parameters) = parse_ok(
            Some("application/json"),
            br#"{"sql":"select 1 as one","parameters":null}"#,
        );
        assert_eq!(sql, "select 1 as one");
        assert!(parameters.is_none());
    }

    #[test]
    fn json_body_with_charset_and_empty_parameters_is_json() {
        let (sql, parameters) = parse_ok(
            Some("application/json; charset=utf-8"),
            br#"{"sql":"select 1 as one","parameters":[]}"#,
        );
        assert_eq!(sql, "select 1 as one");
        assert!(
            matches!(parameters, Some(ParamValues::List(ref values)) if values.is_empty()),
            "{parameters:?}"
        );
    }

    #[test]
    fn json_body_with_application_json_case_variant_is_json() {
        let (sql, parameters) = parse_ok(Some("Application/JSON"), br#"{"sql":"select 1 as one"}"#);
        assert_eq!(sql, "select 1 as one");
        assert!(parameters.is_none());
    }

    #[test]
    fn positional_json_parameters_are_kept() {
        let (sql, parameters) = parse_ok(
            Some("application/json"),
            br#"{"sql":"select $1 as one","parameters":[1]}"#,
        );
        assert_eq!(sql, "select $1 as one");
        let Some(ParamValues::List(values)) = parameters else {
            panic!("expected positional parameters, got {parameters:?}");
        };
        assert_eq!(values.len(), 1);
        assert_eq!(
            values[0].value(),
            &datafusion::scalar::ScalarValue::Int64(Some(1))
        );
    }

    #[test]
    fn named_json_parameters_are_kept() {
        let (sql, parameters) = parse_ok(
            Some("application/json"),
            br#"{"sql":"select :foo + 1 as the_answer","parameters":{"foo":41}}"#,
        );
        assert_eq!(sql, "select :foo + 1 as the_answer");
        let Some(ParamValues::Map(values)) = parameters else {
            panic!("expected named parameters, got {parameters:?}");
        };
        assert_eq!(
            values.get("foo").expect("foo").value(),
            &datafusion::scalar::ScalarValue::Int64(Some(41))
        );
    }

    #[test]
    fn text_plain_body_is_sql() {
        let (sql, parameters) = parse_ok(Some("text/plain"), b"select 1 as one");
        assert_eq!(sql, "select 1 as one");
        assert!(parameters.is_none());
    }

    #[test]
    fn malformed_json_is_400() {
        let (status, message) = parse_err(Some("application/json"), b"{not json");
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(message.starts_with("Invalid JSON:"), "{message}");
        assert!(!message.contains("missing field `parameters`"), "{message}");
    }
}
