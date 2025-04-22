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

use arrow::compute::concat_batches;
use arrow_flight::{
    FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, PutResult, Ticket,
    decode::{DecodedPayload, FlightDataDecoder},
    flight_service_server::FlightService,
    sql::{self, CommandPreparedStatementQuery, DoPutPreparedStatementResult, ProstMessageExt},
};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::SchemaRef;
use arrow_tools::record_batch::record_to_param_values;
use datafusion::common::ParamValues;
use postcard::{from_bytes, to_stdvec};
use prost::Message;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use tokio_stream::{StreamExt, adapters::Peekable};
use tonic::{Request, Response, Status, Streaming};

use crate::{
    flight::{
        Service, metrics, to_tonic_err,
        util::{attach_cache_metadata, set_flightsql_protocol},
    },
    timing::TimedStream,
};

#[derive(Serialize, Deserialize)]
pub(crate) struct PreparedStatement {
    query: String,
    parameters: Vec<u8>,
}

/// Create a prepared statement from given SQL statement.
pub(crate) async fn do_action_create_prepared_statement(
    flight_svc: &Service,
    statement: sql::ActionCreatePreparedStatementRequest,
) -> Result<sql::ActionCreatePreparedStatementResult, Status> {
    tracing::trace!("do_action_create_prepared_statement: {statement:?}");
    set_flightsql_protocol().await;

    let query = mysql_to_postgres_query(&statement.query).map_err(error_to_status)?;

    let (dataset_schema, parameter_schema) =
        Service::get_arrow_schema(Arc::clone(&flight_svc.datafusion), &query)
            .await
            .map_err(to_tonic_err)?;

    let dataset_schema = Service::serialize_schema(&dataset_schema)?;
    let parameter_schema = Service::serialize_schema(
        &parameter_schema.ok_or(Status::internal("no parameter schema"))?,
    )?;

    let stmt = PreparedStatement {
        query: statement.query.clone(),
        parameters: vec![],
    };

    let handle = to_stdvec(&stmt).map_err(error_to_status)?;

    Ok(sql::ActionCreatePreparedStatementResult {
        prepared_statement_handle: handle.into(),
        dataset_schema,
        parameter_schema,
    })
}

pub(crate) async fn get_flight_info(
    _flight_svc: &Service,
    handle: sql::CommandPreparedStatementQuery,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    let _start =
        metrics::track_flight_request("get_flight_info", Some("prepared_statement_query")).await;
    set_flightsql_protocol().await;

    tracing::trace!("get_flight_info_prepared_statement");

    let fd = request.into_inner();

    let endpoint = FlightEndpoint::new().with_ticket(Ticket {
        ticket: handle.as_any().encode_to_vec().into(),
    });

    let info = FlightInfo::new()
        .with_endpoint(endpoint)
        .with_descriptor(fd);

    Ok(Response::new(info))
}

pub(crate) async fn do_get(
    flight_svc: &Service,
    query: sql::CommandPreparedStatementQuery,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let start = metrics::track_flight_request("do_get", Some("prepared_statement_query")).await;
    set_flightsql_protocol().await;

    let datafusion = Arc::clone(&flight_svc.datafusion);
    tracing::trace!("do_get: {query:?}");

    let PreparedStatement {
        query: sql,
        parameters,
    } = from_bytes(&query.prepared_statement_handle).map_err(error_to_status)?;

    let parameters = decode_param_values(&parameters).map_err(error_to_status)?;

    let (output, from_cache) =
        Box::pin(Service::sql_to_flight_stream(datafusion, &sql, parameters)).await?;
    let timed_output = TimedStream::new(output, move || start);

    let mut response =
        Response::new(Box::pin(timed_output) as <Service as FlightService>::DoGetStream);
    attach_cache_metadata(&mut response, from_cache);
    Ok(response)
}

/// Bind the parameters from the [`FlightData`] to the prepared statement
///
/// See [Sequence Diagrams](https://arrow.apache.org/docs/format/FlightSql.html#sequence-diagrams)
pub(crate) async fn do_put_query(
    _flight_svc: &Service,
    query: CommandPreparedStatementQuery,
    streaming_flight: Peekable<Streaming<FlightData>>,
) -> Result<Response<<Service as FlightService>::DoPutStream>, Status> {
    let streaming_flight = streaming_flight
        .map(|flight_data| flight_data.map_err(arrow_flight::error::FlightError::Tonic));

    let mut decoder = FlightDataDecoder::new(streaming_flight);
    let schema = decode_schema(&mut decoder).await?;

    let mut parameters = Vec::new();
    let mut encoder = StreamWriter::try_new(&mut parameters, &schema).map_err(error_to_status)?;
    let mut total_rows = 0;
    while let Some(msg) = decoder.try_next().await? {
        match msg.payload {
            DecodedPayload::None => {}
            DecodedPayload::Schema(_) => {
                return Err(Status::invalid_argument(
                    "parameter flight data must contain a single schema",
                ));
            }
            DecodedPayload::RecordBatch(record_batch) => {
                total_rows += record_batch.num_rows();
                encoder.write(&record_batch).map_err(error_to_status)?;
            }
        }
    }
    if total_rows > 1 {
        return Err(Status::invalid_argument(
            "parameters should contain a single row",
        ));
    }

    let mut stmt: PreparedStatement =
        from_bytes(&query.prepared_statement_handle).map_err(error_to_status)?;
    stmt.parameters = parameters;
    let handle = to_stdvec(&stmt).map_err(error_to_status)?;

    let result = DoPutPreparedStatementResult {
        prepared_statement_handle: Some(handle.into()),
    };

    let output = futures::stream::iter(vec![Ok(PutResult {
        app_metadata: result.encode_to_vec().into(),
    })]);
    Ok(Response::new(Box::pin(output)))
}

async fn decode_schema(decoder: &mut FlightDataDecoder) -> Result<SchemaRef, Status> {
    while let Some(msg) = decoder.try_next().await? {
        match msg.payload {
            DecodedPayload::None => {}
            DecodedPayload::Schema(schema) => {
                return Ok(schema);
            }
            DecodedPayload::RecordBatch(_) => {
                return Err(Status::invalid_argument(
                    "parameter flight data must have a known schema",
                ));
            }
        }
    }

    Err(Status::invalid_argument(
        "parameter flight data must have a schema",
    ))
}

// Decode parameter ipc stream as ParamValues
fn decode_param_values(
    parameters: &[u8],
) -> Result<Option<ParamValues>, datafusion::error::DataFusionError> {
    if parameters.is_empty() {
        Ok(None)
    } else {
        let decoder = StreamReader::try_new(parameters, None)?;
        let schema = decoder.schema();
        let batches = decoder.into_iter().collect::<Result<Vec<_>, _>>()?;
        let batch = concat_batches(&schema, batches.iter())?;
        Ok(Some(record_to_param_values(&batch)?))
    }
}

fn error_to_status<E: std::fmt::Debug>(err: E) -> Status {
    Status::internal(format!("{err:?}"))
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unclosed quote in query"))]
    UnclosedQuote,

    #[snafu(display("Invalid comment syntax at position {}", position))]
    InvalidComment { position: usize },
}

/// Translates a MySQL-style parameterized query to PostgreSQL-style.
/// - Converts MySQL `?` placeholders to Postgres `$1`, `$2`, etc.
/// - Replaces MySQL backticks (`) with Postgres double quotes (") for identifiers.
/// Returns the translated query string or an error if parsing fails.
fn mysql_to_postgres_query(query: &str) -> Result<String, Error> {
    let mut result = String::new();
    let mut param_count = 0;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_backtick = false;
    let mut in_comment = false;
    let mut prev_char = '\0';
    let mut position = 0;

    for c in query.chars() {
        position += 1;
        match c {
            // Handle single quotes
            '\'' if !in_double_quote && !in_backtick && !in_comment => {
                in_single_quote = !in_single_quote;
                result.push(c);
            }
            // Handle double quotes (already in Postgres format)
            '\"' if !in_single_quote && !in_backtick && !in_comment => {
                in_double_quote = !in_double_quote;
                result.push(c);
            }
            // Handle MySQL backticks (convert to double quotes)
            '`' if !in_single_quote && !in_double_quote && !in_comment => {
                in_backtick = !in_backtick;
                result.push('"');
            }
            // Handle line comments
            '-' if prev_char == '-'
                && !in_single_quote
                && !in_double_quote
                && !in_backtick
                && !in_comment =>
            {
                in_comment = true;
                result
                    .pop()
                    .ok_or_else(|| Error::InvalidComment { position })?; // Remove previous '-'
                result.push_str("--");
            }
            // Handle newlines in comments
            '\n' if in_comment => {
                in_comment = false;
                result.push(c);
            }
            // Handle parameter placeholders
            '?' if !in_single_quote && !in_double_quote && !in_backtick && !in_comment => {
                param_count += 1;
                result.push_str(&format!("${param_count}"));
            }
            _ => {
                result.push(c);
            }
        }
        prev_char = c;
    }

    // Check for unclosed quotes or backticks
    if in_single_quote || in_double_quote || in_backtick {
        return Err(Error::UnclosedQuote);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_query() {
        let input = "SELECT * FROM users WHERE id = ? AND name = ?";
        let expected = "SELECT * FROM users WHERE id = $1 AND name = $2";
        assert_eq!(mysql_to_postgres_query(input).unwrap(), expected);
    }

    #[test]
    fn test_query_with_quotes() {
        let input = "SELECT * FROM users WHERE name = 'test?' AND id = ?";
        let expected = "SELECT * FROM users WHERE name = 'test?' AND id = $1";
        assert_eq!(mysql_to_postgres_query(input).unwrap(), expected);
    }

    #[test]
    fn test_query_with_comments() {
        let input = "SELECT * FROM users WHERE id = ? -- comment with ?";
        let expected = "SELECT * FROM users WHERE id = $1 -- comment with ?";
        assert_eq!(mysql_to_postgres_query(input).unwrap(), expected);
    }

    #[test]
    fn test_query_with_double_quotes() {
        let input = r#"SELECT * FROM "users" WHERE name = "?" AND id = ?"#;
        let expected = r#"SELECT * FROM "users" WHERE name = "?" AND id = $1"#;
        assert_eq!(mysql_to_postgres_query(input).unwrap(), expected);
    }

    #[test]
    fn test_unclosed_quote() {
        let input = "SELECT * FROM users WHERE name = 'test";
        assert!(matches!(
            mysql_to_postgres_query(input).unwrap_err(),
            Error::UnclosedQuote
        ));
    }

    #[test]
    fn test_query_with_backticks() {
        let input = "SELECT `name`, `age` FROM `users` WHERE `id` = ?";
        let expected = "SELECT \"name\", \"age\" FROM \"users\" WHERE \"id\" = $1";
        assert_eq!(mysql_to_postgres_query(input).unwrap(), expected);
    }

    #[test]
    fn test_backticks_and_postgres_style() {
        let input = "SELECT `name` FROM `users` WHERE `id` = $1";
        let expected = "SELECT \"name\" FROM \"users\" WHERE \"id\" = $1";
        assert_eq!(mysql_to_postgres_query(input).unwrap(), expected);
    }

    #[test]
    fn test_already_postgres_style() {
        let input = "SELECT * FROM users WHERE id = $1 AND name = $2";
        let expected = "SELECT * FROM users WHERE id = $1 AND name = $2";
        assert_eq!(mysql_to_postgres_query(input).unwrap(), expected);
    }

    #[test]
    fn test_postgres_style_with_quotes() {
        let input = r#"SELECT * FROM "users" WHERE name = '$1' AND id = $1"#;
        let expected = r#"SELECT * FROM "users" WHERE name = '$1' AND id = $1"#;
        assert_eq!(mysql_to_postgres_query(input).unwrap(), expected);
    }
}
