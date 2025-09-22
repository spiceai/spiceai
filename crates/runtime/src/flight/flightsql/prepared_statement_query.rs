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

use std::{borrow::Cow, ops::ControlFlow};

use arrow::compute::concat_batches;
use arrow_flight::{
    FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, PutResult, Ticket,
    decode::{DecodedPayload, FlightDataDecoder},
    error::FlightError,
    flight_service_server::FlightService,
    sql::{self, CommandPreparedStatementQuery, DoPutPreparedStatementResult, ProstMessageExt},
};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::SchemaRef;
use arrow_tools::record_batch::record_to_param_values;
use bytes::Bytes;
use datafusion::{
    common::ParamValues,
    sql::sqlparser::{
        ast::{Expr, Statement, Value, VisitMut, VisitorMut},
        dialect::GenericDialect,
        parser::{Parser, ParserError},
    },
};
use postcard::{from_bytes, to_stdvec};
use prost::Message;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use tokio_stream::{StreamExt, adapters::Peekable};
use tonic::{Request, Response, Status, Streaming};

use crate::{
    datafusion::request_context_extension::get_current_datafusion,
    flight::{
        Service, metrics, to_tonic_err,
        util::{attach_cache_metadata, set_flightsql_protocol},
    },
    request::{AsyncMarker, RequestContext},
    timing::TimedStream,
};

#[derive(Serialize, Deserialize)]
pub(crate) struct PreparedStatement {
    query: String,
    parameters: Vec<u8>,
}

/// Create a prepared statement from given SQL statement.
pub(crate) async fn do_action_create_prepared_statement(
    statement: sql::ActionCreatePreparedStatementRequest,
) -> Result<sql::ActionCreatePreparedStatementResult, Status> {
    tracing::trace!("do_action_create_prepared_statement: {statement:?}");
    set_flightsql_protocol().await;

    let query = convert_jdbc_parameter_placeholders(&statement.query).map_err(error_to_status)?;

    let context = RequestContext::current(AsyncMarker::new().await);
    let datafusion = get_current_datafusion(&context);

    let (dataset_schema, parameter_schema) = Service::get_arrow_schema(datafusion, &query)
        .await
        .map_err(to_tonic_err)?;

    let dataset_schema = Service::serialize_schema(&dataset_schema)?;
    let parameter_schema = if let Some(schema) = &parameter_schema {
        Service::serialize_schema(schema)?
    } else {
        Bytes::default()
    };

    let stmt = PreparedStatement {
        query: query.to_string(),
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
    query: sql::CommandPreparedStatementQuery,
) -> Result<Response<<Service as FlightService>::DoGetStream>, Status> {
    let start = metrics::track_flight_request("do_get", Some("prepared_statement_query")).await;
    set_flightsql_protocol().await;

    let context = RequestContext::current(AsyncMarker::new().await);
    let datafusion = get_current_datafusion(&context);

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
    query: CommandPreparedStatementQuery,
    streaming_flight: Peekable<Streaming<FlightData>>,
) -> Result<Response<<Service as FlightService>::DoPutStream>, Status> {
    let streaming_flight = streaming_flight
        .map(|flight_data| flight_data.map_err(|status| FlightError::Tonic(Box::new(status))));

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
    #[snafu(display("Multiple statements found in query. Only one statement is supported."))]
    MultipleStatements,

    #[snafu(display("Invalid query: {query} {source}"))]
    InvalidQuery { query: String, source: ParserError },
}

/// Converts any JDBC parameter placeholders to Postgres-style placeholders.
///
/// This function handles the conversion of JDBC parameter placeholders (e.g., `?`) to
/// Postgres placeholders (e.g., `$1`, `$2`, etc.). If the query does not contain any JDBC
/// parameter placeholders, the original query is returned unchanged.
fn convert_jdbc_parameter_placeholders(query: &str) -> Result<Cow<'_, str>, Error> {
    // Simple check for the common case where the query does not contain any JDBC parameter placeholders
    if !query.contains('?') {
        return Ok(Cow::Borrowed(query));
    }

    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, query).context(InvalidQuerySnafu { query })?;
    if statements.len() != 1 {
        return Err(Error::MultipleStatements);
    }
    let Some(mut statement) = statements.pop() else {
        unreachable!("We already checked that there is exactly one statement");
    };

    let mut visitor = ConvertJdbcPlaceholdersVisitor::new();
    visitor.visit_statement(&mut statement);

    Ok(Cow::Owned(statement.to_string()))
}

struct ConvertJdbcPlaceholdersVisitor {
    next_placeholder: u32,
}

impl ConvertJdbcPlaceholdersVisitor {
    fn new() -> Self {
        Self {
            next_placeholder: 1,
        }
    }

    fn visit_statement(&mut self, statement: &mut Statement) {
        let _ = statement.visit(self);
    }
}

impl VisitorMut for ConvertJdbcPlaceholdersVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Value(value_with_span) = expr
            && let Value::Placeholder(ref mut placeholder) = value_with_span.value
        {
            let new_placeholder = placeholder.replace('?', &format!("${}", self.next_placeholder));
            value_with_span.value = Value::Placeholder(new_placeholder);
            self.next_placeholder += 1;
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_query() {
        let input = "SELECT * FROM users WHERE id = ? AND name = ?";
        let expected = "SELECT * FROM users WHERE id = $1 AND name = $2";
        assert_eq!(
            convert_jdbc_parameter_placeholders(input).expect("should not fail"),
            expected
        );
    }

    #[test]
    fn test_query_with_quotes() {
        let input = "SELECT * FROM users WHERE name = 'test?' AND id = ?";
        let expected = "SELECT * FROM users WHERE name = 'test?' AND id = $1";
        assert_eq!(
            convert_jdbc_parameter_placeholders(input).expect("should not fail"),
            expected
        );
    }

    #[test]
    fn test_query_with_comments() {
        let input = "SELECT * FROM users WHERE id = ? -- comment with ?";
        let expected = "SELECT * FROM users WHERE id = $1";
        assert_eq!(
            convert_jdbc_parameter_placeholders(input).expect("should not fail"),
            expected
        );
    }

    #[test]
    fn test_query_with_backticks() {
        let input = "SELECT `name`, `age` FROM `users` WHERE `id` = ?";
        let expected = "SELECT `name`, `age` FROM `users` WHERE `id` = $1";
        assert_eq!(
            convert_jdbc_parameter_placeholders(input).expect("should not fail"),
            expected
        );
    }

    #[test]
    fn test_query_with_double_quotes() {
        let input = r#"SELECT * FROM "users" WHERE name = "?" AND id = ?"#;
        let expected = r#"SELECT * FROM "users" WHERE name = "?" AND id = $1"#;
        assert_eq!(
            convert_jdbc_parameter_placeholders(input).expect("should not fail"),
            expected
        );
    }

    #[test]
    fn test_unclosed_quote_no_placeholders() {
        let input = "SELECT * FROM users WHERE name = 'test";

        // Should return the original query because it doesn't contain any JDBC parameter placeholders
        assert_eq!(
            convert_jdbc_parameter_placeholders(input).expect("should not fail"),
            Cow::Borrowed(input)
        );
    }

    #[test]
    fn test_unclosed_quote_with_placeholders() {
        let input = "SELECT * FROM users WHERE name = 'test?";
        assert!(matches!(
            convert_jdbc_parameter_placeholders(input).expect_err("should fail"),
            Error::InvalidQuery { .. }
        ));
    }

    #[test]
    fn test_query_with_one_placeholders() {
        let input = "SELECT name, age FROM users WHERE id = ?";
        let expected = "SELECT name, age FROM users WHERE id = $1";
        assert_eq!(
            convert_jdbc_parameter_placeholders(input).expect("should not fail"),
            expected
        );
    }

    #[test]
    fn test_postgres_style() {
        let input = "SELECT name FROM users WHERE id = $1";
        let expected = "SELECT name FROM users WHERE id = $1";
        assert_eq!(
            convert_jdbc_parameter_placeholders(input).expect("should not fail"),
            expected
        );
    }

    #[test]
    fn test_already_postgres_style() {
        let input = "SELECT * FROM users WHERE id = $1 AND name = $2";
        let expected = "SELECT * FROM users WHERE id = $1 AND name = $2";
        assert_eq!(
            convert_jdbc_parameter_placeholders(input).expect("should not fail"),
            expected
        );
    }

    #[test]
    fn test_postgres_style_with_quotes() {
        let input = r#"SELECT * FROM "users" WHERE name = '$1' AND id = $1"#;
        let expected = r#"SELECT * FROM "users" WHERE name = '$1' AND id = $1"#;
        assert_eq!(
            convert_jdbc_parameter_placeholders(input).expect("should not fail"),
            expected
        );
    }

    #[test]
    fn test_complex_query_multiple_clauses() {
        let input = "SELECT a, b FROM t WHERE x = ? AND y = ? GROUP BY a ORDER BY b DESC LIMIT ?";
        let expected =
            "SELECT a, b FROM t WHERE x = $1 AND y = $2 GROUP BY a ORDER BY b DESC LIMIT $3";
        assert_eq!(
            convert_jdbc_parameter_placeholders(input)
                .expect("should not fail")
                .as_ref(),
            expected
        );
    }

    #[test]
    fn test_insert_statement() {
        let input = "INSERT INTO users (name, age) VALUES (?, ?)";
        let expected = "INSERT INTO users (name, age) VALUES ($1, $2)";
        assert_eq!(
            convert_jdbc_parameter_placeholders(input)
                .expect("should not fail")
                .as_ref(),
            expected
        );
    }

    #[test]
    fn test_update_statement() {
        let input = "UPDATE users SET age = ? WHERE name = ?";
        let expected = "UPDATE users SET age = $1 WHERE name = $2";
        assert_eq!(
            convert_jdbc_parameter_placeholders(input)
                .expect("should not fail")
                .as_ref(),
            expected
        );
    }

    #[test]
    fn test_delete_statement() {
        let input = "DELETE FROM users WHERE id = ?";
        let expected = "DELETE FROM users WHERE id = $1";
        assert_eq!(
            convert_jdbc_parameter_placeholders(input)
                .expect("should not fail")
                .as_ref(),
            expected
        );
    }

    #[test]
    fn test_query_with_function_calls() {
        let input = "SELECT COUNT(*) FROM users WHERE created_at > ? AND status = ?";
        let expected = "SELECT COUNT(*) FROM users WHERE created_at > $1 AND status = $2";
        assert_eq!(
            convert_jdbc_parameter_placeholders(input)
                .expect("should not fail")
                .as_ref(),
            expected
        );
    }

    #[test]
    fn test_query_with_subquery() {
        let input = "SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products WHERE category = ?) AND stock > ?";
        let expected = "SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products WHERE category = $1) AND stock > $2";
        assert_eq!(
            convert_jdbc_parameter_placeholders(input)
                .expect("should not fail")
                .as_ref(),
            expected
        );
    }

    #[test]
    fn test_no_placeholders() {
        let input = "SELECT * FROM users WHERE id = 1";
        let expected = "SELECT * FROM users WHERE id = 1";
        assert_eq!(
            convert_jdbc_parameter_placeholders(input).expect("should not fail"),
            expected
        );
    }

    #[test]
    fn test_mix_of_question_marks_in_literals_and_placeholders() {
        let input = "SELECT '?', name FROM users WHERE id = ? AND notes LIKE '%??%'";
        let expected = "SELECT '?', name FROM users WHERE id = $1 AND notes LIKE '%??%'";
        assert_eq!(
            convert_jdbc_parameter_placeholders(input)
                .expect("should not fail")
                .as_ref(),
            expected
        );
    }
}
