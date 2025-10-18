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
use futures::{StreamExt, TryStreamExt};
use postcard::{from_bytes, to_stdvec};
use prost::Message;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use tokio_stream::adapters::Peekable;
use tonic::{Request, Response, Status, Streaming};

use crate::{
    datafusion::request_context_extension::get_current_datafusion,
    flight::{
        Service, metrics, to_tonic_err,
        util::{attach_cache_metadata, set_flightsql_protocol},
    },
    timing::TimedStream,
};
use runtime_request_context::{AsyncMarker, RequestContext};

/// Attempts to rewrite SQL to include explicit type casts for parameters.
/// This helps `DataFusion` infer parameter types for queries like "SELECT $1 + $2".
///
/// For each parameter $N, wraps it in a CAST($N AS <type>) based on the schema.
fn rewrite_sql_with_type_casts(sql: &str, schema: &SchemaRef) -> String {
    use arrow::datatypes::DataType;

    let mut rewritten = sql.to_string();

    // For each field in the schema (representing each parameter), replace $N with CAST($N AS type)
    for (idx, field) in schema.fields().iter().enumerate() {
        let param_num = idx + 1;
        let param_placeholder = format!("${param_num}");

        // Determine the SQL type name from Arrow DataType
        let sql_type = match field.data_type() {
            DataType::Int8 => "TINYINT",
            DataType::Int16 => "SMALLINT",
            DataType::Int32 => "INT",
            DataType::Int64 => "BIGINT",
            DataType::UInt8 => "TINYINT UNSIGNED",
            DataType::UInt16 => "SMALLINT UNSIGNED",
            DataType::UInt32 => "INT UNSIGNED",
            DataType::UInt64 => "BIGINT UNSIGNED",
            DataType::Float32 => "FLOAT",
            DataType::Float64 => "DOUBLE",
            DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR",
            DataType::Boolean => "BOOLEAN",
            DataType::Date32 | DataType::Date64 => "DATE",
            DataType::Timestamp(_, _) => "TIMESTAMP",
            _ => {
                // For unsupported types, skip casting
                tracing::warn!(
                    "Cannot cast parameter ${} with unsupported type: {:?}",
                    param_num,
                    field.data_type()
                );
                continue;
            }
        };

        // Replace all occurrences of $N with CAST($N AS type)
        // Use word boundaries to avoid replacing $1 in $10, $11, etc.
        let cast_expr = format!("CAST({param_placeholder} AS {sql_type})");

        // Simple replacement - this could be improved with proper SQL parsing
        // but should work for most cases
        rewritten = rewritten.replace(&format!("{param_placeholder} "), &format!("{cast_expr} "));
        rewritten = rewritten.replace(&format!("{param_placeholder})"), &format!("{cast_expr})"));
        rewritten = rewritten.replace(&format!("{param_placeholder},"), &format!("{cast_expr},"));

        // Handle cases where parameter is at the end of the SQL
        if rewritten.ends_with(&param_placeholder) {
            rewritten = rewritten.trim_end_matches(&param_placeholder).to_string() + &cast_expr;
        }
    }

    rewritten
}

#[derive(Serialize, Deserialize)]
pub(crate) struct PreparedStatement {
    pub(super) query: String,
    pub(super) parameters: Vec<u8>,
    /// Parameter schema - stores the Arrow schema of bound parameters from `DoPut`
    /// This schema provides type information for each parameter (e.g., Int64, Utf8, etc.)
    /// and is used to create a properly typed logical plan during execution
    pub(super) parameter_schema: Option<Vec<u8>>,
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

    // Try to get schema, but if it fails due to type inference issues with parameters,
    // we'll return empty schemas. The actual type checking will happen when parameters are bound.
    let (dataset_schema, parameter_schema) = match Service::get_arrow_schema(datafusion, &query)
        .await
    {
        Ok(schemas) => schemas,
        Err(e) => {
            // Check if this is a type inference error related to parameters
            let err_msg = e.to_string();
            if err_msg.contains("Cannot get result type")
                || err_msg.contains("Invalid arithmetic operation")
                || err_msg.contains("type inference")
                || err_msg.contains("No field named")
            {
                tracing::debug!(
                    "Could not infer schema during prepare (will be determined at execution): {err_msg}"
                );
                // Return empty schema - types will be determined when parameters are bound
                (arrow_schema::Schema::empty(), None)
            } else {
                // This is a real error (syntax error, unknown table, etc.), return it
                return Err(e);
            }
        }
    };

    let dataset_schema = Service::serialize_schema(&dataset_schema)?;
    let parameter_schema = if let Some(schema) = &parameter_schema {
        Service::serialize_schema(schema)?
    } else {
        Bytes::default()
    };

    let stmt = PreparedStatement {
        query: query.to_string(),
        parameters: vec![],
        parameter_schema: None,
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

    // Decode the prepared statement to get the query and retrieve its schema
    let PreparedStatement { query: sql, .. } =
        from_bytes(&handle.prepared_statement_handle).map_err(error_to_status)?;

    let context = RequestContext::current(AsyncMarker::new().await);
    let datafusion = get_current_datafusion(&context);

    // Try to get schema, but if it fails due to type inference issues with parameters,
    // we'll omit the schema from FlightInfo. The actual schema will be determined during execution.
    let maybe_arrow_schema = match Service::get_arrow_schema(datafusion, &sql).await {
        Ok((schema, _)) => Some(schema),
        Err(e) => {
            let err_msg = e.to_string();
            if err_msg.contains("Cannot get result type")
                || err_msg.contains("Invalid arithmetic operation")
                || err_msg.contains("type inference")
                || err_msg.contains("No field named")
            {
                tracing::debug!(
                    "Could not infer schema for prepared statement (will be determined at execution): {err_msg}"
                );
                // Return None to indicate schema is unknown - it will be determined during execution
                None
            } else {
                return Err(e);
            }
        }
    };

    let fd = request.into_inner();

    let endpoint = FlightEndpoint::new().with_ticket(Ticket {
        ticket: handle.as_any().encode_to_vec().into(),
    });

    let mut info = FlightInfo::new()
        .with_endpoint(endpoint)
        .with_descriptor(fd);

    // Only include schema if we were able to infer it
    if let Some(schema) = maybe_arrow_schema {
        info = info.try_with_schema(&schema).map_err(to_tonic_err)?;
    }

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
        parameter_schema,
    } = from_bytes(&query.prepared_statement_handle).map_err(error_to_status)?;

    tracing::info!(
        "do_get: Query: {}, Parameters length: {}",
        sql,
        parameters.len()
    );

    let param_values = decode_param_values(&parameters).map_err(error_to_status)?;

    tracing::info!("do_get: Decoded parameters: {:?}", param_values);

    // If we have parameter schema from DoPut, try to use it to help with type inference
    // by rewriting the SQL to include explicit type casts
    let sql_to_execute = if let Some(schema_bytes) = &parameter_schema {
        tracing::info!("do_get: Have parameter schema, attempting to rewrite SQL with type casts");

        // Decode the parameter schema
        let schema = {
            let reader = arrow::ipc::reader::StreamReader::try_new(&schema_bytes[..], None)
                .map_err(error_to_status)?;
            reader.schema()
        };

        tracing::info!("do_get: Parameter schema: {:?}", schema);

        // Try to rewrite the SQL with type casts to help DataFusion infer types
        let rewritten = rewrite_sql_with_type_casts(&sql, &schema);
        tracing::info!("do_get: Rewritten SQL: {}", rewritten);
        Cow::Owned(rewritten)
    } else {
        Cow::Borrowed(sql.as_str())
    };

    // Use the standard flow with the (possibly rewritten) SQL
    let (output, from_cache) = Box::pin(Service::sql_to_flight_stream(
        datafusion,
        &sql_to_execute,
        param_values,
    ))
    .await?;
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
    tracing::info!("do_put_query: Binding parameters to prepared statement");

    let streaming_flight = streaming_flight
        .map(|flight_data| flight_data.map_err(|status| FlightError::Tonic(Box::new(status))));

    let mut decoder = FlightDataDecoder::new(streaming_flight);
    let schema = decode_schema(&mut decoder).await?;

    tracing::info!("do_put_query: Parameter schema: {:?}", schema);

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

    // Serialize the parameter schema for later use in query planning
    let schema_bytes = {
        let mut bytes = Vec::new();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut bytes, &schema)
            .map_err(error_to_status)?;
        writer.finish().map_err(error_to_status)?;
        bytes
    };

    let mut stmt: PreparedStatement =
        from_bytes(&query.prepared_statement_handle).map_err(error_to_status)?;
    stmt.parameters = parameters;
    stmt.parameter_schema = Some(schema_bytes);
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
pub(super) fn decode_param_values(
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

pub(super) fn error_to_status<E: std::fmt::Debug>(err: E) -> Status {
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
    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    /// Helper to encode a `RecordBatch` into Arrow IPC format for parameters
    fn encode_params_to_bytes(batch: &RecordBatch) -> Result<Vec<u8>, arrow::error::ArrowError> {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(Vec::new(), &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
        writer.into_inner()
    }

    #[test]
    fn test_convert_query_with_single_parameter() {
        // Test that JDBC placeholders are converted to Postgres style
        let query = "SELECT ? + 1 AS result";
        let result = convert_jdbc_parameter_placeholders(query);

        assert!(result.is_ok());
        if let Ok(converted) = result {
            assert_eq!(converted, "SELECT $1 + 1 AS result");
        }
    }

    #[test]
    fn test_convert_query_with_multiple_parameters() {
        // Test multiple placeholders
        let query = "SELECT ? + ? AS sum, ? * ? AS product";
        let result = convert_jdbc_parameter_placeholders(query);

        assert!(result.is_ok());
        if let Ok(converted) = result {
            assert_eq!(converted, "SELECT $1 + $2 AS sum, $3 * $4 AS product");
        }
    }

    #[test]
    fn test_convert_query_with_string_parameters() {
        let query = "SELECT ? || ' ' || ? AS greeting";
        let result = convert_jdbc_parameter_placeholders(query);

        assert!(result.is_ok());
        if let Ok(converted) = result {
            assert_eq!(converted, "SELECT $1 || ' ' || $2 AS greeting");
        }
    }

    #[tokio::test]
    async fn test_decode_param_values_single_int() {
        // Create a RecordBatch with a single int64 parameter
        let schema = Arc::new(Schema::new(vec![Field::new(
            "param1",
            DataType::Int64,
            false,
        )]));
        let array = Arc::new(Int64Array::from(vec![42]));
        let batch = match RecordBatch::try_new(schema, vec![array]) {
            Ok(b) => b,
            Err(e) => panic!("Failed to create RecordBatch: {e}"),
        };

        // Encode to bytes
        let bytes = match encode_params_to_bytes(&batch) {
            Ok(b) => b,
            Err(e) => panic!("Failed to encode params: {e}"),
        };

        // Decode
        let result = decode_param_values(&bytes);
        assert!(
            result.is_ok(),
            "Should decode successfully: {:?}",
            result.err()
        );

        if let Ok(Some(_params)) = result {
            // Successfully decoded parameters
        } else {
            panic!("Expected Some parameters");
        }
    }

    #[tokio::test]
    async fn test_decode_param_values_multiple_types() {
        // Create a RecordBatch with multiple parameter types
        let schema = Arc::new(Schema::new(vec![
            Field::new("int_param", DataType::Int64, false),
            Field::new("float_param", DataType::Float64, false),
            Field::new("string_param", DataType::Utf8, false),
            Field::new("bool_param", DataType::Boolean, false),
        ]));

        let batch = match RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(Float64Array::from(vec![3.5])),
                Arc::new(StringArray::from(vec!["hello"])),
                Arc::new(BooleanArray::from(vec![true])),
            ],
        ) {
            Ok(b) => b,
            Err(e) => panic!("Failed to create RecordBatch: {e}"),
        };

        let bytes = match encode_params_to_bytes(&batch) {
            Ok(b) => b,
            Err(e) => panic!("Failed to encode params: {e}"),
        };
        let result = decode_param_values(&bytes);

        assert!(
            result.is_ok(),
            "Should decode successfully: {:?}",
            result.err()
        );
        if let Ok(Some(_params)) = result {
            // Successfully decoded parameters
        } else {
            panic!("Expected Some parameters");
        }
    }

    #[tokio::test]
    async fn test_decode_param_values_empty() {
        // Empty bytes should return None
        let result = decode_param_values(&[]);
        assert!(result.is_ok());
        if let Ok(params) = result {
            assert!(params.is_none(), "Empty bytes should return None");
        }
    }

    #[tokio::test]
    async fn test_prepared_statement_serialization() {
        let stmt = PreparedStatement {
            query: "SELECT ? + 1".to_string(),
            parameters: vec![1, 2, 3],
            parameter_schema: None,
        };

        // Serialize
        let bytes = match to_stdvec(&stmt) {
            Ok(b) => b,
            Err(e) => panic!("Failed to serialize: {e}"),
        };
        assert!(!bytes.is_empty());

        // Deserialize
        let decoded: PreparedStatement = match from_bytes(&bytes) {
            Ok(d) => d,
            Err(e) => panic!("Failed to deserialize: {e}"),
        };
        assert_eq!(decoded.query, stmt.query);
        assert_eq!(decoded.parameters, stmt.parameters);
        assert_eq!(decoded.parameter_schema, stmt.parameter_schema);
    }

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
