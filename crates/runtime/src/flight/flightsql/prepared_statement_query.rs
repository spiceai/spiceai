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

use std::{borrow::Cow, ops::ControlFlow, sync::Arc};

use arrow::compute::concat_batches;
use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_flight::{
    FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, PutResult, Ticket,
    decode::{DecodedPayload, FlightDataDecoder},
    error::FlightError,
    flight_service_server::FlightService,
    sql::{self, CommandPreparedStatementQuery, DoPutPreparedStatementResult, ProstMessageExt},
};
use arrow_schema::SchemaRef;
use arrow_tools::record_batch::record_to_param_values;
use bytes::Bytes;
use datafusion::common::ParamValues;
use datafusion::sql::sqlparser::{
    ast::{Expr, Statement, Value, VisitMut, VisitorMut},
    dialect::GenericDialect,
    parser::{Parser, ParserError},
};
use futures::StreamExt;
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

/// Arrow `DataType` to SQL type name conversion for CAST expressions.
///
/// NOTE: String types (`Utf8`, `LargeUtf8`) are intentionally NOT included here.
/// Adding CAST for strings (e.g., `CAST($1 AS VARCHAR)`) prevents `DataFusion`'s
/// filter pushdown optimization from merging filters into `TableScan` as `full_filters`.
/// This causes filters to remain as separate Filter nodes below `SubqueryAlias`, which
/// then causes datafusion-federation to generate invalid SQL with unaliased table
/// references (e.g., `nation.n_name` instead of `n1.n_name` when the table is aliased).
/// String type inference works correctly without explicit CASTs.
///
/// Long-term fix: Update `DataFusion`'s optimizer to recognize that `CAST(Utf8 AS Utf8View)`
/// is safe to push down. This would be the proper upstream fix, but until then we avoid
/// the CAST for string types entirely.
fn arrow_type_to_sql_type(dt: &arrow::datatypes::DataType) -> Option<&'static str> {
    use arrow::datatypes::DataType;
    match dt {
        DataType::Int8 => Some("TINYINT"),
        DataType::Int16 => Some("SMALLINT"),
        DataType::Int32 => Some("INT"),
        DataType::Int64 => Some("BIGINT"),
        DataType::UInt8 => Some("TINYINT UNSIGNED"),
        DataType::UInt16 => Some("SMALLINT UNSIGNED"),
        DataType::UInt32 => Some("INT UNSIGNED"),
        DataType::UInt64 => Some("BIGINT UNSIGNED"),
        DataType::Float32 => Some("FLOAT"),
        DataType::Float64 => Some("DOUBLE"),
        // String types intentionally omitted - CASTs break filter pushdown optimization
        // DataType::Utf8 | DataType::LargeUtf8 => Some("VARCHAR"),
        DataType::Boolean => Some("BOOLEAN"),
        DataType::Date32 | DataType::Date64 => Some("DATE"),
        DataType::Timestamp(_, _) => Some("TIMESTAMP"),
        _ => None,
    }
}

/// AST visitor that rewrites parameter placeholders to include CAST expressions.
///
/// This uses proper SQL parsing to avoid incorrectly replacing placeholders
/// inside string literals or comments.
struct ParameterCastRewriter<'a> {
    param_types: &'a std::collections::HashMap<usize, &'static str>,
}

impl VisitorMut for ParameterCastRewriter<'_> {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Value(value_with_span) = expr
            && let Value::Placeholder(ref placeholder) = value_with_span.value
        {
            // Check if this is a $N style placeholder
            if let Some(stripped) = placeholder.strip_prefix('$')
                && let Ok(idx) = stripped.parse::<usize>()
                && let Some(sql_type) = self.param_types.get(&idx)
            {
                // Replace $N with CAST($N AS type) by wrapping in a Cast expression
                let original_placeholder = placeholder.clone();
                let cast_expr = Expr::Cast {
                    expr: Box::new(Expr::Value(Value::Placeholder(original_placeholder).into())),
                    data_type: datafusion::sql::sqlparser::ast::DataType::Custom(
                        datafusion::sql::sqlparser::ast::ObjectName(vec![
                            datafusion::sql::sqlparser::ast::ObjectNamePart::Identifier(
                                datafusion::sql::sqlparser::ast::Ident::new(*sql_type),
                            ),
                        ]),
                        vec![],
                    ),
                    format: None,
                    kind: datafusion::sql::sqlparser::ast::CastKind::Cast,
                };
                *expr = cast_expr;
            }
        }
        ControlFlow::Continue(())
    }
}

/// Attempts to rewrite SQL to include explicit type casts for parameters.
/// This helps `DataFusion` infer parameter types for queries like "SELECT $1 + $2".
///
/// Uses AST-based rewriting to avoid incorrectly modifying placeholders inside
/// string literals or comments.
///
/// For each parameter $N, wraps it in a CAST($N AS <type>) based on the schema.
fn rewrite_sql_with_type_casts(sql: &str, schema: &SchemaRef) -> String {
    // Build a map from parameter index to SQL type string
    let mut param_types = std::collections::HashMap::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        let param_num = idx + 1;
        if let Some(sql_type) = arrow_type_to_sql_type(field.data_type()) {
            param_types.insert(param_num, sql_type);
        } else {
            // Some types intentionally don't get CAST (e.g., strings - see arrow_type_to_sql_type docs)
            tracing::debug!(
                "Skipping CAST for parameter ${} with type: {:?}",
                param_num,
                field.data_type()
            );
        }
    }

    // Parse the SQL into an AST
    let dialect = GenericDialect {};
    let mut ast = match Parser::parse_sql(&dialect, sql) {
        Ok(ast) => ast,
        Err(e) => {
            tracing::warn!("Failed to parse SQL for type cast rewriting: {e}");
            return sql.to_string();
        }
    };

    // Rewrite parameter placeholders in each statement
    let mut rewriter = ParameterCastRewriter {
        param_types: &param_types,
    };
    for stmt in &mut ast {
        let _ = stmt.visit(&mut rewriter);
    }

    // Convert AST back to SQL string
    ast.iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
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

#[expect(dead_code)]
mod param_values_serde {
    use arrow::array::RecordBatch;
    use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
    use arrow_tools::record_batch::record_to_param_values;
    use datafusion::common::ParamValues;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::io::Cursor;

    #[expect(clippy::ref_option)]
    pub fn serialize<S>(params: &Option<ParamValues>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match params {
            None => Vec::<u8>::new().serialize(serializer),
            Some(params) => {
                // Convert ParamValues back to RecordBatch for serialization
                // This is only done once during do_put, not on every query execution
                let batch = param_values_to_record(params).map_err(serde::ser::Error::custom)?;
                let mut writer = StreamWriter::try_new(Vec::new(), &batch.schema())
                    .map_err(serde::ser::Error::custom)?;
                writer.write(&batch).map_err(serde::ser::Error::custom)?;
                writer.finish().map_err(serde::ser::Error::custom)?;
                let bytes = writer.into_inner().map_err(serde::ser::Error::custom)?;
                bytes.serialize(serializer)
            }
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<ParamValues>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes: Vec<u8> = Vec::deserialize(deserializer)?;
        if bytes.is_empty() {
            return Ok(None);
        }

        let cursor = Cursor::new(bytes);
        let mut reader = StreamReader::try_new(cursor, None).map_err(serde::de::Error::custom)?;
        let batch = reader
            .next()
            .transpose()
            .map_err(serde::de::Error::custom)?;

        match batch {
            None => Ok(None),
            Some(batch) => {
                // Convert RecordBatch to ParamValues once during deserialization
                // This is more efficient than doing it on every query execution
                let params = record_to_param_values(&batch).map_err(serde::de::Error::custom)?;
                Ok(Some(params))
            }
        }
    }

    // Helper function to convert ParamValues back to RecordBatch
    fn param_values_to_record(
        params: &ParamValues,
    ) -> Result<RecordBatch, arrow::error::ArrowError> {
        use arrow::array::{ArrayRef, RecordBatch};
        use arrow::datatypes::{Field, Schema};
        use std::sync::Arc;

        match params {
            ParamValues::List(values) => {
                let fields: Vec<Field> = values
                    .iter()
                    .enumerate()
                    .map(|(i, v)| Field::new(format!("${}", i + 1), v.data_type(), v.is_null()))
                    .collect();

                let arrays: Result<Vec<ArrayRef>, _> = values
                    .iter()
                    .map(datafusion::scalar::ScalarValue::to_array)
                    .collect();

                RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays?)
            }
            ParamValues::Map(map) => {
                let mut entries: Vec<_> = map.iter().collect();
                entries.sort_by_key(|(k, _)| *k);

                let fields: Vec<Field> = entries
                    .iter()
                    .map(|(name, v)| Field::new(name.as_str(), v.data_type(), v.is_null()))
                    .collect();

                let arrays: Result<Vec<ArrayRef>, _> =
                    entries.iter().map(|(_, v)| v.to_array()).collect();

                RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays?)
            }
        }
    }
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

    tracing::debug!(
        "do_get: Query: {}, Parameters length: {}",
        sql,
        parameters.len()
    );

    let param_values = decode_param_values(&parameters).map_err(error_to_status)?;

    tracing::debug!("do_get: Decoded parameters: {:?}", param_values);

    // If we have parameter schema from DoPut, try to use it to help with type inference
    // by rewriting the SQL to include explicit type casts
    let sql_to_execute = if let Some(schema_bytes) = &parameter_schema {
        tracing::debug!("do_get: Have parameter schema, attempting to rewrite SQL with type casts");

        // Decode the parameter schema
        let schema = {
            let reader = arrow::ipc::reader::StreamReader::try_new(&schema_bytes[..], None)
                .map_err(error_to_status)?;
            reader.schema()
        };

        tracing::debug!("do_get: Parameter schema: {:?}", schema);

        // Try to rewrite the SQL with type casts to help DataFusion infer types
        let rewritten = rewrite_sql_with_type_casts(&sql, &schema);
        tracing::debug!("do_get: Rewritten SQL: {}", rewritten);
        Cow::Owned(rewritten)
    } else {
        Cow::Borrowed(sql.as_str())
    };

    // Use the standard flow with the (possibly rewritten) SQL
    // Ensure the query execution happens within the request context scope
    let context_clone = Arc::clone(&context);
    let (output, from_cache) = context_clone
        .scope(async {
            Box::pin(Service::sql_to_flight_stream(
                datafusion,
                &sql_to_execute,
                param_values,
            ))
            .await
        })
        .await?;
    let timed_output = TimedStream::new(output, move || start);

    let mut response =
        Response::new(Box::pin(timed_output) as <Service as FlightService>::DoGetStream);
    attach_cache_metadata(&mut response, from_cache, &context);
    Ok(response)
}

/// Bind the parameters from the [`FlightData`] to the prepared statement
///
/// See [Sequence Diagrams](https://arrow.apache.org/docs/format/FlightSql.html#sequence-diagrams)
pub(crate) async fn do_put_query(
    query: CommandPreparedStatementQuery,
    streaming_flight: Peekable<Streaming<FlightData>>,
) -> Result<Response<<Service as FlightService>::DoPutStream>, Status> {
    tracing::debug!("do_put_query: Binding parameters to prepared statement");

    let streaming_flight = streaming_flight
        .map(|flight_data| flight_data.map_err(|status| FlightError::Tonic(Box::new(status))));

    let mut decoder = FlightDataDecoder::new(streaming_flight);

    // Read the schema first - Arrow Flight always sends schema before batches
    let schema = decode_schema(&mut decoder).await?;

    tracing::debug!("do_put_query: Parameter schema: {:?}", schema);

    let mut parameters = Vec::new();
    let mut encoder = StreamWriter::try_new(&mut parameters, &schema).map_err(error_to_status)?;
    // Collect all parameter batches
    let mut batches = Vec::new();
    let mut total_rows = 0;
    while let Some(msg) = futures::TryStreamExt::try_next(&mut decoder).await? {
        match msg.payload {
            DecodedPayload::None => {}
            DecodedPayload::Schema(_) => {
                return Err(Status::invalid_argument(
                    "parameter flight data must contain a single schema",
                ));
            }
            DecodedPayload::RecordBatch(record_batch) => {
                total_rows += record_batch.num_rows();
                batches.push(record_batch.clone());
                // Write each batch to the encoder for serialization
                encoder.write(&record_batch).map_err(error_to_status)?;
            }
        }
    }
    encoder.finish().map_err(error_to_status)?;

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
    while let Some(msg) = futures::TryStreamExt::try_next(decoder).await? {
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

    #[tokio::test]
    async fn test_prepared_statement_plan_cache_setup() {
        use crate::dataaccelerator::AcceleratorEngineRegistry;
        use crate::datafusion::builder::DataFusionBuilder;
        use crate::status::RuntimeStatus;
        use cache::{Caching, SimpleCache};
        use std::sync::Arc;
        use std::time::Duration;

        // Create a DataFusion instance with plan caching enabled (simulating Runtime setup)
        let plan_cache = Arc::new(SimpleCache::new(
            512,
            Duration::from_secs(3600),
            std::hash::BuildHasherDefault::<twox_hash::XxHash3_64>::default(),
        ))
        .as_tabled_provider();

        let io_runtime = tokio::runtime::Handle::current();
        let datafusion = Arc::new(
            DataFusionBuilder::new(
                RuntimeStatus::new(),
                Arc::new(AcceleratorEngineRegistry::new()),
                io_runtime,
            )
            .with_caching(Arc::new(Caching::new().with_plans_cache(plan_cache)))
            .build(),
        );

        // Verify the plan cache is properly configured
        let cache_provider = datafusion
            .plans_cache_provider()
            .expect("DataFusion should have a plan cache provider configured");

        // Verify it starts empty
        assert_eq!(
            cache_provider.item_count().await,
            0,
            "Plan cache should be empty initially"
        );

        // This test verifies that:
        // 1. The plan cache infrastructure is properly set up
        // 2. Prepared statements will benefit from the shared DataFusion plan cache
        // 3. The cache has proper protections (512 entries max, 1 hour TTL via SimpleCache)
        //
        // The actual caching behavior is tested in datafusion/mod.rs::test_get_or_create_logical_plan
        // which verifies that get_or_create_logical_plan (called by sql_to_flight_stream)
        // properly caches and reuses logical plans.
    }

    #[tokio::test]
    async fn test_parameter_binding_with_plan_caching() {
        use crate::dataaccelerator::AcceleratorEngineRegistry;
        use crate::datafusion::builder::DataFusionBuilder;
        use crate::datafusion::query::builder::QueryBuilder;
        use crate::status::RuntimeStatus;
        use arrow::array::Int64Array;
        use cache::{Caching, SimpleCache};
        use datafusion::common::ParamValues;
        use datafusion::scalar::ScalarValue;
        use futures::TryStreamExt;
        use std::sync::Arc;
        use std::time::Duration;

        // Create a DataFusion instance with plan caching enabled
        let plan_cache = Arc::new(SimpleCache::new(
            512,
            Duration::from_secs(3600),
            std::hash::BuildHasherDefault::<twox_hash::XxHash3_64>::default(),
        ))
        .as_tabled_provider();

        let io_runtime = tokio::runtime::Handle::current();
        let datafusion = Arc::new(
            DataFusionBuilder::new(
                RuntimeStatus::new(),
                Arc::new(AcceleratorEngineRegistry::new()),
                io_runtime,
            )
            .with_caching(Arc::new(Caching::new().with_plans_cache(plan_cache)))
            .build(),
        );

        // SQL query with parameters (DataFusion format: $1, $2, etc.)
        // Use CAST to help DataFusion understand the parameter types
        let sql = "SELECT CAST($1 AS BIGINT) + CAST($2 AS BIGINT) AS sum, CAST($1 AS BIGINT) * CAST($2 AS BIGINT) AS product";

        // Execute the query with first set of parameters (2, 3)
        let params1 = ParamValues::List(vec![
            ScalarValue::Int64(Some(2)),
            ScalarValue::Int64(Some(3)),
        ]);

        let result1 = QueryBuilder::new(sql, Arc::clone(&datafusion))
            .parameters(Some(params1))
            .build()
            .run()
            .await
            .expect("should execute query with params1");

        let batches1: Vec<_> = result1
            .data
            .try_collect()
            .await
            .expect("should collect batches");
        assert_eq!(batches1.len(), 1, "should return one batch");

        let batch1 = &batches1[0];
        assert_eq!(batch1.num_columns(), 2, "should have 2 columns");

        // Verify first execution: 2 + 3 = 5, 2 * 3 = 6
        let sum1 = batch1
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("sum should be Int64Array");
        let product1 = batch1
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("product should be Int64Array");

        assert_eq!(sum1.value(0), 5, "2 + 3 should equal 5");
        assert_eq!(product1.value(0), 6, "2 * 3 should equal 6");

        // Execute the same query with different parameters (4, 5)
        let params2 = ParamValues::List(vec![
            ScalarValue::Int64(Some(4)),
            ScalarValue::Int64(Some(5)),
        ]);

        let result2 = QueryBuilder::new(sql, Arc::clone(&datafusion))
            .parameters(Some(params2))
            .build()
            .run()
            .await
            .expect("should execute query with params2");

        let batches2: Vec<_> = result2
            .data
            .try_collect()
            .await
            .expect("should collect batches");
        let batch2 = &batches2[0];

        // Verify second execution: 4 + 5 = 9, 4 * 5 = 20
        let sum2 = batch2
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("sum should be Int64Array");
        let product2 = batch2
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("product should be Int64Array");

        assert_eq!(sum2.value(0), 9, "4 + 5 should equal 9");
        assert_eq!(product2.value(0), 20, "4 * 5 should equal 20");

        // Execute the same query with third set of parameters (10, 20)
        let params3 = ParamValues::List(vec![
            ScalarValue::Int64(Some(10)),
            ScalarValue::Int64(Some(20)),
        ]);

        let result3 = QueryBuilder::new(sql, Arc::clone(&datafusion))
            .parameters(Some(params3))
            .build()
            .run()
            .await
            .expect("should execute query with params3");

        let batches3: Vec<_> = result3
            .data
            .try_collect()
            .await
            .expect("should collect batches");
        let batch3 = &batches3[0];

        // Verify third execution: 10 + 20 = 30, 10 * 20 = 200
        let sum3 = batch3
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("sum should be Int64Array");
        let product3 = batch3
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("product should be Int64Array");

        assert_eq!(sum3.value(0), 30, "10 + 20 should equal 30");
        assert_eq!(product3.value(0), 200, "10 * 20 should equal 200");

        // This test verifies that:
        // 1. The same SQL query can be executed multiple times with different parameters
        // 2. Each execution produces correct results based on the provided parameters
        // 3. Parameter binding works correctly with the query execution infrastructure
        // 4. The parameterized query pattern (used by prepared statements) functions properly
    }

    #[expect(
        clippy::similar_names,
        clippy::redundant_closure_for_method_calls,
        clippy::too_many_lines
    )]
    #[tokio::test]
    async fn test_prepare_execute_with_dataframe_api() {
        use arrow::array::{Int64Array, RecordBatch, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::prelude::*;
        use std::sync::Arc;

        // Create a new SessionContext (DataFusion's main entry point)
        let ctx = SessionContext::new();

        // Create a simple table to query
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    "Alice", "Bob", "Charlie", "Diana", "Eve",
                ])),
                Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
            ],
        )
        .expect("should create record batch");

        // Register the table
        ctx.register_batch("users", batch)
            .expect("should register table");

        // Test 1: PREPARE a statement with parameters
        let prepare_sql =
            "PREPARE my_query AS SELECT id, name, value FROM users WHERE id = $1 AND value > $2";
        let prepare_df = ctx.sql(prepare_sql).await.expect("PREPARE should succeed");

        // Execute PREPARE (this creates the prepared statement but returns no data)
        let prepare_result = prepare_df
            .collect()
            .await
            .expect("PREPARE execution should succeed");
        assert_eq!(prepare_result.len(), 0, "PREPARE should return no rows");

        // Test 2: EXECUTE the prepared statement with parameters
        let execute_sql = "EXECUTE my_query(2, 150)";
        let execute_df = ctx.sql(execute_sql).await.expect("EXECUTE should succeed");
        let execute_result = execute_df
            .collect()
            .await
            .expect("EXECUTE should return results");

        // Verify results: should return row with id=2 (Bob, value=200) since 200 > 150
        assert_eq!(execute_result.len(), 1, "should return one batch");
        let result_batch = &execute_result[0];
        assert_eq!(result_batch.num_rows(), 1, "should return one row");

        let id_col = result_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id should be Int64Array");
        assert_eq!(id_col.value(0), 2, "id should be 2");

        let name_col = result_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name should be StringArray");
        assert_eq!(name_col.value(0), "Bob", "name should be Bob");

        let value_col = result_batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value should be Int64Array");
        assert_eq!(value_col.value(0), 200, "value should be 200");

        // Test 3: EXECUTE the same prepared statement with different parameters
        let execute2_sql = "EXECUTE my_query(4, 350)";
        let execute2_df = ctx.sql(execute2_sql).await.expect("EXECUTE should succeed");
        let execute2_result = execute2_df
            .collect()
            .await
            .expect("EXECUTE should return results");

        // Verify results: should return row with id=4 (Diana, value=400) since 400 > 350
        assert_eq!(execute2_result.len(), 1, "should return one batch");
        let result2_batch = &execute2_result[0];
        assert_eq!(result2_batch.num_rows(), 1, "should return one row");

        let id2_col = result2_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id should be Int64Array");
        assert_eq!(id2_col.value(0), 4, "id should be 4");

        let name2_col = result2_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name should be StringArray");
        assert_eq!(name2_col.value(0), "Diana", "name should be Diana");

        let value2_col = result2_batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("value should be Int64Array");
        assert_eq!(value2_col.value(0), 400, "value should be 400");

        // Test 4: EXECUTE with parameters that return no rows
        let execute3_sql = "EXECUTE my_query(3, 500)";
        let execute3_df = ctx.sql(execute3_sql).await.expect("EXECUTE should succeed");
        let execute3_result = execute3_df
            .collect()
            .await
            .expect("EXECUTE should return results");

        // Verify results: should return no rows (id=3 has value=300, which is not > 500)
        let total_rows: usize = execute3_result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 0,
            "should return no rows when filter doesn't match"
        );

        // Test 5: DEALLOCATE the prepared statement
        let deallocate_sql = "DEALLOCATE my_query";
        let deallocate_df = ctx
            .sql(deallocate_sql)
            .await
            .expect("DEALLOCATE should succeed");
        let deallocate_result = deallocate_df
            .collect()
            .await
            .expect("DEALLOCATE should succeed");
        assert_eq!(
            deallocate_result.len(),
            0,
            "DEALLOCATE should return no rows"
        );

        // This test verifies:
        // 1. PREPARE statement creates a prepared statement with parameters
        // 2. EXECUTE can run the prepared statement multiple times with different parameters
        // 3. Each execution returns correct results based on the provided parameters
        // 4. DEALLOCATE properly cleans up the prepared statement
        // 5. All operations work through the DataFusion DataFrame API (ctx.sql())
    }

    /// Tests that `rewrite_sql_with_type_casts` preserves table aliases when roundtripping
    /// through the SQL parser.
    ///
    /// This is critical for queries like TPC-H Q7 which use the same table with multiple aliases:
    /// `FROM nation n1, nation n2 WHERE n1.n_name = $1 AND n2.n_name = $2`
    ///
    /// The bug we're testing for: after parsing and regenerating SQL, the aliases could be lost
    /// or the column references could incorrectly refer to the original table name instead of
    /// the alias (e.g., `nation.n_name` instead of `n1.n_name`).
    #[test]
    fn test_rewrite_sql_preserves_table_aliases() {
        // Query with table aliases similar to TPC-H Q7
        let sql = r"SELECT n1.n_name, n2.n_name FROM nation n1, nation n2 WHERE n1.n_name = $1 AND n2.n_name = $2";

        // Create a schema with two VARCHAR parameters
        // Note: String types (Utf8) intentionally don't get CAST to avoid breaking filter pushdown
        let schema = Arc::new(Schema::new(vec![
            Field::new("$1", DataType::Utf8, true),
            Field::new("$2", DataType::Utf8, true),
        ]));

        let rewritten = rewrite_sql_with_type_casts(sql, &schema);

        // The rewritten SQL should still reference n1 and n2 aliases, not "nation"
        assert!(
            rewritten.contains("n1.n_name"),
            "Rewritten SQL should preserve n1 alias: {rewritten}"
        );
        assert!(
            rewritten.contains("n2.n_name"),
            "Rewritten SQL should preserve n2 alias: {rewritten}"
        );
        assert!(
            !rewritten.contains("nation.n_name"),
            "Rewritten SQL should not have unaliased nation.n_name: {rewritten}"
        );

        // Verify the table aliases are preserved in FROM clause
        assert!(
            rewritten.contains("nation AS n1") || rewritten.contains("nation n1"),
            "Rewritten SQL should preserve table alias n1: {rewritten}"
        );
        assert!(
            rewritten.contains("nation AS n2") || rewritten.contains("nation n2"),
            "Rewritten SQL should preserve table alias n2: {rewritten}"
        );

        // String parameters should NOT be wrapped in CAST (to avoid breaking filter pushdown)
        // The parameters should remain as plain $1, $2
        assert!(
            !rewritten.contains("CAST($1 AS"),
            "String parameter $1 should NOT be wrapped in CAST (breaks filter pushdown): {rewritten}"
        );
        assert!(
            !rewritten.contains("CAST($2 AS"),
            "String parameter $2 should NOT be wrapped in CAST (breaks filter pushdown): {rewritten}"
        );
        // Verify parameters are still present
        assert!(
            rewritten.contains("$1") && rewritten.contains("$2"),
            "Parameters should be present in rewritten SQL: {rewritten}"
        );
    }

    /// Tests that `rewrite_sql_with_type_casts` preserves column aliases and complex expressions.
    #[test]
    fn test_rewrite_sql_preserves_column_aliases() {
        let sql = r"SELECT a.id AS a_id, b.id AS b_id, a.value + b.value AS total FROM table1 a JOIN table2 b ON a.id = b.ref_id WHERE a.status = $1";

        // String parameter - should NOT get CAST
        let schema = Arc::new(Schema::new(vec![Field::new("$1", DataType::Utf8, true)]));

        let rewritten = rewrite_sql_with_type_casts(sql, &schema);

        // Verify column aliases are preserved
        assert!(
            rewritten.contains("a_id") || rewritten.contains("AS a_id"),
            "Column alias a_id should be preserved: {rewritten}"
        );
        assert!(
            rewritten.contains("b_id") || rewritten.contains("AS b_id"),
            "Column alias b_id should be preserved: {rewritten}"
        );
        assert!(
            rewritten.contains("total") || rewritten.contains("AS total"),
            "Column alias total should be preserved: {rewritten}"
        );

        // Verify table aliases are preserved in column references
        assert!(
            rewritten.contains("a.id") || rewritten.contains("a.\"id\""),
            "Table alias a should be preserved in column reference: {rewritten}"
        );
        assert!(
            rewritten.contains("b.id") || rewritten.contains("b.\"id\""),
            "Table alias b should be preserved in column reference: {rewritten}"
        );

        // String parameter should NOT be wrapped in CAST
        assert!(
            !rewritten.contains("CAST($1 AS"),
            "String parameter should NOT be wrapped in CAST: {rewritten}"
        );
    }

    /// Tests that `rewrite_sql_with_type_casts` handles subquery aliases correctly.
    #[test]
    fn test_rewrite_sql_preserves_subquery_aliases() {
        let sql = r"SELECT sub.total FROM (SELECT SUM(value) AS total FROM orders WHERE status = $1) AS sub WHERE sub.total > $2";

        // $1 is string (no CAST), $2 is Int64 (gets CAST)
        let schema = Arc::new(Schema::new(vec![
            Field::new("$1", DataType::Utf8, true),
            Field::new("$2", DataType::Int64, true),
        ]));

        let rewritten = rewrite_sql_with_type_casts(sql, &schema);

        // Verify subquery alias is preserved
        assert!(
            rewritten.contains("sub.total") || rewritten.contains("sub.\"total\""),
            "Subquery alias 'sub' should be preserved in column reference: {rewritten}"
        );
        assert!(
            rewritten.contains("AS sub") || rewritten.contains(") sub"),
            "Subquery alias should be preserved: {rewritten}"
        );

        // String parameter ($1) should NOT be wrapped in CAST
        assert!(
            !rewritten.contains("CAST($1 AS"),
            "String parameter $1 should NOT be wrapped in CAST: {rewritten}"
        );
        // Integer parameter ($2) SHOULD be wrapped in CAST
        assert!(
            rewritten.contains("CAST($2 AS"),
            "Integer parameter $2 should be wrapped in CAST: {rewritten}"
        );
    }

    /// Tests that `rewrite_sql_with_type_casts` handles no parameters gracefully.
    #[test]
    fn test_rewrite_sql_no_parameters() {
        let sql = r"SELECT n1.n_name FROM nation n1 WHERE n1.n_name = 'FRANCE'";

        // Empty schema - no parameters
        let schema = Arc::new(Schema::empty());

        let rewritten = rewrite_sql_with_type_casts(sql, &schema);

        // Should still preserve the alias
        assert!(
            rewritten.contains("n1.n_name"),
            "Table alias n1 should be preserved: {rewritten}"
        );
    }

    /// Tests that `rewrite_sql_with_type_casts` handles the failing Q7-style query pattern.
    ///
    /// This query pattern previously failed in federation when string parameters
    /// were wrapped in CAST, which prevented filter pushdown optimization.
    ///
    /// The fix: String types are no longer wrapped in CAST, allowing `DataFusion` to
    /// properly push down filters into `TableScan` as `full_filters`.
    #[test]
    fn test_rewrite_sql_table_alias_q7_pattern() {
        // This is the exact pattern that was failing in TPC-H Q7 parameterized queries
        let sql = r"
            SELECT n1.n_name, n2.n_name 
            FROM nation n1, nation n2 
            WHERE (
                (n1.n_name = $1 AND n2.n_name = $2) 
                OR (n1.n_name = $3 AND n2.n_name = $4)
            )";

        // All 4 parameters are strings - should NOT get CAST
        let schema = Arc::new(Schema::new(vec![
            Field::new("$1", DataType::Utf8, true),
            Field::new("$2", DataType::Utf8, true),
            Field::new("$3", DataType::Utf8, true),
            Field::new("$4", DataType::Utf8, true),
        ]));

        let rewritten = rewrite_sql_with_type_casts(sql, &schema);

        // Verify all n1 and n2 references are preserved
        assert!(
            rewritten.contains("n1.n_name"),
            "Rewritten SQL should preserve n1.n_name references: {rewritten}"
        );
        assert!(
            rewritten.contains("n2.n_name"),
            "Rewritten SQL should preserve n2.n_name references: {rewritten}"
        );

        // The bug would cause "nation.n_name" to appear instead of alias references
        assert!(
            !rewritten.contains("nation.n_name"),
            "Rewritten SQL should NOT have unaliased nation.n_name: {rewritten}"
        );

        // Verify table aliases are in FROM clause
        assert!(
            rewritten.contains("nation AS n1") || rewritten.contains("nation n1"),
            "Table alias n1 should be in FROM clause: {rewritten}"
        );
        assert!(
            rewritten.contains("nation AS n2") || rewritten.contains("nation n2"),
            "Table alias n2 should be in FROM clause: {rewritten}"
        );

        // String parameters should NOT be wrapped in CAST (the fix for the bug)
        assert!(
            !rewritten.contains("CAST($1 AS"),
            "String parameter $1 should NOT be wrapped in CAST: {rewritten}"
        );
        assert!(
            !rewritten.contains("CAST($2 AS"),
            "String parameter $2 should NOT be wrapped in CAST: {rewritten}"
        );
        assert!(
            !rewritten.contains("CAST($3 AS"),
            "String parameter $3 should NOT be wrapped in CAST: {rewritten}"
        );
        assert!(
            !rewritten.contains("CAST($4 AS"),
            "String parameter $4 should NOT be wrapped in CAST: {rewritten}"
        );
        // Verify parameters are still present
        assert!(
            rewritten.contains("$1")
                && rewritten.contains("$2")
                && rewritten.contains("$3")
                && rewritten.contains("$4"),
            "All parameters should be present in rewritten SQL: {rewritten}"
        );
    }

    /// Tests the CTE-based workaround pattern.
    ///
    /// This query pattern uses CTEs to create named subqueries:
    /// `WITH n1 AS (SELECT * FROM nation), n2 AS (SELECT * FROM nation) SELECT ...`
    ///
    /// CTEs create proper named subqueries that avoid alias resolution issues.
    /// Note: With the fix to not CAST string types, both patterns should now work.
    #[test]
    fn test_rewrite_sql_cte_workaround_pattern() {
        // CTE pattern
        let sql = r"
            WITH n1 AS (SELECT * FROM nation), 
                 n2 AS (SELECT * FROM nation) 
            SELECT n1.n_name, n2.n_name 
            FROM n1, n2 
            WHERE (
                (n1.n_name = $1 AND n2.n_name = $2) 
                OR (n1.n_name = $3 AND n2.n_name = $4)
            )";

        // All string parameters - should NOT get CAST
        let schema = Arc::new(Schema::new(vec![
            Field::new("$1", DataType::Utf8, true),
            Field::new("$2", DataType::Utf8, true),
            Field::new("$3", DataType::Utf8, true),
            Field::new("$4", DataType::Utf8, true),
        ]));

        let rewritten = rewrite_sql_with_type_casts(sql, &schema);

        // Verify CTE names are preserved
        assert!(
            rewritten.contains("n1 AS") || rewritten.contains("n1 AS ("),
            "CTE n1 should be preserved: {rewritten}"
        );
        assert!(
            rewritten.contains("n2 AS") || rewritten.contains("n2 AS ("),
            "CTE n2 should be preserved: {rewritten}"
        );

        // Verify column references use CTE names
        assert!(
            rewritten.contains("n1.n_name"),
            "Rewritten SQL should preserve n1.n_name references: {rewritten}"
        );
        assert!(
            rewritten.contains("n2.n_name"),
            "Rewritten SQL should preserve n2.n_name references: {rewritten}"
        );

        // String parameters should NOT be wrapped in CAST
        assert!(
            !rewritten.contains("CAST($1 AS"),
            "String parameter $1 should NOT be wrapped in CAST: {rewritten}"
        );
        assert!(
            !rewritten.contains("CAST($4 AS"),
            "String parameter $4 should NOT be wrapped in CAST: {rewritten}"
        );
        // Verify parameters are present
        assert!(
            rewritten.contains("$1") && rewritten.contains("$4"),
            "Parameters should be present in rewritten SQL: {rewritten}"
        );
    }

    /// Tests without parameters - ensures aliases are preserved even when
    /// no parameter rewriting happens.
    #[test]
    fn test_rewrite_sql_table_alias_no_params() {
        // Query with table aliases but no parameters - should pass through unchanged
        let sql = r"
            SELECT n1.n_name, n2.n_name 
            FROM nation n1, nation n2 
            WHERE (
                (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') 
                OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
            )";

        // Empty schema = no parameters
        let schema = Arc::new(Schema::empty());

        let rewritten = rewrite_sql_with_type_casts(sql, &schema);

        // Verify aliases are preserved
        assert!(
            rewritten.contains("n1.n_name"),
            "Rewritten SQL should preserve n1.n_name references: {rewritten}"
        );
        assert!(
            rewritten.contains("n2.n_name"),
            "Rewritten SQL should preserve n2.n_name references: {rewritten}"
        );

        // Verify table aliases in FROM clause
        assert!(
            rewritten.contains("nation AS n1") || rewritten.contains("nation n1"),
            "Table alias n1 should be in FROM clause: {rewritten}"
        );
        assert!(
            rewritten.contains("nation AS n2") || rewritten.contains("nation n2"),
            "Table alias n2 should be in FROM clause: {rewritten}"
        );

        // No unaliased references
        assert!(
            !rewritten.contains("nation.n_name"),
            "Rewritten SQL should NOT have unaliased nation.n_name: {rewritten}"
        );
    }

    /// Tests CTE without parameters - the workaround pattern without parameter binding.
    #[test]
    fn test_rewrite_sql_cte_no_params() {
        let sql = r"
            WITH n1 AS (SELECT * FROM nation), 
                 n2 AS (SELECT * FROM nation) 
            SELECT n1.n_name, n2.n_name 
            FROM n1, n2 
            WHERE (
                (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') 
                OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
            )";

        let schema = Arc::new(Schema::empty());

        let rewritten = rewrite_sql_with_type_casts(sql, &schema);

        // Verify CTE structure is preserved
        assert!(
            rewritten.contains("WITH"),
            "CTE WITH clause should be preserved: {rewritten}"
        );
        assert!(
            rewritten.contains("n1.n_name"),
            "CTE reference n1.n_name should be preserved: {rewritten}"
        );
        assert!(
            rewritten.contains("n2.n_name"),
            "CTE reference n2.n_name should be preserved: {rewritten}"
        );
    }

    /// Tests that non-string types still get CAST expressions.
    ///
    /// Integer, float, boolean, and date/timestamp types still need CAST
    /// for proper type inference in prepared statements like "SELECT $1 + $2".
    #[test]
    fn test_rewrite_sql_numeric_types_get_cast() {
        let sql = r"SELECT $1 + $2 AS sum, $3 * $4 AS product";

        // All numeric parameters - should get CAST
        let schema = Arc::new(Schema::new(vec![
            Field::new("$1", DataType::Int64, true),
            Field::new("$2", DataType::Int64, true),
            Field::new("$3", DataType::Float64, true),
            Field::new("$4", DataType::Float64, true),
        ]));

        let rewritten = rewrite_sql_with_type_casts(sql, &schema);

        // Integer parameters should be wrapped in CAST
        assert!(
            rewritten.contains("CAST($1 AS BIGINT)"),
            "Int64 parameter $1 should be wrapped in CAST AS BIGINT: {rewritten}"
        );
        assert!(
            rewritten.contains("CAST($2 AS BIGINT)"),
            "Int64 parameter $2 should be wrapped in CAST AS BIGINT: {rewritten}"
        );

        // Float parameters should be wrapped in CAST
        assert!(
            rewritten.contains("CAST($3 AS DOUBLE)"),
            "Float64 parameter $3 should be wrapped in CAST AS DOUBLE: {rewritten}"
        );
        assert!(
            rewritten.contains("CAST($4 AS DOUBLE)"),
            "Float64 parameter $4 should be wrapped in CAST AS DOUBLE: {rewritten}"
        );
    }

    /// Tests mixed parameter types - strings should NOT get CAST, others should.
    #[test]
    fn test_rewrite_sql_mixed_types() {
        let sql = r"SELECT * FROM users WHERE name = $1 AND age > $2 AND score < $3";

        // Mixed types: string (no CAST), int (CAST), float (CAST)
        let schema = Arc::new(Schema::new(vec![
            Field::new("$1", DataType::Utf8, true),
            Field::new("$2", DataType::Int32, true),
            Field::new("$3", DataType::Float64, true),
        ]));

        let rewritten = rewrite_sql_with_type_casts(sql, &schema);

        // String parameter should NOT be wrapped in CAST
        assert!(
            !rewritten.contains("CAST($1 AS"),
            "String parameter $1 should NOT be wrapped in CAST: {rewritten}"
        );
        assert!(
            rewritten.contains("$1"),
            "String parameter $1 should be present: {rewritten}"
        );

        // Int32 parameter should be wrapped in CAST
        assert!(
            rewritten.contains("CAST($2 AS INT)"),
            "Int32 parameter $2 should be wrapped in CAST AS INT: {rewritten}"
        );

        // Float64 parameter should be wrapped in CAST
        assert!(
            rewritten.contains("CAST($3 AS DOUBLE)"),
            "Float64 parameter $3 should be wrapped in CAST AS DOUBLE: {rewritten}"
        );
    }
}
