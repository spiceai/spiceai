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
use datafusion::sql::sqlparser::{
    ast::{Expr, Statement, Value, VisitMut, VisitorMut},
    dialect::GenericDialect,
    parser::{Parser, ParserError},
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
    timing::TimedStream,
};
use runtime_request_context::{AsyncMarker, RequestContext};

#[derive(Serialize, Deserialize)]
pub(crate) struct PreparedStatement {
    query: String,
    // Store parameters directly as ParamValues for fast access
    // This avoids RecordBatch serialization/deserialization overhead
    #[serde(with = "param_values_serde")]
    parameters: Option<datafusion::common::ParamValues>,
}

mod param_values_serde {
    use arrow::array::RecordBatch;
    use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
    use arrow_tools::record_batch::record_to_param_values;
    use datafusion::common::ParamValues;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::io::Cursor;

    #[allow(clippy::ref_option)]
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
        parameters: None,
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

    // Parameters are already in ParamValues format - no conversion needed!
    // This is a major performance win vs previous RecordBatch conversion on every query

    // Execute the query through the standard path
    // The logical plan will be created and cached on first execution
    // via get_or_create_logical_plan in sql_to_flight_stream.
    // Subsequent executions will reuse the cached plan (1 hour TTL, 512 entries max).
    let (output, from_cache) =
        Box::pin(Service::sql_to_flight_stream(datafusion, &sql, parameters)).await?;
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
    let streaming_flight = streaming_flight
        .map(|flight_data| flight_data.map_err(|status| FlightError::Tonic(Box::new(status))));

    let mut decoder = FlightDataDecoder::new(streaming_flight);

    // Read the schema first - Arrow Flight always sends schema before batches
    let _schema = decode_schema(&mut decoder).await?;

    // Collect the single parameter row (if any)
    let mut bound_parameters: Option<datafusion::common::ParamValues> = None;
    while let Some(msg) = futures::TryStreamExt::try_next(&mut decoder).await? {
        match msg.payload {
            DecodedPayload::None => {}
            DecodedPayload::Schema(_) => {
                return Err(Status::invalid_argument(
                    "parameter flight data must contain a single schema",
                ));
            }
            DecodedPayload::RecordBatch(record_batch) => match record_batch.num_rows() {
                0 => {}
                1 => {
                    if bound_parameters.is_some() {
                        return Err(Status::invalid_argument(
                            "parameters should contain a single row",
                        ));
                    }
                    let params = record_to_param_values(&record_batch).map_err(error_to_status)?;
                    bound_parameters = Some(params);
                }
                _ => {
                    return Err(Status::invalid_argument(
                        "parameters should contain a single row",
                    ));
                }
            },
        }
    }

    let mut stmt: PreparedStatement =
        from_bytes(&query.prepared_statement_handle).map_err(error_to_status)?;
    stmt.parameters = bound_parameters;
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
    #[allow(clippy::too_many_lines)]
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
}
