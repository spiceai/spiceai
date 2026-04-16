/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Prepared statement lifecycle for Flight SQL.
//!
//! Prepared statement metadata is encoded entirely inside the opaque handle
//! bytes (via `postcard`) rather than being stored server-side. This means
//! no per-session state is required beyond the `SessionContext` needed for
//! schema introspection and query execution.

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
use datafusion::common::ParamValues;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::{
    ast::{Expr, Statement, Value, VisitMut, VisitorMut},
    dialect::GenericDialect,
    parser::{Parser, ParserError},
};
use futures::StreamExt as _;
use postcard::{from_bytes, to_stdvec};
use prost::Message;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use tokio_stream::adapters::Peekable;
use tonic::{Request, Response, Status, Streaming};

use crate::{FlightSqlService, to_tonic_err};

// ── Arrow type → SQL cast name ────────────────────────────────────────────────

/// NOTE: String types (`Utf8`, `LargeUtf8`) are intentionally NOT included.
/// Adding CAST for strings prevents `DataFusion`'s filter pushdown optimisation
/// from merging filters into `TableScan` as `full_filters`.
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
        DataType::Boolean => Some("BOOLEAN"),
        DataType::Date32 | DataType::Date64 => Some("DATE"),
        DataType::Timestamp(_, _) => Some("TIMESTAMP"),
        _ => None,
    }
}

// ── AST visitor: rewrite $N placeholders to CAST($N AS <type>) ───────────────

struct ParameterCastRewriter<'a> {
    param_types: &'a std::collections::HashMap<usize, &'static str>,
}

impl VisitorMut for ParameterCastRewriter<'_> {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Value(value_with_span) = expr
            && let Value::Placeholder(ref placeholder) = value_with_span.value
            && let Some(stripped) = placeholder.strip_prefix('$')
            && let Ok(idx) = stripped.parse::<usize>()
            && let Some(sql_type) = self.param_types.get(&idx)
        {
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
        ControlFlow::Continue(())
    }
}

fn rewrite_sql_with_type_casts(sql: &str, schema: &SchemaRef) -> String {
    let mut param_types = std::collections::HashMap::new();
    for (idx, field) in schema.fields().iter().enumerate() {
        if let Some(sql_type) = arrow_type_to_sql_type(field.data_type()) {
            param_types.insert(idx + 1, sql_type);
        }
    }

    let dialect = GenericDialect {};
    let mut ast = match Parser::parse_sql(&dialect, sql) {
        Ok(ast) => ast,
        Err(e) => {
            tracing::warn!("failed to parse SQL for type-cast rewriting: {e}");
            return sql.to_string();
        }
    };

    let mut rewriter = ParameterCastRewriter {
        param_types: &param_types,
    };
    for stmt in &mut ast {
        let _ = stmt.visit(&mut rewriter);
    }

    ast.iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

// ── JDBC placeholder → Postgres placeholder conversion ───────────────────────

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Multiple statements found in query. Only one statement is supported."))]
    MultipleStatements,
    #[snafu(display("Invalid query: {query} {source}"))]
    InvalidQuery { query: String, source: ParserError },
}

fn convert_jdbc_parameter_placeholders(query: &str) -> Result<Cow<'_, str>, Error> {
    if !query.contains('?') {
        return Ok(Cow::Borrowed(query));
    }
    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, query).context(InvalidQuerySnafu { query })?;
    ensure!(statements.len() == 1, MultipleStatementsSnafu);
    let Some(mut statement) = statements.pop() else {
        unreachable!("exactly one statement confirmed above");
    };
    let mut visitor = ConvertJdbcPlaceholdersVisitor {
        next_placeholder: 1,
    };
    visitor.visit_statement(&mut statement);
    Ok(Cow::Owned(statement.to_string()))
}

struct ConvertJdbcPlaceholdersVisitor {
    next_placeholder: u32,
}

impl ConvertJdbcPlaceholdersVisitor {
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
            let new = placeholder.replace('?', &format!("${}", self.next_placeholder));
            value_with_span.value = Value::Placeholder(new);
            self.next_placeholder += 1;
        }
        ControlFlow::Continue(())
    }
}

// ── Prepared statement handle ─────────────────────────────────────────────────

/// Serialised prepared statement stored inside the opaque handle bytes.
#[derive(Serialize, Deserialize)]
pub(crate) struct PreparedStatement {
    pub(super) query: String,
    pub(super) parameters: Vec<u8>,
    /// Arrow IPC bytes of the bound parameter schema (set during `DoPut`).
    pub(super) parameter_schema: Option<Vec<u8>>,
}

// ── DoAction: CreatePreparedStatement ────────────────────────────────────────

pub(crate) async fn do_action_create_prepared_statement(
    ctx: Arc<SessionContext>,
    statement: sql::ActionCreatePreparedStatementRequest,
) -> Result<sql::ActionCreatePreparedStatementResult, Status> {
    tracing::trace!("do_action_create_prepared_statement: {:?}", statement.query);

    let query = convert_jdbc_parameter_placeholders(&statement.query)
        .map_err(|e| Status::invalid_argument(e.to_string()))?;

    // Attempt schema inference; fall back to empty schema if type inference
    // fails (e.g. for parameterised queries before parameters are bound).
    let (dataset_schema, parameter_schema) =
        match FlightSqlService::get_arrow_schema(&ctx, &query).await {
            Ok(schema) => (schema, None),
            Err(e) => {
                let msg = e.message();
                if msg.contains("Cannot get result type")
                    || msg.contains("Invalid arithmetic operation")
                    || msg.contains("type inference")
                    || msg.contains("No field named")
                {
                    tracing::debug!("schema inference deferred until execution: {msg}");
                    (arrow::datatypes::Schema::empty(), None)
                } else {
                    return Err(e);
                }
            }
        };

    let dataset_schema_bytes = FlightSqlService::serialize_schema(&dataset_schema)?;
    let parameter_schema_bytes = parameter_schema
        .as_ref()
        .map(FlightSqlService::serialize_schema)
        .transpose()?
        .unwrap_or_default();

    let stmt = PreparedStatement {
        query: query.into_owned(),
        parameters: vec![],
        parameter_schema: None,
    };
    let handle = to_stdvec(&stmt).map_err(error_to_status)?;

    Ok(sql::ActionCreatePreparedStatementResult {
        prepared_statement_handle: handle.into(),
        dataset_schema: dataset_schema_bytes,
        parameter_schema: parameter_schema_bytes,
    })
}

// ── GetFlightInfo for prepared statement query ────────────────────────────────

pub(crate) async fn get_flight_info(
    ctx: Arc<SessionContext>,
    handle: sql::CommandPreparedStatementQuery,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    let PreparedStatement { query: sql, .. } =
        from_bytes(&handle.prepared_statement_handle).map_err(error_to_status)?;

    let maybe_schema = match FlightSqlService::get_arrow_schema(&ctx, &sql).await {
        Ok(schema) => Some(schema),
        Err(e) => {
            let msg = e.message();
            if msg.contains("Cannot get result type")
                || msg.contains("Invalid arithmetic operation")
                || msg.contains("type inference")
                || msg.contains("No field named")
            {
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

    if let Some(schema) = maybe_schema {
        info = info.try_with_schema(&schema).map_err(to_tonic_err)?;
    }

    Ok(Response::new(info))
}

// ── DoGet: execute prepared statement query ───────────────────────────────────

pub(crate) async fn do_get(
    ctx: Arc<SessionContext>,
    query: sql::CommandPreparedStatementQuery,
) -> Result<Response<<FlightSqlService as FlightService>::DoGetStream>, Status> {
    tracing::trace!("do_get prepared_statement_query");

    let PreparedStatement {
        query: sql,
        parameters,
        parameter_schema,
    } = from_bytes(&query.prepared_statement_handle).map_err(error_to_status)?;

    let param_values = decode_param_values(&parameters).map_err(error_to_status)?;

    // Rewrite SQL with explicit type casts when parameter schema is available.
    let sql_to_execute = if let Some(schema_bytes) = &parameter_schema {
        let reader = StreamReader::try_new(&schema_bytes[..], None).map_err(error_to_status)?;
        let schema = reader.schema();
        let rewritten = rewrite_sql_with_type_casts(&sql, &schema);
        tracing::debug!("do_get prepared: rewritten SQL = {rewritten}");
        Cow::Owned(rewritten)
    } else {
        Cow::Borrowed(sql.as_str())
    };

    let output = FlightSqlService::sql_to_flight_stream(ctx, &sql_to_execute, param_values).await?;
    Ok(Response::new(
        Box::pin(output) as <FlightSqlService as FlightService>::DoGetStream
    ))
}

// ── DoPut: bind parameters to a prepared query ───────────────────────────────

pub(crate) async fn do_put_query(
    query: CommandPreparedStatementQuery,
    streaming_flight: Peekable<Streaming<FlightData>>,
) -> Result<Response<<FlightSqlService as FlightService>::DoPutStream>, Status> {
    tracing::debug!("do_put_query: binding parameters");

    let streaming = streaming_flight.map(|r| r.map_err(|s| FlightError::Tonic(Box::new(s))));
    let mut decoder = FlightDataDecoder::new(streaming);

    let schema = decode_schema(&mut decoder).await?;

    let mut parameters = Vec::new();
    let mut encoder = StreamWriter::try_new(&mut parameters, &schema).map_err(error_to_status)?;
    let mut total_rows = 0_usize;

    while let Some(msg) = futures::TryStreamExt::try_next(&mut decoder).await? {
        match msg.payload {
            DecodedPayload::None => {}
            DecodedPayload::Schema(_) => {
                return Err(Status::invalid_argument(
                    "parameter flight data must contain a single schema",
                ));
            }
            DecodedPayload::RecordBatch(batch) => {
                total_rows += batch.num_rows();
                encoder.write(&batch).map_err(error_to_status)?;
            }
        }
    }
    encoder.finish().map_err(error_to_status)?;

    if total_rows > 1 {
        return Err(Status::invalid_argument(
            "parameters should contain a single row",
        ));
    }

    // Persist the parameter schema for type-cast rewriting at execution time.
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

// ── Shared helpers ────────────────────────────────────────────────────────────

async fn decode_schema(decoder: &mut FlightDataDecoder) -> Result<SchemaRef, Status> {
    while let Some(msg) = futures::TryStreamExt::try_next(decoder).await? {
        match msg.payload {
            DecodedPayload::None => {}
            DecodedPayload::Schema(schema) => return Ok(schema),
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

pub(super) fn decode_param_values(
    parameters: &[u8],
) -> Result<Option<ParamValues>, datafusion::error::DataFusionError> {
    if parameters.is_empty() {
        return Ok(None);
    }
    let decoder = StreamReader::try_new(parameters, None)?;
    let schema = decoder.schema();
    let batches = decoder.into_iter().collect::<Result<Vec<_>, _>>()?;
    let batch = concat_batches(&schema, batches.iter())?;
    Ok(Some(record_to_param_values(&batch)?))
}

pub(super) fn error_to_status<E: std::fmt::Debug>(err: E) -> Status {
    Status::internal(format!("{err:?}"))
}
