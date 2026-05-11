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

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::compute::cast;
use arrow::datatypes::{Schema, SchemaRef};
use arrow_flight::{
    FlightData, PutResult,
    decode::{DecodedPayload, FlightDataDecoder},
    error::FlightError,
    flight_service_server::FlightService,
    sql::{Any, Command},
};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use prost::Message;
use tokio_stream::adapters::Peekable;
use tonic::{Request, Response, Status, Streaming};

use crate::{FlightSqlService, flightsql, handle_datafusion_error, to_tonic_err};

pub(crate) async fn handle(
    ctx: Arc<SessionContext>,
    request: Request<Streaming<FlightData>>,
) -> Result<Response<<FlightSqlService as FlightService>::DoPutStream>, Status> {
    let mut streaming_flight: Peekable<Streaming<FlightData>> =
        tokio_stream::StreamExt::peekable(request.into_inner());

    let Some(Ok(first_message)) = streaming_flight.peek().await else {
        return Err(Status::invalid_argument("no flight data provided"));
    };
    let Some(fd) = first_message.flight_descriptor.clone() else {
        return Err(Status::invalid_argument("no flight descriptor provided"));
    };
    let cmd = fd.cmd.clone();
    let descriptor_path = fd.path.clone();

    let Ok(message) = Any::decode(&*cmd) else {
        return do_put_raw(ctx, streaming_flight, None).await;
    };

    match Command::try_from(message).map_err(|e| Status::internal(format!("{e:?}")))? {
        Command::CommandPreparedStatementQuery(cmd) => {
            flightsql::prepared_statement_query::do_put_query(cmd, streaming_flight).await
        }
        Command::CommandPreparedStatementUpdate(cmd) => {
            flightsql::prepared_statement_update::do_put_update(cmd, streaming_flight).await
        }
        Command::CommandStatementUpdate(cmd) => flightsql::statement_update::do_put(ctx, cmd).await,
        Command::CommandStatementIngest(cmd) => {
            let path_override = ingest_command_path_override(&cmd, &descriptor_path);
            do_put_raw(ctx, streaming_flight, path_override).await
        }
        _ => do_put_raw(ctx, streaming_flight, None).await,
    }
}

fn ingest_command_path_override(
    ingest_cmd: &arrow_flight::sql::CommandStatementIngest,
    descriptor_path: &[String],
) -> Option<Vec<String>> {
    match (ingest_cmd.catalog.as_ref(), ingest_cmd.schema.as_ref()) {
        (Some(catalog), Some(schema)) => Some(vec![
            catalog.clone(),
            schema.clone(),
            ingest_cmd.table.clone(),
        ]),
        // If command is under-qualified, prefer descriptor path when present.
        (Some(catalog), None) => {
            if descriptor_path.is_empty() {
                Some(vec![catalog.clone(), ingest_cmd.table.clone()])
            } else {
                None
            }
        }
        (None, Some(schema)) => {
            if descriptor_path.is_empty() {
                Some(vec![schema.clone(), ingest_cmd.table.clone()])
            } else {
                None
            }
        }
        (None, None) => {
            if descriptor_path.is_empty() {
                Some(vec![ingest_cmd.table.clone()])
            } else {
                None
            }
        }
    }
}

fn resolve_table_path(path: &[String]) -> datafusion::sql::TableReference {
    match path.len() {
        3 => datafusion::sql::TableReference::full(
            path[0].as_str(),
            path[1].as_str(),
            path[2].as_str(),
        ),
        2 => datafusion::sql::TableReference::partial(path[0].as_str(), path[1].as_str()),
        _ => datafusion::sql::TableReference::parse_str(&path.join(".")),
    }
}

async fn decode_flight_batches(
    streaming: Peekable<Streaming<FlightData>>,
) -> Result<
    (
        arrow_schema::SchemaRef,
        Vec<arrow::record_batch::RecordBatch>,
    ),
    Status,
> {
    let streaming = tokio_stream::StreamExt::map(streaming, |r| {
        r.map_err(|s| FlightError::Tonic(Box::new(s)))
    });
    let mut decoder = FlightDataDecoder::new(streaming);

    let mut schema: Option<arrow_schema::SchemaRef> = None;
    let mut batches = Vec::new();

    while let Some(msg) = futures::TryStreamExt::try_next(&mut decoder)
        .await
        .map_err(to_tonic_err)?
    {
        match msg.payload {
            DecodedPayload::None => {}
            DecodedPayload::Schema(decoded_schema) => {
                if schema.is_some() {
                    return Err(Status::invalid_argument(
                        "DoPut stream must contain a single schema message",
                    ));
                }
                schema = Some(decoded_schema);
            }
            DecodedPayload::RecordBatch(batch) => {
                if schema.is_none() {
                    return Err(Status::invalid_argument(
                        "DoPut stream must include schema before record batches",
                    ));
                }
                batches.push(batch);
            }
        }
    }

    let schema = schema.ok_or_else(|| {
        Status::invalid_argument("DoPut stream must include at least one schema message")
    })?;

    Ok((schema, batches))
}

/// Cast columns in `batches` to match `target_schema` where the types differ
/// but are compatible (e.g. `Timestamp(µs)` → `Timestamp(ns)`).
fn coerce_batches_to_schema(
    src_schema: SchemaRef,
    batches: Vec<RecordBatch>,
    target_schema: &Schema,
) -> DFResult<(SchemaRef, Vec<RecordBatch>)> {
    if src_schema.fields().len() == target_schema.fields().len()
        && src_schema
            .fields()
            .iter()
            .zip(target_schema.fields().iter())
            .all(|(s, t)| s.data_type() == t.data_type())
    {
        return Ok((src_schema, batches));
    }

    let target_fields = target_schema.fields();
    let coerced_batches = batches
        .into_iter()
        .map(|batch| {
            if batch.num_columns() != target_fields.len() {
                return Ok(batch);
            }
            let columns = batch
                .columns()
                .iter()
                .zip(target_fields.iter())
                .map(|(col, target_field)| {
                    if col.data_type() == target_field.data_type() {
                        Ok(Arc::clone(col))
                    } else {
                        cast(col, target_field.data_type())
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
                    }
                })
                .collect::<DFResult<Vec<_>>>()?;
            RecordBatch::try_new(Arc::new(target_schema.clone()), columns)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        })
        .collect::<DFResult<Vec<_>>>()?;

    Ok((Arc::new(target_schema.clone()), coerced_batches))
}

async fn do_put_raw(
    ctx: Arc<SessionContext>,
    mut streaming: Peekable<Streaming<FlightData>>,
    path_override: Option<Vec<String>>,
) -> Result<Response<<FlightSqlService as FlightService>::DoPutStream>, Status> {
    let Some(Ok(first_message)) = streaming.peek().await else {
        return Err(Status::invalid_argument("no flight data provided"));
    };
    let Some(fd) = first_message.flight_descriptor.clone() else {
        return Err(Status::invalid_argument("no flight descriptor provided"));
    };

    let path = path_override.as_ref().unwrap_or(&fd.path);
    if path.is_empty() {
        return Err(Status::invalid_argument("no path provided"));
    }

    let table_provider = ctx
        .table_provider(resolve_table_path(path))
        .await
        .map_err(handle_datafusion_error)?;

    let (schema, batches) = decode_flight_batches(streaming).await?;

    // Cast batches to the table's schema for compatible type differences
    // (e.g. Timestamp(µs) incoming vs Timestamp(ns) in the table).
    let table_schema = table_provider.schema();
    let (schema, batches) = coerce_batches_to_schema(schema, batches, &table_schema)
        .map_err(handle_datafusion_error)?;

    let insert_plan = table_provider
        .insert_into(
            &ctx.state(),
            MemorySourceConfig::try_new_exec(&[batches], schema, None)
                .map_err(handle_datafusion_error)?,
            InsertOp::Append,
        )
        .await
        .map_err(handle_datafusion_error)?;
    collect(insert_plan, ctx.task_ctx())
        .await
        .map_err(handle_datafusion_error)?;

    Ok(Response::new(
        Box::pin(futures::stream::iter(vec![Ok(PutResult::default())]))
            as <FlightSqlService as FlightService>::DoPutStream,
    ))
}
