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

use std::{collections::HashMap, sync::Arc};

use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use arrow_flight::{
    FlightData, PutResult,
    flight_service_server::FlightService,
    sql::{Any, Command, CommandStatementIngest, TableExistsOption, TableNotExistOption},
    utils::flight_data_to_arrow_batch,
};
use arrow_ipc::convert::try_schema_from_flatbuffer_bytes;
use arrow_schema::{Schema, SchemaRef};
use arrow_tools::schema::verify_schema;
use datafusion::{
    error::DataFusionError, execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter, sql::TableReference,
};
use opentelemetry::KeyValue;
use prost::Message as _;
use runtime_auth::AuthRequestContext;
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::{StreamExt as _, adapters::Peekable, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};

use async_stream::stream;

use crate::{
    cluster::partition,
    config::ClusterRole,
    datafusion::{DataFusion, request_context_extension::get_current_datafusion},
    dataupdate::{StreamingDataUpdate, UpdateType},
    timing::TimedStream,
};
use runtime_request_context::{AsyncMarker, RequestContext};

use super::{
    Service, flightsql, flightsql::prepared_statement_query, metrics,
    middleware::rate_limit::RateLimiterExtension,
};

/// Options extracted from `CommandStatementIngest.table_definition_options`.
///
/// Controls auto-creation and existence-check behavior for DoPut ingestion.
struct DoPutTableOptions {
    if_not_exist: TableNotExistOption,
    if_exists: TableExistsOption,
    /// Backend-specific options from `CommandStatementIngest.options`.
    /// Used to carry `create_like_table` for staging table creation.
    ingest_options: HashMap<String, String>,
}

impl DoPutTableOptions {
    /// Extract table definition options from a `CommandStatementIngest`.
    fn from_ingest_cmd(cmd: &CommandStatementIngest) -> Self {
        let (if_not_exist, if_exists) = cmd
            .table_definition_options
            .as_ref()
            .map(|opts| {
                (
                    TableNotExistOption::try_from(opts.if_not_exist)
                        .unwrap_or(TableNotExistOption::Fail),
                    TableExistsOption::try_from(opts.if_exists)
                        .unwrap_or(TableExistsOption::Append),
                )
            })
            .unwrap_or((TableNotExistOption::Fail, TableExistsOption::Append));

        // When `create_like_table` is present, imply Create + Append
        let has_create_like = cmd.options.contains_key("create_like_table");
        let if_not_exist = if has_create_like {
            TableNotExistOption::Create
        } else {
            if_not_exist
        };
        let if_exists = if has_create_like && if_exists == TableExistsOption::Unspecified {
            TableExistsOption::Append
        } else {
            if_exists
        };

        Self {
            if_not_exist,
            if_exists,
            ingest_options: cmd.options.clone(),
        }
    }
}

pub(crate) async fn handle(
    request: Request<Streaming<FlightData>>,
) -> Result<Response<<Service as FlightService>::DoPutStream>, Status> {
    let rate_limit_check_fn = request
        .extensions()
        .get::<RateLimiterExtension>()
        .map(RateLimiterExtension::check_fn);

    let mut streaming_flight = request.into_inner().peekable();

    // We need to peek at the stream in case we branch below to prepared statements
    let Some(Ok(first_message)) = streaming_flight.peek().await else {
        let _start = metrics::track_flight_request("do_put", None);
        return Err(Status::invalid_argument("No flight data provided"));
    };
    let Some(fd) = &first_message.flight_descriptor else {
        let _start = metrics::track_flight_request("do_put", None);
        return Err(Status::invalid_argument("No flight descriptor provided"));
    };

    // Extract table path and table options from FlightSQL commands if present
    let (table_path_override, table_options) = if let Ok(message) = Any::decode(&*fd.cmd) {
        match Command::try_from(message).map_err(|e| Status::internal(format!("{e:?}")))? {
            Command::CommandPreparedStatementQuery(query) => {
                return prepared_statement_query::do_put_query(query, streaming_flight).await;
            }
            Command::CommandPreparedStatementUpdate(query) => {
                return flightsql::prepared_statement_update::do_put_update(
                    query,
                    streaming_flight,
                )
                .await;
            }
            Command::CommandStatementUpdate(cmd) => {
                return flightsql::statement_update::do_put(cmd).await;
            }
            Command::CommandStatementIngest(ingest_cmd) => {
                // Handle FlightSQL bulk ingestion command
                // Prefer descriptor path when command is under-qualified (table only).
                // This preserves fully-qualified paths forwarded by the scheduler.
                let path = match (ingest_cmd.catalog.as_ref(), ingest_cmd.schema.as_ref()) {
                    (Some(catalog), Some(schema)) => Some(vec![
                        catalog.clone(),
                        schema.clone(),
                        ingest_cmd.table.clone(),
                    ]),
                    // If command is under-qualified, prefer descriptor path if present,
                    // because scheduler forwarding includes fully-qualified path parts.
                    (Some(catalog), None) => {
                        if fd.path.is_empty() {
                            Some(vec![catalog.clone(), ingest_cmd.table.clone()])
                        } else {
                            None
                        }
                    }
                    (None, Some(schema)) => {
                        if fd.path.is_empty() {
                            Some(vec![schema.clone(), ingest_cmd.table.clone()])
                        } else {
                            None
                        }
                    }
                    (None, None) => {
                        if fd.path.is_empty() {
                            Some(vec![ingest_cmd.table.clone()])
                        } else {
                            None
                        }
                    }
                };
                let opts = Some(DoPutTableOptions::from_ingest_cmd(&ingest_cmd));
                (path, opts)
            }
            _ => (None, None),
        }
    } else {
        (None, None)
    };

    // Check if the request should be rate limited.
    if let Some(rate_limit_check) = rate_limit_check_fn {
        rate_limit_check()?;
    }

    let context = RequestContext::current(AsyncMarker::new().await);
    let datafusion = get_current_datafusion(&context);

    match context.auth_principal() {
        Some(principal) => {
            if !principal
                .groups()
                .iter()
                .any(|group| *group == "write" || *group == "read_write")
            {
                return Err(Status::permission_denied(
                    "Write access denied. Verify that authentication key used has write access and try again.",
                ));
            }
        }
        None => {
            if allow_scheduler_trusted_executor_write(&datafusion) {
                tracing::debug!(
                    "Allowing unauthenticated DoPut on executor in mTLS scheduler-trusted mode"
                );
            } else {
                return Err(Status::unauthenticated(
                    "Flight DoPut requires authentication.\nFor auth details, visit https://spiceai.org/docs/api/auth",
                ));
            }
        }
    }

    // Since it is not a prepared statement we can take from the stream
    let Some(Ok(first_message)) = streaming_flight.next().await else {
        let _start = metrics::track_flight_request("do_put", None);
        return Err(Status::invalid_argument("No flight data provided"));
    };
    let Some(fd) = &first_message.flight_descriptor else {
        let _start = metrics::track_flight_request("do_put", None);
        return Err(Status::invalid_argument("No flight descriptor provided"));
    };

    // Use table path from FlightSQL command if available, otherwise use descriptor path
    let path_vec = table_path_override.as_ref().unwrap_or(&fd.path);

    if path_vec.is_empty() {
        let _start = metrics::track_flight_request("do_put", None);
        return Err(Status::invalid_argument("No path provided"));
    }

    let path = match path_vec.len() {
        3 => TableReference::full(
            path_vec[0].as_str(),
            path_vec[1].as_str(),
            path_vec[2].as_str(),
        ),
        2 => TableReference::partial(path_vec[0].as_str(), path_vec[1].as_str()),
        _ => TableReference::parse_str(&path_vec.join(".")),
    };
    let path = normalize_path_table_reference(path, &datafusion);

    // Initializing tracking here so that both counter and duration have consistent path dimensions
    let start = metrics::track_flight_request("do_put", Some(&path.to_string())).await;

    if !datafusion.is_writable(&path) && !datafusion.is_path_catalog_writable(&path) {
        return Err(Status::invalid_argument(format!(
            "Path doesn't exist or is not writable: {path}",
        )));
    }

    // Fast path: for scheduler -> executor Cayenne writes, split by partition
    // and forward to each executor.
    if let Some(executor_registry) = datafusion.executor_registry.as_ref()
        && let Some(partition_expression) = datafusion.get_table_partition_expr(&path).await.map_err(|e| Status::internal(format!(
            "Failed to resolve partition expression for table `{path}` in distributed Cayenne write via Flight: {e}"
        )))?
        && matches!(
            datafusion.cluster_config.effective_role(),
            Some(ClusterRole::Scheduler)
        )
    {
        if executor_registry.flight_sql_clients.read().await.is_empty() {
            return Err(Status::unavailable(
                "No executors available to write data to. Ensure that at least one executor is connected to the cluster and try again.",
            ));
        }

        let response = partition::write_through::forward_federated_partitioned_write(
            executor_registry,
            Arc::clone(&datafusion.ctx),
            datafusion.io_runtime.clone(),
            &path,
            first_message,
            streaming_flight,
            &[partition_expression],
        )
        .await;

        if let Err(e) = datafusion.caching().invalidate_for_table(path.clone()) {
            tracing::warn!(
                "Failed to invalidate caches for distributed Flight DoPut table {path}: {e}"
            );
        }

        return response.map_err(Into::into);
    }

    // In distributed mode, the scheduler must NEVER write data locally.
    // Writes should always be forwarded to executors via the partitioned write path above.
    // If we reached this point on the scheduler, the table is either not partitioned
    // or partition resolution failed — reject the write to prevent silent data misrouting.
    if matches!(
        datafusion.cluster_config.effective_role(),
        Some(ClusterRole::Scheduler)
    ) {
        return Err(Status::failed_precondition(format!(
            "Cannot write data to table `{path}` on the scheduler. Ensure the table has a partition expression configured for distributed writes.",
        )));
    }

    let schema = try_schema_from_flatbuffer_bytes(&first_message.data_header)
        .map_err(|e| Status::internal(format!("Failed to get schema from data header: {e}")))?;
    let schema = Arc::new(schema);

    // Auto-create / existence-check based on table_definition_options
    if let Some(ref opts) = table_options {
        handle_table_definition_options(&datafusion, &path, &schema, opts).await?;
    }

    let target_schema = datafusion
        .get_arrow_schema(path.clone())
        .await
        .map_err(|e| Status::internal(format!("Failed to get target dataset schema: {e}")))?;

    if let Err(e) = verify_schema(target_schema.fields(), schema.fields()) {
        return Err(Status::invalid_argument(format!(
            "Schema validation error: the provided data schema does not match the expected schema for dataset `{path}`: {e}",
        )));
    }

    let first_message = first_message.clone();
    let response_stream = create_response_stream(
        path,
        schema,
        Arc::clone(&datafusion),
        streaming_flight,
        &first_message,
    );
    let response_stream = context.scope_stream(response_stream);

    let timed_stream = TimedStream::new(response_stream, move || start);

    Ok(Response::new(Box::pin(timed_stream)))
}

fn allow_scheduler_trusted_executor_write(datafusion: &DataFusion) -> bool {
    datafusion.cluster_config.effective_role() == Some(ClusterRole::Executor)
        && datafusion.cluster_config.tls_config().is_some()
}

fn normalize_path_table_reference(path: TableReference, datafusion: &DataFusion) -> TableReference {
    // NOTE: this uses synchronous `table_exist` checks on schema providers. These
    // checks are expected to be in-memory lookups in current catalog implementations.
    match path {
        TableReference::Full { .. } => path,
        TableReference::Partial { schema, table } => {
            let matching_catalogs = datafusion
                .ctx
                .catalog_names()
                .into_iter()
                .filter(|catalog_name| {
                    datafusion
                        .ctx
                        .catalog(catalog_name)
                        .and_then(|catalog| catalog.schema(schema.as_ref()))
                        .is_some_and(|schema_provider| {
                            schema_provider.table_exist(table.as_ref())
                                || datafusion.is_catalog_writable(catalog_name)
                        })
                })
                .collect::<Vec<_>>();

            if matching_catalogs.len() == 1 {
                return TableReference::full(
                    matching_catalogs[0].clone(),
                    schema.to_string(),
                    table.to_string(),
                );
            }

            TableReference::partial(schema, table)
        }
        TableReference::Bare { table } => {
            let table_name = table.to_string();
            let matching_tables = datafusion
                .ctx
                .catalog_names()
                .into_iter()
                .flat_map(|catalog_name| {
                    let table_name_for_catalog = table_name.clone();
                    datafusion
                        .ctx
                        .catalog(&catalog_name)
                        .into_iter()
                        .flat_map(move |catalog| {
                            let catalog_name = catalog_name.clone();
                            let table_name = table_name_for_catalog.clone();
                            catalog
                                .schema_names()
                                .into_iter()
                                .filter_map(move |schema_name| {
                                    let table_name = table_name.clone();
                                    catalog
                                        .schema(&schema_name)
                                        .filter(|schema_provider| {
                                            schema_provider.table_exist(table_name.as_str())
                                        })
                                        .map(|_| (catalog_name.clone(), schema_name, table_name))
                                })
                        })
                })
                .collect::<Vec<_>>();

            if matching_tables.len() == 1 {
                let (catalog, schema, table_name) = matching_tables[0].clone();
                return TableReference::full(catalog, schema, table_name);
            }

            TableReference::bare(table_name)
        }
    }
}

fn create_response_stream(
    path: TableReference,
    schema: SchemaRef,
    df: Arc<DataFusion>,
    mut streaming_flight: Peekable<Streaming<FlightData>>,
    first_message: &FlightData,
) -> impl futures::Stream<Item = Result<PutResult, Status>> + use<> {
    let dictionaries_by_id = Arc::new(HashMap::new());
    tracing::debug!("Starting writing data into dataset: {path}");

    // Sometimes the first message only contains the schema and no data
    let first_batch = arrow_flight::utils::flight_data_to_arrow_batch(
        first_message,
        Arc::clone(&schema),
        &dictionaries_by_id,
    )
    .ok();

    stream! {
        // channel to propagate new record batches to the data writing stream
        let (batch_tx, batch_rx)= mpsc::channel::<Result<RecordBatch, DataFusionError>>(100);

        let write_stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&schema), Box::new(ReceiverStream::new(batch_rx))));
        let streaming_update = StreamingDataUpdate::new(write_stream, UpdateType::Append);
        let path = path.clone();
        let mut write_future = Box::pin(df.write_streaming_data(&path, streaming_update));

        if let Some(first_batch) = first_batch {
            yield handle_record_batch(first_batch, &batch_tx, &path.to_string()).await;
        }

        // Use a single pinned Sleep future that is reset on each received message,
        // rather than creating a new timer allocation on every loop iteration.
        let idle_timeout = crate::flight::do_put_idle_timeout();
        let deadline = tokio::time::sleep(idle_timeout);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                () = &mut deadline => {
                    tracing::error!(
                        dataset = %path,
                        "Timeout: no record batch received within {} seconds",
                        idle_timeout.as_secs()
                    );
                    yield Err(Status::deadline_exceeded(format!(
                        "Timeout: no record batch received within {} seconds",
                        idle_timeout.as_secs()
                    )));
                    break;
                }
                // Poll the writing task to check if it has completed with an error while processing the data
                write_result = &mut write_future => {
                    match write_result {
                        Ok(()) => {
                            // The write operation completed before the flight stream
                            // ended. This can happen when the data sink does not
                            // consume the input stream or finishes early. Drain
                            // remaining messages and report success.
                            tracing::warn!("Write operation completed before stream ended for dataset: {path}");
                            while let Some(msg) = streaming_flight.next().await {
                                if let Err(e) = msg {
                                    tracing::error!("Error reading remaining message after early write completion: {e}");
                                }
                            }
                            yield Ok(PutResult::default());
                            break;
                        }
                        Err(e) => {
                            tracing::error!("Write operation failed. Details included in the response.");
                            yield Err(Status::internal(format!("Write operation failed: {e}")));
                            break;
                        }
                    }
                },
                message = streaming_flight.next() => {
                    match message {
                        Some(Ok(message)) => {
                            // Reset the idle timeout on each received message
                            deadline.as_mut().reset(tokio::time::Instant::now() + idle_timeout);

                            // Skip keepalive messages — these are heartbeats from
                            // write-through forwarding to prevent the idle timeout.
                            if message.app_metadata.as_ref() == crate::flight::KEEPALIVE_APP_METADATA {
                                continue;
                            }

                            let new_batch = match flight_data_to_arrow_batch(
                                &message,
                                Arc::clone(&schema),
                                &dictionaries_by_id,
                            ) {
                                Ok(batches) => batches,
                                Err(e) => {
                                    tracing::error!("Failed to convert flight data to batches: {e}");
                                    yield Err(Status::internal(format!("Failed to convert flight data to batches: {e}")));
                                    break;
                                }
                            };

                            // Only report errors; a success message is sent as the final step upon successful write completion
                            if let Err(err) = handle_record_batch(new_batch, &batch_tx, &path.to_string()).await {
                                yield Err(err);
                                break;
                            }
                        }
                        None => {
                            // End of the stream; signal that stream is completed and data write should be finalized
                            drop(batch_tx);
                            tracing::trace!("No more messages in the stream, finalizing write operation for path: {path}");

                            // Wait for the write operation to complete
                            if let Err(e) = write_future.await {
                                tracing::error!("Write operation failed. Details included in the response.");
                                yield Err(Status::internal(format!("Write operation failed: {e}")));
                            }
                            tracing::debug!("Write operation completed successfully for dataset: {path}");
                            yield Ok(PutResult::default())
                            break;
                        }
                        Some(Err(e)) => {
                            tracing::error!("Error reading message: {e}");
                            yield Err(Status::internal(format!("Error reading message: {e}")));
                            break;
                        }
                    }
                }
            }
        };

        tracing::debug!("Finished writing data into dataset: {path}");
    }
}

/// Handle `table_definition_options` semantics from `CommandStatementIngest`.
///
/// - `if_not_exist == Create`: auto-create the table from the incoming schema
///   (or from `create_like_table` if specified) when it doesn't exist and the
///   catalog allows DDL.
/// - `if_exists == Fail`: error if the table already exists.
/// - `if_exists == Replace`: drop and recreate the table.
/// - `if_exists == Append` (default): no action, fall through to normal write.
async fn handle_table_definition_options(
    datafusion: &DataFusion,
    path: &TableReference,
    incoming_schema: &Schema,
    opts: &DoPutTableOptions,
) -> Result<(), Status> {
    let table_exists = datafusion.table_exists(path.clone());

    if table_exists {
        match opts.if_exists {
            TableExistsOption::Fail => {
                return Err(Status::already_exists(format!(
                    "Table `{path}` already exists and if_exists=Fail was specified",
                )));
            }
            TableExistsOption::Replace => {
                // Verify catalog allows DDL before dropping
                let catalog_name = path
                    .catalog()
                    .unwrap_or(crate::datafusion::SPICE_DEFAULT_CATALOG);
                if !datafusion.is_catalog_ddl_enabled(catalog_name) {
                    return Err(Status::permission_denied(format!(
                        "Cannot replace table `{path}`: catalog `{catalog_name}` does not allow DDL operations. \
                         Set `access: read_write_create` on the catalog to enable DDL.",
                    )));
                }
                let drop_sql = format!("DROP TABLE {path}");
                tracing::info!("DoPut: dropping existing table `{path}` for if_exists=Replace");
                datafusion
                    .ctx
                    .sql(&drop_sql)
                    .await
                    .map_err(|e| Status::internal(format!("Failed to drop table `{path}`: {e}")))?
                    .collect()
                    .await
                    .map_err(|e| Status::internal(format!("Failed to drop table `{path}`: {e}")))?;

                // Fall through to auto-create below
                auto_create_table(datafusion, path, incoming_schema, opts).await?;
            }
            // Append or Unspecified: no action, table exists, proceed to normal write
            _ => {}
        }
    } else {
        // Table doesn't exist
        match opts.if_not_exist {
            TableNotExistOption::Create => {
                auto_create_table(datafusion, path, incoming_schema, opts).await?;
            }
            TableNotExistOption::Fail | TableNotExistOption::Unspecified => {
                return Err(Status::not_found(format!(
                    "Table `{path}` does not exist and if_not_exist=Create was not specified",
                )));
            }
        }
    }

    Ok(())
}

/// Auto-create a table via DDL.
///
/// If `create_like_table` is present in `opts.ingest_options`, copies the referenced
/// table's schema and, when available, its partition expression.
/// Otherwise, creates from the incoming data schema.
async fn auto_create_table(
    datafusion: &DataFusion,
    path: &TableReference,
    incoming_schema: &Schema,
    opts: &DoPutTableOptions,
) -> Result<(), Status> {
    let catalog_name = path
        .catalog()
        .unwrap_or(crate::datafusion::SPICE_DEFAULT_CATALOG);

    // Gate: catalog must allow DDL (read_write_create)
    if !datafusion.is_catalog_ddl_enabled(catalog_name) {
        return Err(Status::permission_denied(format!(
            "Cannot auto-create table `{path}`: catalog `{catalog_name}` does not allow DDL operations. \
             Set `access: read_write_create` on the catalog to enable DDL.",
        )));
    }

    // Gate: catalog must be Cayenne-backed
    if !datafusion.is_cayenne_catalog(path) {
        return Err(Status::unimplemented(format!(
            "Cannot auto-create table `{path}`: auto-creation is only supported for Cayenne-backed catalogs",
        )));
    }

    let create_sql = if let Some(like_table) = opts.ingest_options.get("create_like_table") {
        build_create_like_table_sql(datafusion, path, like_table).await?
    } else {
        arrow_schema_to_create_table_sql(path, incoming_schema)
    };

    tracing::info!("DoPut: auto-creating table `{path}` via DDL: {create_sql}");
    datafusion
        .ctx
        .sql(&create_sql)
        .await
        .map_err(|e| Status::internal(format!("Failed to auto-create table `{path}`: {e}")))?
        .collect()
        .await
        .map_err(|e| Status::internal(format!("Failed to auto-create table `{path}`: {e}")))?;

    Ok(())
}

/// Build a `CREATE TABLE` SQL statement by copying the definition of an existing table.
///
/// Copies schema and partition expression (when available) from the referenced source table.
async fn build_create_like_table_sql(
    datafusion: &DataFusion,
    target_path: &TableReference,
    source_table_name: &str,
) -> Result<String, Status> {
    let source_ref = TableReference::parse_str(source_table_name);

    // Get the source table's schema
    let source_schema = datafusion
        .get_arrow_schema(source_ref.clone())
        .await
        .map_err(|e| {
            Status::not_found(format!(
                "Cannot create table like `{source_table_name}`: failed to get source table schema: {e}"
            ))
        })?;

    let mut sql = arrow_schema_to_create_table_sql(target_path, &source_schema);

    // Copy partition expression from source table if it has one.
    // Propagate errors — silently omitting a partition expression would cause
    // hard-to-diagnose failures in distributed MERGE routing.
    match datafusion.get_table_partition_expr(&source_ref).await {
        Ok(Some(partition_sql)) => {
            sql = format!("{sql} PARTITION BY {partition_sql}");
        }
        Ok(None) => { /* source table has no partition expression — OK */ }
        Err(e) => {
            return Err(Status::internal(format!(
                "Cannot create table like `{source_table_name}`: \
                 failed to resolve partition expression: {e}"
            )));
        }
    }

    Ok(sql)
}

/// Convert an Arrow schema to a `CREATE TABLE IF NOT EXISTS` SQL statement.
fn arrow_schema_to_create_table_sql(path: &TableReference, schema: &Schema) -> String {
    let columns: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| {
            let col_name = quote_identifier(field.name());
            let col_type = arrow_type_to_sql(field.data_type());
            let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
            format!("{col_name} {col_type}{nullable}")
        })
        .collect();
    let quoted_path = path.to_quoted_string();
    format!(
        "CREATE TABLE IF NOT EXISTS {quoted_path} ({})",
        columns.join(", ")
    )
}

/// Map Arrow data types to SQL type names for CREATE TABLE DDL.
fn arrow_type_to_sql(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 | DataType::UInt8 => "TINYINT".to_string(),
        DataType::Int16 | DataType::UInt16 => "SMALLINT".to_string(),
        DataType::Int32 | DataType::UInt32 => "INTEGER".to_string(),
        DataType::Int64 | DataType::UInt64 => "BIGINT".to_string(),
        DataType::Float16 => "REAL".to_string(),
        DataType::Float32 => "REAL".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
            format!("DECIMAL({p},{s})")
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "TEXT".to_string(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => "BYTEA".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMPTZ".to_string(),
        DataType::Timestamp(_, None) => "TIMESTAMP".to_string(),
        DataType::Time32(_) | DataType::Time64(_) => "TIME".to_string(),
        DataType::Duration(_) => "BIGINT".to_string(),
        DataType::Interval(_) => "INTERVAL".to_string(),
        DataType::FixedSizeBinary(_) => "BYTEA".to_string(), // no fixed-size binary in SQL; use BYTEA
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            let inner = arrow_type_to_sql(field.data_type());
            format!("{inner}[]")
        }
        // Fallback: use TEXT for unsupported types
        _ => "TEXT".to_string(),
    }
}

/// Quote a SQL identifier if it contains special characters or is a reserved word.
fn quote_identifier(name: &str) -> String {
    // Always quote to be safe — avoids issues with reserved words and special chars
    format!("\"{}\"", name.replace('"', "\"\""))
}

async fn handle_record_batch(
    batch: RecordBatch,
    batch_tx: &Sender<Result<RecordBatch, DataFusionError>>,
    path: &str,
) -> Result<PutResult, Status> {
    tracing::trace!("Received batch with {} rows", batch.num_rows());

    let labels = [KeyValue::new("dataset", path.to_string())];
    metrics::DO_PUT_ROWS_WRITTEN.add(batch.num_rows() as u64, &labels);
    metrics::DO_PUT_BYTES_WRITTEN.add(batch.get_array_memory_size() as u64, &labels);

    if let Err(e) = batch_tx.send(Ok(batch)).await {
        tracing::error!("Error sending record batch to write channel: {e}");
        return Err(Status::internal(format!(
            "Error sending record batch to write channel: {e}"
        )));
    }
    Ok(PutResult::default())
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow_flight::sql::{
        CommandStatementIngest, TableDefinitionOptions, TableExistsOption, TableNotExistOption,
    };
    use datafusion::sql::TableReference;
    use std::collections::HashMap;

    use super::*;

    // ── DoPutTableOptions extraction tests ──

    #[test]
    fn test_from_ingest_cmd_with_explicit_options() {
        let cmd = CommandStatementIngest {
            table_definition_options: Some(TableDefinitionOptions {
                if_not_exist: TableNotExistOption::Create as i32,
                if_exists: TableExistsOption::Append as i32,
            }),
            table: "test_table".to_string(),
            schema: None,
            catalog: None,
            temporary: false,
            transaction_id: None,
            options: HashMap::new(),
        };
        let opts = DoPutTableOptions::from_ingest_cmd(&cmd);
        assert_eq!(opts.if_not_exist, TableNotExistOption::Create);
        assert_eq!(opts.if_exists, TableExistsOption::Append);
        assert!(opts.ingest_options.is_empty());
    }

    #[test]
    fn test_from_ingest_cmd_defaults_when_no_options() {
        let cmd = CommandStatementIngest {
            table_definition_options: None,
            table: "test_table".to_string(),
            schema: None,
            catalog: None,
            temporary: false,
            transaction_id: None,
            options: HashMap::new(),
        };
        let opts = DoPutTableOptions::from_ingest_cmd(&cmd);
        assert_eq!(opts.if_not_exist, TableNotExistOption::Fail);
        assert_eq!(opts.if_exists, TableExistsOption::Append);
    }

    #[test]
    fn test_from_ingest_cmd_create_like_implies_create_append() {
        let mut options = HashMap::new();
        options.insert(
            "create_like_table".to_string(),
            "my_catalog.my_schema.source_table".to_string(),
        );
        let cmd = CommandStatementIngest {
            table_definition_options: None, // No explicit options
            table: "staging_table".to_string(),
            schema: None,
            catalog: None,
            temporary: false,
            transaction_id: None,
            options,
        };
        let opts = DoPutTableOptions::from_ingest_cmd(&cmd);
        // create_like_table implies Create + Append
        assert_eq!(opts.if_not_exist, TableNotExistOption::Create);
        assert_eq!(opts.if_exists, TableExistsOption::Append);
        assert_eq!(
            opts.ingest_options.get("create_like_table"),
            Some(&"my_catalog.my_schema.source_table".to_string())
        );
    }

    #[test]
    fn test_from_ingest_cmd_create_like_preserves_explicit_if_exists() {
        let mut options = HashMap::new();
        options.insert("create_like_table".to_string(), "source".to_string());
        let cmd = CommandStatementIngest {
            table_definition_options: Some(TableDefinitionOptions {
                if_not_exist: TableNotExistOption::Fail as i32, // will be overridden
                if_exists: TableExistsOption::Fail as i32,      // explicit, should be preserved
            }),
            table: "staging".to_string(),
            schema: None,
            catalog: None,
            temporary: false,
            transaction_id: None,
            options,
        };
        let opts = DoPutTableOptions::from_ingest_cmd(&cmd);
        assert_eq!(opts.if_not_exist, TableNotExistOption::Create); // overridden by create_like
        assert_eq!(opts.if_exists, TableExistsOption::Fail); // preserved
    }

    // ── Arrow schema to SQL conversion tests ──

    #[test]
    fn test_arrow_schema_to_create_table_basic() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]);
        let path = TableReference::full("catalog", "schema", "my_table");
        let sql = arrow_schema_to_create_table_sql(&path, &schema);
        assert_eq!(
            sql,
            r#"CREATE TABLE IF NOT EXISTS catalog.schema.my_table ("id" BIGINT NOT NULL, "name" TEXT, "score" DOUBLE)"#
        );
    }

    #[test]
    fn test_arrow_schema_to_create_table_all_types() {
        let schema = Schema::new(vec![
            Field::new("col_bool", DataType::Boolean, true),
            Field::new("col_i8", DataType::Int8, true),
            Field::new("col_i16", DataType::Int16, true),
            Field::new("col_i32", DataType::Int32, true),
            Field::new("col_i64", DataType::Int64, true),
            Field::new("col_f32", DataType::Float32, true),
            Field::new("col_f64", DataType::Float64, true),
            Field::new("col_decimal", DataType::Decimal128(18, 4), true),
            Field::new("col_text", DataType::Utf8, true),
            Field::new("col_binary", DataType::Binary, true),
            Field::new("col_date", DataType::Date32, true),
            Field::new(
                "col_ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "col_tstz",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]);
        let path = TableReference::bare("test");
        let sql = arrow_schema_to_create_table_sql(&path, &schema);
        assert!(sql.contains("\"col_bool\" BOOLEAN"));
        assert!(sql.contains("\"col_i8\" TINYINT"));
        assert!(sql.contains("\"col_i16\" SMALLINT"));
        assert!(sql.contains("\"col_i32\" INTEGER"));
        assert!(sql.contains("\"col_i64\" BIGINT"));
        assert!(sql.contains("\"col_f32\" REAL"));
        assert!(sql.contains("\"col_f64\" DOUBLE"));
        assert!(sql.contains("\"col_decimal\" DECIMAL(18,4)"));
        assert!(sql.contains("\"col_text\" TEXT"));
        assert!(sql.contains("\"col_binary\" BYTEA"));
        assert!(sql.contains("\"col_date\" DATE"));
        assert!(sql.contains("\"col_ts\" TIMESTAMP"));
        assert!(sql.contains("\"col_tstz\" TIMESTAMPTZ"));
    }

    #[test]
    fn test_arrow_schema_to_create_table_not_null() {
        let schema = Schema::new(vec![
            Field::new("required", DataType::Int32, false),
            Field::new("optional", DataType::Int32, true),
        ]);
        let path = TableReference::bare("t");
        let sql = arrow_schema_to_create_table_sql(&path, &schema);
        assert!(sql.contains("\"required\" INTEGER NOT NULL"));
        assert!(sql.contains("\"optional\" INTEGER"));
        // "optional" should NOT contain "NOT NULL"
        assert!(!sql.contains("\"optional\" INTEGER NOT NULL"));
    }

    #[test]
    fn test_arrow_schema_to_create_table_list_types() {
        let schema = Schema::new(vec![Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        )]);
        let path = TableReference::bare("t");
        let sql = arrow_schema_to_create_table_sql(&path, &schema);
        assert!(sql.contains("\"tags\" TEXT[]"));
    }

    // ── arrow_type_to_sql tests ──

    #[test]
    fn test_arrow_type_to_sql_utf8_variants() {
        assert_eq!(arrow_type_to_sql(&DataType::Utf8), "TEXT");
        assert_eq!(arrow_type_to_sql(&DataType::LargeUtf8), "TEXT");
        assert_eq!(arrow_type_to_sql(&DataType::Utf8View), "TEXT");
    }

    #[test]
    fn test_arrow_type_to_sql_decimal() {
        assert_eq!(
            arrow_type_to_sql(&DataType::Decimal128(10, 2)),
            "DECIMAL(10,2)"
        );
        assert_eq!(
            arrow_type_to_sql(&DataType::Decimal256(38, 0)),
            "DECIMAL(38,0)"
        );
    }

    #[test]
    fn test_arrow_type_to_sql_timestamp_tz() {
        assert_eq!(
            arrow_type_to_sql(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            "TIMESTAMP"
        );
        assert_eq!(
            arrow_type_to_sql(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            )),
            "TIMESTAMPTZ"
        );
    }

    // ── quote_identifier tests ──

    #[test]
    fn test_quote_identifier_simple() {
        assert_eq!(quote_identifier("name"), r#""name""#);
    }

    #[test]
    fn test_quote_identifier_with_quotes() {
        assert_eq!(quote_identifier(r#"has"quote"#), r#""has""quote""#);
    }

    #[test]
    fn test_quote_identifier_reserved_word() {
        assert_eq!(quote_identifier("select"), r#""select""#);
    }
}
