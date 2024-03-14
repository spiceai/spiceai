use crate::datafusion::DataFusion;
use crate::dataupdate::{DataUpdate, UpdateType};
use arrow::datatypes::Schema;
use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::sql::{Any, Command, ProstMessageExt};
use arrow_flight::{sql, Action, ActionType, Criteria, IpcMessage, SchemaResult};
use arrow_ipc::convert::try_schema_from_flatbuffer_bytes;
use arrow_ipc::writer::{self, IpcWriteOptions};
use bytes::Bytes;
use datafusion::arrow::error::ArrowError;
use datafusion::datasource::TableType;
use datafusion::execution::SendableRecordBatchStream;
use futures::stream::{self, BoxStream, StreamExt};
use futures::{Stream, TryStreamExt};
use prost::Message;
use snafu::prelude::*;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::broadcast::Sender;
use tokio::sync::{broadcast, RwLock};
use tonic::metadata::MetadataValue;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

mod sql_info;

use arrow_flight::{
    flight_service_server::{FlightService, FlightServiceServer},
    FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse,
    PutResult, SchemaAsIpc, Ticket,
};

use self::sql_info::get_sql_info_data;

pub(crate) static CREATE_PREPARED_STATEMENT: &str = "CreatePreparedStatement";
pub(crate) static CLOSE_PREPARED_STATEMENT: &str = "ClosePreparedStatement";
pub(crate) static CREATE_PREPARED_SUBSTRAIT_PLAN: &str = "CreatePreparedSubstraitPlan";
pub(crate) static BEGIN_TRANSACTION: &str = "BeginTransaction";
pub(crate) static END_TRANSACTION: &str = "EndTransaction";
pub(crate) static BEGIN_SAVEPOINT: &str = "BeginSavepoint";
pub(crate) static END_SAVEPOINT: &str = "EndSavepoint";
pub(crate) static CANCEL_QUERY: &str = "CancelQuery";

pub struct Service {
    datafusion: Arc<RwLock<DataFusion>>,
    channel_map: Arc<RwLock<HashMap<String, Arc<Sender<DataUpdate>>>>>,
}

async fn get_sender_channel(
    channel_map: Arc<RwLock<HashMap<String, Arc<Sender<DataUpdate>>>>>,
    path: String,
) -> Option<Arc<Sender<DataUpdate>>> {
    let channel_map_read = channel_map.read().await;
    if channel_map_read.contains_key(&path) {
        let Some(channel) = channel_map_read.get(&path) else {
            return None;
        };
        Some(Arc::clone(channel))
    } else {
        None
    }
}

#[tonic::async_trait]
impl FlightService for Service {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + 'static>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + 'static>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send + 'static>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        tracing::trace!("handshake");
        let res = self.do_handshake(request).await?;
        Ok(res)
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        tracing::trace!("list_flights");
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        tracing::trace!("get_flight_info");
        let message = Any::decode(&*request.get_ref().cmd).map_err(to_tonic_err)?;

        match Command::try_from(message).map_err(to_tonic_err)? {
            Command::CommandStatementQuery(token) => {
                //self.get_flight_info_statement(token, request).await
                Err(Status::unimplemented("Not yet implemented"))
            }
            Command::CommandPreparedStatementQuery(handle) => {
                self.get_flight_info_prepared_statement(handle, request)
                    .await
            }
            Command::CommandStatementSubstraitPlan(handle) => {
                //self.get_flight_info_substrait_plan(handle, request).await
                Err(Status::unimplemented("Not yet implemented"))
            }
            Command::CommandGetCatalogs(token) => {
                //self.get_flight_info_catalogs(token, request).await
                Err(Status::unimplemented("Not yet implemented"))
            }
            Command::CommandGetDbSchemas(token) => {
                //return self.get_flight_info_schemas(token, request).await
                Err(Status::unimplemented("Not yet implemented"))
            }
            Command::CommandGetTables(token) => self.get_flight_info_tables(token, request).await,
            Command::CommandGetTableTypes(token) => {
                //self.get_flight_info_table_types(token, request).await
                Err(Status::unimplemented("Not yet implemented"))
            }
            Command::CommandGetSqlInfo(token) => {
                self.get_flight_info_sql_info(token, request).await
            }
            Command::CommandGetPrimaryKeys(token) => {
                //self.get_flight_info_primary_keys(token, request).await
                Err(Status::unimplemented("Not yet implemented"))
            }
            Command::CommandGetExportedKeys(token) => {
                //self.get_flight_info_exported_keys(token, request).await
                Err(Status::unimplemented("Not yet implemented"))
            }
            Command::CommandGetImportedKeys(token) => {
                //self.get_flight_info_imported_keys(token, request).await
                Err(Status::unimplemented("Not yet implemented"))
            }
            Command::CommandGetCrossReference(token) => {
                //self.get_flight_info_cross_reference(token, request).await
                Err(Status::unimplemented("Not yet implemented"))
            }
            Command::CommandGetXdbcTypeInfo(token) => {
                //self.get_flight_info_xdbc_type_info(token, request).await
                Err(Status::unimplemented("Not yet implemented"))
            }
            cmd => self.get_flight_info_fallback(cmd, request).await,
        }
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        tracing::trace!("get_schema");
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        tracing::trace!("do_get");
        let msg: Any = Message::decode(&*request.get_ref().ticket).map_err(to_tonic_err)?;

        match Command::try_from(msg).map_err(to_tonic_err)? {
            Command::TicketStatementQuery(command) => self.do_get_statement(command, request).await,
            Command::CommandPreparedStatementQuery(command) => {
                self.do_get_prepared_statement(command, request).await
            }
            Command::CommandGetCatalogs(command) => self.do_get_catalogs(command, request).await,
            Command::CommandGetDbSchemas(command) => self.do_get_schemas(command, request).await,
            Command::CommandGetTables(command) => self.do_get_tables(command, request).await,
            Command::CommandGetTableTypes(command) => {
                //self.do_get_table_types(command, request).await
                Err(Status::unimplemented("Not yet implemented"))
            }
            Command::CommandGetSqlInfo(command) => self.do_get_sql_info(command, request).await,
            Command::CommandGetPrimaryKeys(command) => {
                //self.do_get_primary_keys(command, request).await
                Err(Status::unimplemented("Not yet implemented"))
            }
            Command::CommandGetExportedKeys(command) => {
                //self.do_get_exported_keys(command, request).await
                Err(Status::unimplemented("Not yet implemented"))
            }
            Command::CommandGetImportedKeys(command) => {
                //self.do_get_imported_keys(command, request).await
                Err(Status::unimplemented("Not yet implemented"))
            }
            Command::CommandGetCrossReference(command) => {
                //self.do_get_cross_reference(command, request).await
                Err(Status::unimplemented("Not yet implemented"))
            }
            Command::CommandGetXdbcTypeInfo(command) => {
                //self.do_get_xdbc_type_info(command, request).await
                Err(Status::unimplemented("Not yet implemented"))
            }
            cmd => self.do_get_fallback(request, cmd.into_any()).await,
        }
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        tracing::trace!("do_put");
        let res = self.do_put_fallback(request).await?;
        Ok(res)
    }

    async fn list_actions(
        &self,
        _request: Request<arrow_flight::Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        tracing::trace!("list_actions");
        let create_prepared_statement_action_type = ActionType {
            r#type: CREATE_PREPARED_STATEMENT.to_string(),
            description: "Creates a reusable prepared statement resource on the server.\n
                Request Message: ActionCreatePreparedStatementRequest\n
                Response Message: ActionCreatePreparedStatementResult"
                .into(),
        };
        let close_prepared_statement_action_type = ActionType {
            r#type: CLOSE_PREPARED_STATEMENT.to_string(),
            description: "Closes a reusable prepared statement resource on the server.\n
                Request Message: ActionClosePreparedStatementRequest\n
                Response Message: N/A"
                .into(),
        };
        let create_prepared_substrait_plan_action_type = ActionType {
            r#type: CREATE_PREPARED_SUBSTRAIT_PLAN.to_string(),
            description: "Creates a reusable prepared substrait plan resource on the server.\n
                Request Message: ActionCreatePreparedSubstraitPlanRequest\n
                Response Message: ActionCreatePreparedStatementResult"
                .into(),
        };
        let begin_transaction_action_type = ActionType {
            r#type: BEGIN_TRANSACTION.to_string(),
            description: "Begins a transaction.\n
                Request Message: ActionBeginTransactionRequest\n
                Response Message: ActionBeginTransactionResult"
                .into(),
        };
        let end_transaction_action_type = ActionType {
            r#type: END_TRANSACTION.to_string(),
            description: "Ends a transaction\n
                Request Message: ActionEndTransactionRequest\n
                Response Message: N/A"
                .into(),
        };
        let begin_savepoint_action_type = ActionType {
            r#type: BEGIN_SAVEPOINT.to_string(),
            description: "Begins a savepoint.\n
                Request Message: ActionBeginSavepointRequest\n
                Response Message: ActionBeginSavepointResult"
                .into(),
        };
        let end_savepoint_action_type = ActionType {
            r#type: END_SAVEPOINT.to_string(),
            description: "Ends a savepoint\n
                Request Message: ActionEndSavepointRequest\n
                Response Message: N/A"
                .into(),
        };
        let cancel_query_action_type = ActionType {
            r#type: CANCEL_QUERY.to_string(),
            description: "Cancels a query\n
                Request Message: ActionCancelQueryRequest\n
                Response Message: ActionCancelQueryResult"
                .into(),
        };
        let mut actions: Vec<Result<ActionType, Status>> = vec![
            Ok(create_prepared_statement_action_type),
            Ok(close_prepared_statement_action_type),
            Ok(create_prepared_substrait_plan_action_type),
            Ok(begin_transaction_action_type),
            Ok(end_transaction_action_type),
            Ok(begin_savepoint_action_type),
            Ok(end_savepoint_action_type),
            Ok(cancel_query_action_type),
        ];

        // if let Some(mut custom_actions) = self.list_custom_actions().await {
        //     actions.append(&mut custom_actions);
        // }

        let output = futures::stream::iter(actions);
        Ok(Response::new(Box::pin(output) as Self::ListActionsStream))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        tracing::trace!("do_action");
        if request.get_ref().r#type == CREATE_PREPARED_STATEMENT {
            let any = Any::decode(&*request.get_ref().body).map_err(to_tonic_err)?;

            let cmd: sql::ActionCreatePreparedStatementRequest =
                any.unpack().map_err(to_tonic_err)?.ok_or_else(|| {
                    Status::invalid_argument(
                        "Unable to unpack ActionCreatePreparedStatementRequest.",
                    )
                })?;
            let stmt = self
                .do_action_create_prepared_statement(cmd, request)
                .await?;
            let output = futures::stream::iter(vec![Ok(arrow_flight::Result {
                body: stmt.as_any().encode_to_vec().into(),
            })]);
            return Ok(Response::new(Box::pin(output)));
        } else if request.get_ref().r#type == CLOSE_PREPARED_STATEMENT {
            let any = Any::decode(&*request.get_ref().body).map_err(to_tonic_err)?;

            let cmd: sql::ActionClosePreparedStatementRequest =
                any.unpack().map_err(to_tonic_err)?.ok_or_else(|| {
                    Status::invalid_argument(
                        "Unable to unpack ActionClosePreparedStatementRequest.",
                    )
                })?;
            // self.do_action_close_prepared_statement(cmd, request)
            //     .await?;
            return Ok(Response::new(Box::pin(futures::stream::empty())));
        } else if request.get_ref().r#type == CREATE_PREPARED_SUBSTRAIT_PLAN {
            let any = Any::decode(&*request.get_ref().body).map_err(to_tonic_err)?;

            let cmd: sql::ActionCreatePreparedSubstraitPlanRequest =
                any.unpack().map_err(to_tonic_err)?.ok_or_else(|| {
                    Status::invalid_argument(
                        "Unable to unpack ActionCreatePreparedSubstraitPlanRequest.",
                    )
                })?;
            // self.do_action_create_prepared_substrait_plan(cmd, request)
            //     .await?;
            return Ok(Response::new(Box::pin(futures::stream::empty())));
        } else if request.get_ref().r#type == BEGIN_TRANSACTION {
            let any = Any::decode(&*request.get_ref().body).map_err(to_tonic_err)?;

            let cmd: sql::ActionBeginTransactionRequest =
                any.unpack().map_err(to_tonic_err)?.ok_or_else(|| {
                    Status::invalid_argument("Unable to unpack ActionBeginTransactionRequest.")
                })?;
            let stmt = self.do_action_begin_transaction(cmd, request).await?;
            let output = futures::stream::iter(vec![Ok(arrow_flight::Result {
                body: stmt.as_any().encode_to_vec().into(),
            })]);
            return Ok(Response::new(Box::pin(output)));
        } else if request.get_ref().r#type == END_TRANSACTION {
            let any = Any::decode(&*request.get_ref().body).map_err(to_tonic_err)?;

            let cmd: sql::ActionEndTransactionRequest =
                any.unpack().map_err(to_tonic_err)?.ok_or_else(|| {
                    Status::invalid_argument("Unable to unpack ActionEndTransactionRequest.")
                })?;
            self.do_action_end_transaction(cmd, request).await?;
            return Ok(Response::new(Box::pin(futures::stream::empty())));
        } else if request.get_ref().r#type == BEGIN_SAVEPOINT {
            let any = Any::decode(&*request.get_ref().body).map_err(to_tonic_err)?;

            let cmd: sql::ActionBeginSavepointRequest =
                any.unpack().map_err(to_tonic_err)?.ok_or_else(|| {
                    Status::invalid_argument("Unable to unpack ActionBeginSavepointRequest.")
                })?;
            let stmt = self.do_action_begin_savepoint(cmd, request).await?;
            let output = futures::stream::iter(vec![Ok(arrow_flight::Result {
                body: stmt.as_any().encode_to_vec().into(),
            })]);
            return Ok(Response::new(Box::pin(output)));
        } else if request.get_ref().r#type == END_SAVEPOINT {
            let any = Any::decode(&*request.get_ref().body).map_err(to_tonic_err)?;

            let cmd: sql::ActionEndSavepointRequest =
                any.unpack().map_err(to_tonic_err)?.ok_or_else(|| {
                    Status::invalid_argument("Unable to unpack ActionEndSavepointRequest.")
                })?;
            self.do_action_end_savepoint(cmd, request).await?;
            return Ok(Response::new(Box::pin(futures::stream::empty())));
        } else if request.get_ref().r#type == CANCEL_QUERY {
            let any = Any::decode(&*request.get_ref().body).map_err(to_tonic_err)?;

            let cmd: sql::ActionCancelQueryRequest =
                any.unpack().map_err(to_tonic_err)?.ok_or_else(|| {
                    Status::invalid_argument("Unable to unpack ActionCancelQueryRequest.")
                })?;
            let stmt = self.do_action_cancel_query(cmd, request).await?;
            let output = futures::stream::iter(vec![Ok(arrow_flight::Result {
                body: stmt.as_any().encode_to_vec().into(),
            })]);
            return Ok(Response::new(Box::pin(output)));
        }

        self.do_action_fallback(request).await
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        tracing::trace!("do_exchange");
        self.do_exchange_fallback(request).await
    }
}

impl Service {
    async fn get_arrow_schema_and_size_sql(
        datafusion: Arc<RwLock<DataFusion>>,
        sql: String,
    ) -> Result<(Schema, usize), Status> {
        let df = datafusion
            .read()
            .await
            .ctx
            .sql(&sql)
            .await
            .map_err(to_tonic_err)?;

        let schema = df.schema();
        let arrow_schema: Schema = schema.into();

        let size = df.count().await.map_err(to_tonic_err)?;

        Ok((arrow_schema, size))
    }

    fn serialize_schema(schema: &Schema) -> Result<Bytes, Status> {
        let message: IpcMessage = SchemaAsIpc::new(schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(to_tonic_err)?;
        let IpcMessage(schema_bytes) = message;

        Ok(schema_bytes)
    }

    async fn sql_to_flight_stream(
        datafusion: Arc<RwLock<DataFusion>>,
        sql: String,
    ) -> Result<BoxStream<'static, Result<FlightData, Status>>, Status> {
        let df = datafusion
            .read()
            .await
            .ctx
            .sql(&sql)
            .await
            .map_err(to_tonic_err)?;
        let schema = df.schema().clone().into();
        let options = datafusion::arrow::ipc::writer::IpcWriteOptions::default();
        let schema_as_ipc = SchemaAsIpc::new(&schema, &options);
        let schema_flight_data = FlightData::from(schema_as_ipc);

        let batches_stream: SendableRecordBatchStream =
            df.execute_stream().await.map_err(to_tonic_err)?;

        let batches_stream = batches_stream
            .then(move |batch_result| {
                let options_clone = options.clone();
                async move {
                    let encoder = IpcDataGenerator::default();
                    let mut tracker = DictionaryTracker::new(false);

                    match batch_result {
                        Ok(batch) => {
                            let (flight_dictionaries, flight_batch) = encoder
                                .encoded_batch(&batch, &mut tracker, &options_clone)
                                .map_err(|e| Status::internal(e.to_string()))?;

                            let mut flights: Vec<FlightData> =
                                flight_dictionaries.into_iter().map(Into::into).collect();
                            flights.push(flight_batch.into());
                            Ok(flights)
                        }
                        Err(e) => Err(Status::internal(e.to_string())),
                    }
                }
            })
            .map(|result| {
                // Convert Result<Vec<FlightData>, Status> into Stream of Result<FlightData, Status>
                match result {
                    Ok(flights) => stream::iter(flights.into_iter().map(Ok)).left_stream(),
                    Err(e) => stream::once(async { Err(e) }).right_stream(),
                }
            })
            .flatten();

        let flights_stream = stream::once(async { Ok(schema_flight_data) }).chain(batches_stream);

        Ok(flights_stream.boxed())
    }

    #[allow(clippy::unused_async)]
    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        // THIS IS PLACEHOLDER NO-OP AUTH THAT DOES NOT CHECK THE PROVIDED TOKEN AND SIMPLY RETURNS A UUID.
        // TODO: Implement proper auth.
        let token = Uuid::new_v4().to_string();
        let result = HandshakeResponse {
            protocol_version: 0,
            payload: token.as_bytes().to_vec().into(),
        };
        let result = Ok(result);
        let output = futures::stream::iter(vec![result]);
        let str = format!("Bearer {token}");
        let mut resp: Response<Pin<Box<dyn Stream<Item = Result<_, _>> + Send>>> =
            Response::new(Box::pin(output));
        let md = MetadataValue::try_from(str)
            .map_err(|_| Status::internal("generated authorization could not be parsed"))?;
        resp.metadata_mut().insert("authorization", md);
        Ok(resp)
    }

    async fn do_get_catalogs(
        &self,
        query: sql::CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        tracing::trace!("do_get_catalogs: {query:?}");
        let mut builder = query.into_builder();

        let catalog_names = self.datafusion.read().await.ctx.catalog_names();

        for catalog in catalog_names {
            builder.append(catalog);
        }

        let record_batch = builder.build().map_err(to_tonic_err)?;

        Ok(Response::new(
            Box::pin(record_batches_to_flight_stream(vec![record_batch])?)
                as <Service as FlightService>::DoGetStream,
        ))
    }

    async fn do_get_schemas(
        &self,
        query: sql::CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let catalog = &query.catalog;
        tracing::trace!("do_get_schemas: {query:?}");
        let filtered_catalogs = match catalog {
            Some(catalog) => vec![catalog.to_string()],
            None => self.datafusion.read().await.ctx.catalog_names(),
        };
        let mut builder = query.into_builder();

        for catalog in filtered_catalogs {
            let catalog_provider = self
                .datafusion
                .read()
                .await
                .ctx
                .catalog(&catalog)
                .ok_or_else(|| {
                    Status::internal(format!("unable to get catalog provider for {catalog}"))
                })?;
            for schema in catalog_provider.schema_names() {
                builder.append(&catalog, schema);
            }
        }

        let record_batch = builder.build().map_err(to_tonic_err)?;

        Ok(Response::new(
            Box::pin(record_batches_to_flight_stream(vec![record_batch])?)
                as <Service as FlightService>::DoGetStream,
        ))
    }

    async fn do_get_tables(
        &self,
        query: sql::CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let catalog = &query.catalog;
        tracing::trace!("do_get_tables: {query:?}");
        let filtered_catalogs = match catalog {
            Some(catalog) => vec![catalog.to_string()],
            None => self.datafusion.read().await.ctx.catalog_names(),
        };
        let mut builder = query.into_builder();

        for catalog_name in filtered_catalogs {
            let catalog_provider = self
                .datafusion
                .read()
                .await
                .ctx
                .catalog(&catalog_name)
                .ok_or_else(|| {
                    Status::internal(format!("unable to get catalog provider for {catalog_name}"))
                })?;

            for schema_name in catalog_provider.schema_names() {
                let Some(schema_provider) = catalog_provider.schema(&schema_name) else {
                    continue;
                };

                for table_name in schema_provider.table_names() {
                    let Some(table_provider) = schema_provider.table(&table_name).await else {
                        continue;
                    };

                    let table_type = table_type_name(table_provider.table_type());

                    builder.append(
                        &catalog_name,
                        &schema_name,
                        &table_name,
                        table_type,
                        table_provider.schema().as_ref(),
                    )?;
                }
            }
        }

        let record_batch = builder.build().map_err(to_tonic_err)?;

        Ok(Response::new(
            Box::pin(record_batches_to_flight_stream(vec![record_batch])?)
                as <Service as FlightService>::DoGetStream,
        ))
    }

    async fn do_get_statement(
        &self,
        ticket: sql::TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let datafusion = Arc::clone(&self.datafusion);
        tracing::trace!("do_get_statement: {ticket:?}");
        match std::str::from_utf8(&ticket.statement_handle) {
            Ok(sql) => {
                let output = Self::sql_to_flight_stream(datafusion, sql.to_owned()).await?;
                Ok(Response::new(
                    Box::pin(output) as <Service as FlightService>::DoGetStream
                ))
            }
            Err(e) => Err(Status::invalid_argument(format!("Invalid ticket: {e:?}"))),
        }
    }

    async fn do_get_prepared_statement(
        &self,
        query: sql::CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let datafusion = Arc::clone(&self.datafusion);
        tracing::trace!("do_get_prepared_statement: {query:?}");
        match std::str::from_utf8(&query.prepared_statement_handle) {
            Ok(sql) => {
                let output = Self::sql_to_flight_stream(datafusion, sql.to_owned()).await?;
                Ok(Response::new(
                    Box::pin(output) as <Service as FlightService>::DoGetStream
                ))
            }
            Err(e) => Err(Status::invalid_argument(format!(
                "Invalid prepared statement handle: {e:?}"
            ))),
        }
    }

    // If the ticket isn't a FlightSQL command, then try interpreting the ticket as a raw SQL query.
    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        _message: arrow_flight::sql::Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let datafusion = Arc::clone(&self.datafusion);
        let ticket = request.into_inner();
        tracing::trace!("do_get_fallback: {ticket:?}");
        match std::str::from_utf8(&ticket.ticket) {
            Ok(sql) => {
                let output = Self::sql_to_flight_stream(datafusion, sql.to_owned()).await?;
                Ok(Response::new(
                    Box::pin(output) as <Service as FlightService>::DoGetStream
                ))
            }
            Err(e) => Err(Status::invalid_argument(format!("Invalid ticket: {e:?}"))),
        }
    }

    #[allow(clippy::unused_async)]
    async fn get_flight_info_tables(
        &self,
        query: sql::CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let fd = request.into_inner();
        tracing::trace!("get_flight_info_tables: {query:?}");
        Ok(Response::new(FlightInfo {
            flight_descriptor: Some(fd.clone()),
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket { ticket: fd.cmd }),
                ..Default::default()
            }],
            ..Default::default()
        }))
    }

    async fn get_flight_info_prepared_statement(
        &self,
        handle: sql::CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        tracing::trace!("get_flight_info_prepared_statement: {handle:?}");

        let sql = match std::str::from_utf8(&handle.prepared_statement_handle) {
            Ok(sql) => sql.to_string(),
            Err(e) => {
                return Err(Status::invalid_argument(format!(
                    "Invalid prepared statement handle: {e:?}"
                )))
            }
        };

        let (arrow_schema, num_rows) =
            Self::get_arrow_schema_and_size_sql(Arc::clone(&self.datafusion), sql)
                .await
                .map_err(to_tonic_err)?;

        tracing::trace!("get_flight_info_prepared_statement: arrow_schema={arrow_schema:?} num_rows={num_rows:?}");

        let fd = request.into_inner();

        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: handle.as_any().encode_to_vec().into(),
        });

        let info = FlightInfo::new()
            .with_endpoint(endpoint)
            .try_with_schema(&arrow_schema)
            .map_err(to_tonic_err)?
            .with_descriptor(fd)
            .with_total_records(num_rows.try_into().map_err(to_tonic_err)?);

        Ok(Response::new(info))
    }

    #[allow(clippy::unused_async)]
    async fn get_flight_info_fallback(
        &self,
        cmd: sql::Command,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        tracing::trace!("get_flight_info_fallback: {cmd:?}");
        let fd = request.into_inner();
        Ok(Response::new(FlightInfo {
            flight_descriptor: Some(fd.clone()),
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket { ticket: fd.cmd }),
                ..Default::default()
            }],
            ..Default::default()
        }))
    }

    /// Create a prepared statement from given SQL statement.
    async fn do_action_create_prepared_statement(
        &self,
        statement: sql::ActionCreatePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<sql::ActionCreatePreparedStatementResult, Status> {
        tracing::trace!("do_action_create_prepared_statement: {statement:?}");
        let (arrow_schema, _) = Self::get_arrow_schema_and_size_sql(
            Arc::clone(&self.datafusion),
            statement.query.clone(),
        )
        .await
        .map_err(to_tonic_err)?;

        let schema_bytes = Self::serialize_schema(&arrow_schema)?;

        Ok(sql::ActionCreatePreparedStatementResult {
            prepared_statement_handle: statement.query.into(),
            dataset_schema: schema_bytes,
            ..Default::default()
        })
    }

    async fn do_put_fallback(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        tracing::trace!("do_put_fallback");
        let mut streaming_flight = request.into_inner();

        let Ok(Some(message)) = streaming_flight.message().await else {
            return Err(Status::invalid_argument("No flight data provided"));
        };
        let Some(fd) = &message.flight_descriptor else {
            return Err(Status::invalid_argument("No flight descriptor provided"));
        };
        if fd.path.is_empty() {
            return Err(Status::invalid_argument("No path provided"));
        };

        let path = fd.path.join(".");

        let df = self.datafusion.read().await;

        let Some(publishers) = df.get_publishers(&path) else {
            return Err(Status::invalid_argument(format!(
                "No publishers registered for path: {path:?}",
            )));
        };
        let dataset = Arc::clone(&publishers.0);
        let data_publishers = Arc::clone(&publishers.1);

        let schema = try_schema_from_flatbuffer_bytes(&message.data_header).map_err(|e| {
            Status::internal(format!("Unable to get schema from data header: {e:?}"))
        })?;
        let schema = Arc::new(schema);
        let dictionaries_by_id = Arc::new(HashMap::new());

        // Sometimes the first message only contains the schema and no data
        let first_batch = arrow_flight::utils::flight_data_to_arrow_batch(
            &message,
            schema.clone(),
            &dictionaries_by_id,
        )
        .ok();

        let mut batches = vec![];
        if let Some(first_batch) = first_batch {
            batches.push(first_batch);
        }

        let channel_map = Arc::clone(&self.channel_map);

        let response_stream = stream::unfold(streaming_flight, move |mut flight| {
            let schema = Arc::clone(&schema);
            let dictionaries_by_id = Arc::clone(&dictionaries_by_id);
            let dataset = Arc::clone(&dataset);
            let data_publishers = Arc::clone(&data_publishers);
            let path = path.clone();
            let channel_map = Arc::clone(&channel_map);
            async move {
                match flight.message().await {
                    Ok(Some(message)) => {
                        let new_batch = match arrow_flight::utils::flight_data_to_arrow_batch(
                            &message,
                            schema.clone(),
                            &dictionaries_by_id,
                        ) {
                            Ok(batches) => batches,
                            Err(e) => {
                                tracing::error!("Unable to convert flight data to batches: {e:?}");
                                return None;
                            }
                        };
                        tracing::trace!("Received batch with {} rows", new_batch.num_rows());

                        let data_update = DataUpdate {
                            data: vec![new_batch],
                            update_type: UpdateType::Append,
                        };

                        if let Some(channel) = get_sender_channel(channel_map, path).await {
                            let _ = channel.send(data_update.clone());
                        };

                        let data_publishers = data_publishers.read().await;
                        for publisher in data_publishers.iter() {
                            if let Err(err) = publisher
                                .add_data(Arc::clone(&dataset), data_update.clone())
                                .await
                                .map_err(|e| Status::internal(format!("Unable to add data: {e:?}")))
                            {
                                return Some((Err(err), flight));
                            };
                        }

                        Some((Ok(PutResult::default()), flight))
                    }
                    Ok(None) => {
                        // End of the stream
                        None
                    }
                    Err(e) => Some((
                        Err(Status::internal(format!("Error reading message: {e:?}"))),
                        flight,
                    )),
                }
            }
        });

        Ok(Response::new(response_stream.boxed()))
    }

    #[allow(clippy::too_many_lines)]
    async fn do_exchange_fallback(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<<Self as FlightService>::DoExchangeStream>, Status> {
        tracing::trace!("do_exchange_fallback");
        let mut streaming_request = request.into_inner();
        let req = streaming_request.next().await;
        let Some(subscription_request) = req else {
            return Err(Status::invalid_argument(
                "Need to send a FlightData message with a FlightDescriptor to subscribe to",
            ));
        };

        let subscription_request = match subscription_request {
            Ok(subscription_request) => subscription_request,
            Err(e) => {
                return Err(Status::invalid_argument(format!(
                    "Unable to read subscription request: {e:?}",
                )));
            }
        };

        // TODO: Support multiple flight descriptors to subscribe to multiple data sources
        let Some(flight_descriptor) = subscription_request.flight_descriptor else {
            return Err(Status::invalid_argument(
                "Flight descriptor required to indicate which data to subscribe to",
            ));
        };

        if flight_descriptor.path.is_empty() {
            return Err(Status::invalid_argument(
                "Flight descriptor needs to specify a path to indicate which data to subscribe to",
            ));
        };

        let data_path = flight_descriptor.path.join(".");

        if !self.datafusion.read().await.has_publishers(&data_path) {
            return Err(Status::invalid_argument(format!(
                r#"Unknown dataset: "{data_path}""#,
            )));
        };

        let channel_map = Arc::clone(&self.channel_map);
        let channel_map_read = channel_map.read().await;
        let (tx, rx) = if let Some(channel) = channel_map_read.get(&data_path) {
            (Arc::clone(channel), channel.subscribe())
        } else {
            drop(channel_map_read);
            let mut channel_map_write = channel_map.write().await;
            let (tx, rx) = broadcast::channel(100);
            let tx = Arc::new(tx);
            channel_map_write.insert(data_path.clone(), Arc::clone(&tx));
            (tx, rx)
        };

        let response_stream = stream::unfold(rx, move |mut rx| {
            let encoder = IpcDataGenerator::default();
            let mut tracker = DictionaryTracker::new(false);
            let write_options = writer::IpcWriteOptions::default();
            async move {
                match rx.recv().await {
                    Ok(data_update) => {
                        let mut schema_sent: bool = false;

                        let mut flights = vec![];

                        for batch in &data_update.data {
                            if !schema_sent {
                                let schema = batch.schema();
                                flights.push(FlightData::from(SchemaAsIpc::new(
                                    &schema,
                                    &write_options,
                                )));
                                schema_sent = true;
                            }
                            let Ok((flight_dictionaries, flight_batch)) =
                                encoder.encoded_batch(batch, &mut tracker, &write_options)
                            else {
                                panic!("Unable to encode batch")
                            };

                            flights.extend(flight_dictionaries.into_iter().map(Into::into));
                            flights.push(flight_batch.into());
                        }

                        let output = futures::stream::iter(flights.into_iter().map(Ok));

                        Some((output, rx))
                    }
                    Err(_e) => {
                        let output = futures::stream::iter(vec![].into_iter().map(Ok));
                        Some((output, rx))
                    }
                }
            }
        })
        .flat_map(|x| x);

        let datafusion = Arc::clone(&self.datafusion);
        tokio::spawn(async move {
            let Ok(df) = datafusion
                .read()
                .await
                .ctx
                .sql(&format!(r#"SELECT * FROM "{data_path}""#))
                .await
            else {
                return;
            };
            let Ok(results) = df.collect().await else {
                return;
            };
            if results.is_empty() {
                return;
            }

            for batch in &results {
                let data_update = DataUpdate {
                    data: vec![batch.clone()],
                    update_type: UpdateType::Append,
                };
                let _ = tx.send(data_update);
            }
        });

        Ok(Response::new(response_stream.boxed()))
    }

    /// Begin a transaction
    #[allow(clippy::unused_async)]
    async fn do_action_begin_transaction(
        &self,
        _query: sql::ActionBeginTransactionRequest,
        _request: Request<Action>,
    ) -> Result<sql::ActionBeginTransactionResult, Status> {
        Err(Status::unimplemented(
            "do_action_begin_transaction has no default implementation",
        ))
    }

    /// End a transaction
    #[allow(clippy::unused_async)]
    async fn do_action_end_transaction(
        &self,
        _query: sql::ActionEndTransactionRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Err(Status::unimplemented(
            "do_action_end_transaction has no default implementation",
        ))
    }

    /// Begin a savepoint
    #[allow(clippy::unused_async)]
    async fn do_action_begin_savepoint(
        &self,
        _query: sql::ActionBeginSavepointRequest,
        _request: Request<Action>,
    ) -> Result<sql::ActionBeginSavepointResult, Status> {
        Err(Status::unimplemented(
            "do_action_begin_savepoint has no default implementation",
        ))
    }

    /// End a savepoint
    #[allow(clippy::unused_async)]
    async fn do_action_end_savepoint(
        &self,
        _query: sql::ActionEndSavepointRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Err(Status::unimplemented(
            "do_action_end_savepoint has no default implementation",
        ))
    }

    /// Cancel a query
    #[allow(clippy::unused_async)]
    async fn do_action_cancel_query(
        &self,
        _query: sql::ActionCancelQueryRequest,
        _request: Request<Action>,
    ) -> Result<sql::ActionCancelQueryResult, Status> {
        Err(Status::unimplemented(
            "do_action_cancel_query has no default implementation",
        ))
    }

    #[allow(clippy::unused_async)]
    async fn do_action_fallback(
        &self,
        request: Request<Action>,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        Err(Status::invalid_argument(format!(
            "do_action: The defined request is invalid: {:?}",
            request.get_ref().r#type
        )))
    }

    /// Get a FlightInfo for retrieving other information (See SqlInfo).
    #[allow(clippy::unused_async)]
    async fn get_flight_info_sql_info(
        &self,
        query: sql::CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        tracing::trace!("get_flight_info_sql_info: query={query:?}");
        let builder = query.clone().into_builder(get_sql_info_data());
        let record_batch = builder.build().map_err(to_tonic_err)?;

        let fd = request.into_inner();

        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };

        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        Ok(Response::new(
            FlightInfo::new()
                .with_endpoint(endpoint)
                .with_descriptor(fd)
                .try_with_schema(&record_batch.schema())
                .map_err(to_tonic_err)?,
        ))
    }

    /// Get a FlightDataStream containing the list of SqlInfo results.
    #[allow(clippy::unused_async)]
    async fn do_get_sql_info(
        &self,
        query: sql::CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        tracing::trace!("do_get_sql_info: {query:?}");
        let builder = query.into_builder(get_sql_info_data());
        let record_batch = builder.build().map_err(to_tonic_err)?;

        let batches_stream = stream::iter(vec![Ok(record_batch)]);

        let flight_data_stream = FlightDataEncoderBuilder::new().build(batches_stream);

        Ok(Response::new(
            flight_data_stream.map_err(to_tonic_err).boxed(),
        ))
    }
}

fn table_type_name(table_type: TableType) -> &'static str {
    match table_type {
        // from https://github.com/apache/arrow-datafusion/blob/26b8377b0690916deacf401097d688699026b8fb/datafusion/core/src/catalog/information_schema.rs#L284-L288
        TableType::Base => "BASE TABLE",
        TableType::View => "VIEW",
        TableType::Temporary => "LOCAL TEMPORARY",
    }
}

fn record_batches_to_flight_stream(
    record_batches: Vec<arrow::record_batch::RecordBatch>,
) -> Result<impl Stream<Item = Result<FlightData, Status>>, Status> {
    let options = datafusion::arrow::ipc::writer::IpcWriteOptions::default();
    let mut flights: Vec<FlightData> = Vec::new();
    let encoder = IpcDataGenerator::default();
    let mut tracker = DictionaryTracker::new(false);

    for record_batch in record_batches {
        let schema = record_batch.schema();
        let schema_flight_data = SchemaAsIpc::new(&schema, &options);
        flights.push(FlightData::from(schema_flight_data));

        let (flight_dictionaries, flight_batch) = encoder
            .encoded_batch(&record_batch, &mut tracker, &options)
            .map_err(|e: ArrowError| Status::internal(e.to_string()))?;

        flights.extend(flight_dictionaries.into_iter().map(Into::into));
        flights.push(flight_batch.into());
    }

    Ok(futures::stream::iter(flights.into_iter().map(Ok)))
}

#[allow(clippy::needless_pass_by_value)]
fn to_tonic_err<E>(e: E) -> Status
where
    E: std::fmt::Debug,
{
    Status::internal(format!("{e:?}"))
}

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to register parquet file: {source}"))]
    RegisterParquet { source: crate::datafusion::Error },

    #[snafu(display("{source}"))]
    DataFusion {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Unable to start Flight server: {source}"))]
    UnableToStartFlightServer { source: tonic::transport::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn start(bind_address: std::net::SocketAddr, df: Arc<RwLock<DataFusion>>) -> Result<()> {
    let service = Service {
        datafusion: df.clone(),
        channel_map: Arc::new(RwLock::new(HashMap::new())),
    };
    let svc = FlightServiceServer::new(service);

    tracing::info!("Spice Runtime Flight listening on {bind_address}");
    metrics::counter!("spiced_runtime_flight_server_start").increment(1);

    Server::builder()
        .add_service(svc)
        .serve(bind_address)
        .await
        .context(UnableToStartFlightServerSnafu)?;

    Ok(())
}
