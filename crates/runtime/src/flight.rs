use crate::datafusion::DataFusion;
use crate::dataupdate::{DataUpdate, UpdateType};
use arrow::datatypes::Schema;
use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator};
use arrow_flight::{sql, Action, IpcMessage};
use arrow_ipc::convert::try_schema_from_flatbuffer_bytes;
use arrow_ipc::writer::{self, IpcWriteOptions};
use datafusion::arrow::error::ArrowError;
use datafusion::datasource::TableType;
use datafusion::execution::SendableRecordBatchStream;
use futures::stream::{self, BoxStream, StreamExt};
use futures::Stream;
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

use arrow_flight::{
    flight_service_server::{FlightService, FlightServiceServer},
    sql::server::FlightSqlService,
    FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse,
    PutResult, SchemaAsIpc, Ticket,
};

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

impl Service {
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
}

#[tonic::async_trait]
impl FlightSqlService for Service {
    type FlightService = Service;

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
        let mut builder = query.into_builder();

        let catalog_names = self.datafusion.read().await.ctx.catalog_names();

        for catalog in catalog_names {
            builder.append(catalog);
        }

        let record_batch = builder.build().map_err(to_tonic_err)?;

        Ok(Response::new(
            Box::pin(record_batches_to_flight_stream(vec![record_batch])?)
                as <<Service as FlightSqlService>::FlightService as FlightService>::DoGetStream,
        ))
    }

    async fn do_get_schemas(
        &self,
        query: sql::CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let catalog = &query.catalog;
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
                as <<Service as FlightSqlService>::FlightService as FlightService>::DoGetStream,
        ))
    }

    async fn do_get_tables(
        &self,
        query: sql::CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let catalog = &query.catalog;
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
                as <<Service as FlightSqlService>::FlightService as FlightService>::DoGetStream,
        ))
    }

    async fn do_get_statement(
        &self,
        ticket: sql::TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let datafusion = Arc::clone(&self.datafusion);
        match std::str::from_utf8(&ticket.statement_handle) {
            Ok(sql) => {
                let output = Self::sql_to_flight_stream(datafusion, sql.to_owned()).await?;
                Ok(Response::new(Box::pin(output) as <<Service as FlightSqlService>::FlightService as FlightService>::DoGetStream))
            }
            Err(e) => Err(Status::invalid_argument(format!("Invalid ticket: {e:?}"))),
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
        match std::str::from_utf8(&ticket.ticket) {
            Ok(sql) => {
                let output = Self::sql_to_flight_stream(datafusion, sql.to_owned()).await?;
                Ok(Response::new(Box::pin(output) as <<Service as FlightSqlService>::FlightService as FlightService>::DoGetStream))
            }
            Err(e) => Err(Status::invalid_argument(format!("Invalid ticket: {e:?}"))),
        }
    }

    async fn get_flight_info_tables(
        &self,
        _query: sql::CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
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

    async fn get_flight_info_prepared_statement(
        &self,
        handle: sql::CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let fd = request.into_inner();
        Ok(Response::new(FlightInfo {
            flight_descriptor: Some(fd.clone()),
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: handle.prepared_statement_handle,
                }),
                ..Default::default()
            }],
            ..Default::default()
        }))
    }

    async fn get_flight_info_fallback(
        &self,
        _cmd: sql::Command,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
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
        let dataframe = self
            .datafusion
            .read()
            .await
            .ctx
            .sql(&statement.query)
            .await
            .map_err(to_tonic_err)?;

        let schema = dataframe.schema();
        let arrow_schema: Schema = schema.into();

        let message = SchemaAsIpc::new(&arrow_schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(to_tonic_err)?;
        let IpcMessage(schema_bytes) = message;

        Ok(sql::ActionCreatePreparedStatementResult {
            prepared_statement_handle: statement.query.into(),
            dataset_schema: schema_bytes,
            ..Default::default()
        })
    }

    async fn do_put_fallback(
        &self,
        request: Request<sql::server::PeekableFlightDataStream>,
        _message: sql::Any,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        let mut streaming_flight = request.into_inner().into_inner();

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

    async fn register_sql_info(&self, _id: i32, _result: &sql::SqlInfo) {}
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
