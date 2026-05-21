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

//! Arrow Flight SQL service backed by a `DataFusion` [`SessionContext`].
//!
//! # Overview
//!
//! This crate provides a [`FlightSqlService`] that implements the
//! [`arrow_flight::flight_service_server::FlightService`] gRPC trait over an
//! arbitrary `Arc<SessionContext>`. Unlike the Spice runtime's own flight
//! service it has **no** dependency on `runtime::DataFusion`, cluster
//! middleware, job executors, or caching infrastructure.
//!
//! # Usage
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use datafusion::prelude::SessionContext;
//! use datafusion_flightsql::FlightSqlService;
//! use arrow_flight::flight_service_server::FlightServiceServer;
//! use tonic::transport::Server;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let ctx = Arc::new(SessionContext::new());
//!     let svc = FlightSqlService::new(ctx);
//!     Server::builder()
//!         .add_service(FlightServiceServer::new(svc))
//!         .serve("0.0.0.0:50051".parse()?)
//!         .await?;
//!     Ok(())
//! }
//! ```

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::ipc::writer::{CompressionContext, DictionaryTracker, IpcDataGenerator};
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, IpcMessage, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
    flight_service_server::FlightServiceServer,
};
use arrow_ipc::writer::IpcWriteOptions;
use async_stream::try_stream;
use bytes::Bytes;
use datafusion::common::ParamValues;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use futures::stream::BoxStream;
use tonic::{Request, Response, Status, Streaming};

mod actions;
mod do_get;
mod do_put;
pub(crate) mod flightsql;
mod get_flight_info;
mod get_schema;
mod handshake;
mod session;
pub mod session_auth;
pub(crate) mod util;

pub use session::SessionStore;
pub(crate) use util::{handle_datafusion_error, record_batches_to_flight_stream, to_tonic_err};

/// A `FlightSQL` service backed by an arbitrary `DataFusion` [`SessionContext`].
///
/// Create with [`FlightSqlService::new`] and wrap in
/// [`FlightServiceServer`] to serve over gRPC.
pub struct FlightSqlService {
    /// Base context used when no per-session context exists for a request.
    ctx: Arc<SessionContext>,
    session_store: SessionStore,
}

impl FlightSqlService {
    /// Create a new service using `ctx` as the default query context.
    ///
    /// Call [`FlightSqlService::into_server`] to wrap in a `FlightServiceServer`.
    #[must_use]
    pub fn new(ctx: Arc<SessionContext>) -> Self {
        Self {
            ctx,
            session_store: SessionStore::new(),
        }
    }

    /// Returns a clone of the session store (useful for auth wrappers).
    #[must_use]
    pub fn session_store(&self) -> SessionStore {
        self.session_store.clone()
    }

    /// Wraps `self` in a [`FlightServiceServer`] ready for `Server::add_service`.
    #[must_use]
    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }

    /// Resolve the [`SessionContext`] for a request.
    ///
    /// Returns the per-session context if the request carries a known session
    /// ID (via `x-session-id` or `Authorization: Bearer <id>`), otherwise
    /// falls back to the base context.
    pub(crate) fn session_ctx(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Arc<SessionContext> {
        self.session_store
            .get_session_from_metadata(metadata)
            .unwrap_or_else(|| Arc::clone(&self.ctx))
    }
}

// ── FlightService implementation ─────────────────────────────────────────────

#[tonic::async_trait]
impl FlightService for FlightSqlService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        handshake::handle(request.metadata(), &self.ctx, &self.session_store)
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights is not implemented"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ctx = self.session_ctx(request.metadata());
        Box::pin(get_flight_info::handle(ctx, request)).await
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info is not implemented"))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let ctx = self.session_ctx(request.metadata());
        get_schema::handle(ctx, request).await
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ctx = self.session_ctx(request.metadata());
        Box::pin(do_get::handle(ctx, request)).await
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let ctx = self.session_ctx(request.metadata());
        do_put::handle(ctx, request).await
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented(
            "do_exchange is not supported by this service",
        ))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let ctx = self.session_ctx(request.metadata());
        Box::pin(actions::do_action(ctx, request)).await
    }

    async fn list_actions(
        &self,
        _request: Request<arrow_flight::Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Ok(actions::list())
    }
}

// ── Query execution utilities ─────────────────────────────────────────────────

impl FlightSqlService {
    /// Plan `sql` against `ctx` and return the output Arrow schema.
    ///
    /// Applies `expand_views_schema` so that the advertised schema matches
    /// what `sql_to_flight_stream` will actually produce.
    pub(crate) async fn get_arrow_schema(
        ctx: &Arc<SessionContext>,
        sql: &str,
    ) -> Result<Schema, Status> {
        let df = ctx.sql(sql).await.map_err(handle_datafusion_error)?;
        let schema = df.schema().as_arrow().clone();
        Ok(arrow_tools::schema::expand_views_schema(&schema))
    }

    /// Serialize an Arrow `Schema` to IPC bytes suitable for a `FlightInfo`.
    pub(crate) fn serialize_schema(schema: &Schema) -> Result<Bytes, Status> {
        let message: IpcMessage = SchemaAsIpc::new(schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(to_tonic_err)?;
        Ok(message.0)
    }

    /// Execute `sql` against `ctx` and encode the result as a Flight data stream.
    pub(crate) async fn sql_to_flight_stream(
        ctx: Arc<SessionContext>,
        sql: &str,
        parameters: Option<ParamValues>,
    ) -> Result<BoxStream<'static, Result<FlightData, Status>>, Status> {
        let df = ctx.sql(sql).await.map_err(handle_datafusion_error)?;

        let df = if let Some(params) = parameters {
            df.with_param_values(params)
                .map_err(handle_datafusion_error)?
        } else {
            df
        };

        let raw_schema = std::sync::Arc::clone(df.schema().inner());
        // Expand view types (Utf8View → LargeUtf8, BinaryView → LargeBinary) so
        // that the advertised schema matches the cast batches we send below.
        let schema = Arc::new(arrow_tools::schema::expand_views_schema(&raw_schema));
        let data_stream = df.execute_stream().await.map_err(handle_datafusion_error)?;

        // Pre-compute schema flight data once, matching the runtime's approach.
        let options = IpcWriteOptions::default();
        let mut dict_tracker = DictionaryTracker::new(true);
        let mut compression_context = CompressionContext::default();
        let encoder = IpcDataGenerator::default();
        let schema_ipc = IpcMessage(
            encoder
                .schema_to_bytes_with_dictionary_tracker(
                    schema.as_ref(),
                    &mut dict_tracker,
                    &options,
                )
                .ipc_message
                .into(),
        );
        let schema_flight_data = FlightData {
            data_header: schema_ipc.0,
            ..Default::default()
        };

        let stream = try_stream! {
            yield schema_flight_data;

            let mut data_stream = data_stream.fuse();
            while let Some(batch_result) = data_stream.next().await {
                match batch_result {
                    Ok(batch) => {
                        // Cast view types to their non-view equivalents to match
                        // the expanded schema we advertised in GetFlightInfo.
                        let batch = arrow_tools::schema::cast_view_columns(batch, &schema)
                            .map_err(|e| Status::internal(e.to_string()))?;
                        let (dicts, batch_data) = encoder
                            .encode(
                                &batch,
                                &mut dict_tracker,
                                &options,
                                &mut compression_context,
                            )
                            .map_err(|e| Status::internal(e.to_string()))?;
                        for dict in dicts {
                            yield dict.into();
                        }
                        yield batch_data.into();
                    }
                    Err(e) => {
                        Err(handle_datafusion_error(e))?;
                    }
                }
            }
        };

        Ok(stream.boxed())
    }
}
