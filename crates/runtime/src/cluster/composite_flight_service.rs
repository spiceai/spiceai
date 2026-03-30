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

//! Composite Flight service that routes requests between Ballista and Spice Flight services.
//!
//! On executor nodes in distributed mode, we need to serve both:
//! - Ballista Flight protocol for shuffle data transfer between executors
//! - Spice Flight protocol for SQL queries and `FlightSQL`
//!
//! This composite service inspects incoming requests and routes them to the appropriate
//! underlying service based on the message format.

use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    flight_service_server::FlightService,
};
use ballista_core::serde::decode_protobuf;
use ballista_executor::flight_service::BallistaFlightService;
use std::pin::Pin;
use tonic::{Request, Response, Status, Streaming};

use crate::flight::Service as SpiceFlightService;

type BoxedFlightStream<T> =
    Pin<Box<dyn futures::Stream<Item = Result<T, Status>> + Send + 'static>>;

/// A composite Flight service that routes between Ballista and Spice Flight services.
///
/// Request routing logic:
/// - `do_get`: If ticket decodes as Ballista protobuf → Ballista, otherwise → Spice
/// - `do_action`: If action type is "`IO_BLOCK_TRANSPORT`" → Ballista, otherwise → Spice
/// - `list_actions`: Combined from both services
/// - All other methods → Spice (Ballista returns unimplemented)
pub struct CompositeFlightService {
    ballista: BallistaFlightService,
    spice: SpiceFlightService,
}

impl CompositeFlightService {
    /// Creates a new composite Flight service.
    #[must_use]
    pub fn new(spice: SpiceFlightService) -> Self {
        Self {
            ballista: BallistaFlightService::new(),
            spice,
        }
    }

    /// Checks if a ticket is a Ballista-format ticket by attempting to decode it.
    fn is_ballista_ticket(ticket: &Ticket) -> bool {
        decode_protobuf(&ticket.ticket).is_ok()
    }
}

#[tonic::async_trait]
impl FlightService for CompositeFlightService {
    type HandshakeStream = BoxedFlightStream<HandshakeResponse>;
    type ListFlightsStream = BoxedFlightStream<FlightInfo>;
    type DoGetStream = BoxedFlightStream<FlightData>;
    type DoPutStream = BoxedFlightStream<PutResult>;
    type DoActionStream = BoxedFlightStream<arrow_flight::Result>;
    type ListActionsStream = BoxedFlightStream<ActionType>;
    type DoExchangeStream = BoxedFlightStream<FlightData>;

    // These methods are unimplemented by `BallistaFlightService`.
    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        self.spice.handshake(request).await
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        self.spice.list_flights(request).await
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.spice.get_flight_info(request).await
    }

    async fn poll_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        self.spice.poll_flight_info(request).await
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        self.spice.get_schema(request).await
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        self.spice.do_put(request).await
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        self.spice.do_exchange(request).await
    }

    // These methods are implemented by both `BallistaFlightService` and `SpiceFlightService`.
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        // Check if this is a Ballista-format ticket (FetchPartition)
        if Self::is_ballista_ticket(request.get_ref()) {
            return self.ballista.do_get(request).await;
        }
        // Otherwise route to Spice for SQL/FlightSQL
        self.spice.do_get(request).await
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        // Route based. BallistaFlightService only handles IO_BLOCK_TRANSPORT actions.
        if request.get_ref().r#type == "IO_BLOCK_TRANSPORT" {
            return self.ballista.do_action(request).await;
        }
        // All other actions go to Spice
        self.spice.do_action(request).await
    }

    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let spice = self.spice.list_actions(Request::new(Empty {})).await?;
        let ballista = self.ballista.list_actions(request).await?;

        Ok(Response::new(Box::pin(futures::stream::select(
            spice.into_inner(),
            ballista.into_inner(),
        )) as Self::ListActionsStream))
    }
}
