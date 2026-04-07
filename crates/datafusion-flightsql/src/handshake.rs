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

//! Flight SQL handshake handler.
//!
//! Creates a new per-request `SessionContext` and returns the session ID in
//! both the response payload and as an `x-session-id` metadata header.

use std::sync::Arc;

use arrow_flight::HandshakeResponse;
use datafusion::prelude::SessionContext;
use futures::Stream;
use std::pin::Pin;
use tonic::{Response, Status, metadata::MetadataValue};

use crate::SessionStore;

type HandshakeStream = Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>;

/// Handle a Flight SQL handshake request.
///
/// Ignores any credentials present in the request metadata — authentication
/// should be enforced upstream by a gRPC interceptor or Tower middleware
/// before this is called.  A new session is always created from `base_ctx`.
pub(crate) async fn handle(
    _metadata: &tonic::metadata::MetadataMap,
    base_ctx: &Arc<SessionContext>,
    session_store: &SessionStore,
) -> Result<Response<HandshakeStream>, Status> {
    let (session_id, _ctx) = session_store.create_session(base_ctx, None);

    tracing::debug!("Flight SQL: created session {session_id}");

    let result = HandshakeResponse {
        protocol_version: 0,
        payload: session_id.as_bytes().to_vec().into(),
    };

    let mut resp: Response<HandshakeStream> =
        Response::new(Box::pin(futures::stream::iter(vec![Ok(result)])));

    let header_value = MetadataValue::try_from(&session_id)
        .map_err(|_| Status::internal("session ID could not be converted to header value"))?;
    resp.metadata_mut().insert("x-session-id", header_value);

    // Return the session ID as a Bearer token so that `FlightSqlServiceClient`
    // (which reads the `authorization` response header) can use it for all
    // subsequent requests without needing custom-header support.
    let auth_value = MetadataValue::try_from(format!("Bearer {session_id}"))
        .map_err(|_| Status::internal("session ID could not be converted to auth header value"))?;
    resp.metadata_mut().insert("authorization", auth_value);

    Ok(resp)
}
