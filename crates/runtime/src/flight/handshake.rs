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

use arrow_flight::HandshakeResponse;
use futures::Stream;
use runtime_auth::FlightBasicAuth;
use runtime_request_context::{AsyncMarker, RequestContext};
use std::pin::Pin;
use std::sync::Arc;
use tonic::{
    Response, Status,
    metadata::{MetadataMap, MetadataValue},
};

use crate::{datafusion::request_context_extension::get_current_datafusion, timing::TimedStream};
use runtime_auth::layer::flight as flight_auth;

use super::{SessionStore, metrics::track_flight_request};

type HandshakeResponseStream =
    Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>;

/// Handles Flight SQL handshake, creating a new session and returning a session ID.
///
/// The session ID is returned both in the response payload and as an "x-session-id" header
/// for the client to use in subsequent requests.
pub(crate) async fn handle(
    metadata: &MetadataMap,
    basic_auth: Option<&Arc<dyn FlightBasicAuth + Send + Sync>>,
    session_store: &SessionStore,
) -> Result<Response<HandshakeResponseStream>, Status> {
    let start = track_flight_request("handshake", None).await;

    // Validate authentication if required
    let auth_token = flight_auth::validate_basic_auth_handshake(metadata, basic_auth)?;

    // Get the base DataFusion context from the request context
    let request_context = RequestContext::current(AsyncMarker::new().await);
    let datafusion = get_current_datafusion(&request_context);

    // Create a new session from the base context, associating it with the auth token
    let (session_id, _session_ctx) =
        session_store.create_session(&datafusion.ctx, auth_token.as_deref());

    tracing::debug!(
        "Created new Flight SQL session: {} (auth_token={:?})",
        session_id,
        auth_token
    );

    // Return the session ID in the response payload
    let result = HandshakeResponse {
        protocol_version: 0,
        payload: session_id.as_bytes().to_vec().into(),
    };
    let result = Ok(result);
    let output = TimedStream::new(futures::stream::iter(vec![result]), || start);
    let mut resp: Response<HandshakeResponseStream> = Response::new(Box::pin(output));

    // Add session ID as a header for standard session tracking
    let session_header = MetadataValue::try_from(&session_id)
        .map_err(|_| Status::internal("generated session ID could not be parsed"))?;
    resp.metadata_mut().insert("x-session-id", session_header);

    // Return session ID as the Authorization Bearer token.
    // The FlightSqlServiceClient extracts this token and uses it for all subsequent requests.
    // Using session_id (not auth_token) ensures prepared statements are isolated per session.
    // We only set this if authentication was performed (auth_token is Some), to maintain
    // backward compatibility with unauthenticated setups.
    if auth_token.is_some() {
        let auth_str = format!("Bearer {session_id}");
        let md = MetadataValue::try_from(auth_str)
            .map_err(|_| Status::internal("generated authorization could not be parsed"))?;
        resp.metadata_mut().insert("authorization", md);
    }

    Ok(resp)
}
