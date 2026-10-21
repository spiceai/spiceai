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
/// Authentication should be enforced upstream (gRPC interceptor / middleware).
/// If an authorization credential is present, it is associated with the
/// created session so session-aware auth wrappers can re-validate it later.
pub(crate) fn handle(
    metadata: &tonic::metadata::MetadataMap,
    base_ctx: &Arc<SessionContext>,
    session_store: &SessionStore,
) -> Result<Response<HandshakeStream>, Status> {
    let credential = metadata
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(|auth| {
            auth.strip_prefix("Bearer ")
                .or_else(|| auth.strip_prefix("bearer "))
                .unwrap_or(auth)
                .to_string()
        });

    let (session_id, _ctx) = session_store.create_session(base_ctx, credential.as_deref());

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

    // Return the session ID as a Bearer token for clients that rely on the
    // authorization response header, but only when handshake included auth.
    if credential.is_some() {
        let auth_value = MetadataValue::try_from(format!("Bearer {session_id}")).map_err(|_| {
            Status::internal("session ID could not be converted to auth header value")
        })?;
        resp.metadata_mut().insert("authorization", auth_value);
    }

    Ok(resp)
}
