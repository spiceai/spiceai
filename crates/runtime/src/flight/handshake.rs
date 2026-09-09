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
use runtime_auth::{AuthPrincipalRef, AuthRequestContext, AuthVerdict, FlightBasicAuth};
use runtime_request_context::{AsyncMarker, RequestContext};
use std::pin::Pin;
use std::sync::Arc;
use tonic::{
    Response, Status,
    metadata::{MetadataMap, MetadataValue},
};

use crate::datafusion::request_context_extension::get_current_datafusion;
use runtime_auth::layer::flight as flight_auth;
use telemetry::timing::TimedStream;

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

    // Bind the session to the principal that authenticated, so a leaked id is
    // not usable by anyone else.
    let owner_stable_id = session_owner(
        request_context.auth_principal(),
        auth_token.as_deref(),
        basic_auth,
    );

    let owned = owner_stable_id.is_some();

    // `auth_token` is also the credential the session id stands in for when a
    // client presents the id as its bearer token (see `SessionAwareAuth`).
    let session = session_store.issue(&datafusion.ctx, owner_stable_id, auth_token.clone());
    let session_id = session.id().to_string();

    // Same reason the HTTP session endpoint does not log its id: the id is a
    // bearer credential.
    tracing::debug!(
        authenticated = auth_token.is_some(),
        owned,
        "Created a new Flight SQL session"
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

/// The stable id to record as the session's owner, or `None` when the handshake
/// was unauthenticated.
///
/// Two things authenticate a handshake, and they become known at different
/// points. Under `client_auth` as identity the mTLS layer has already resolved
/// the peer certificate and set the principal on the request context — it runs
/// before this RPC — so that principal is authoritative. For Basic auth nothing
/// has run yet: the handshake *is* where the credential is checked, so the owner
/// has to be resolved from the token this call just validated.
///
/// Preferring the context principal is what stops an mTLS handshake from minting
/// an *unowned* session, which [`crate::sessions::SqlSession::is_owned_by`]
/// would then let any other certificate use.
fn session_owner(
    context_principal: Option<&AuthPrincipalRef>,
    auth_token: Option<&str>,
    basic_auth: Option<&Arc<dyn FlightBasicAuth + Send + Sync>>,
) -> Option<String> {
    if let Some(stable_id) = context_principal.and_then(|principal| principal.stable_id()) {
        return Some(stable_id.into_owned());
    }

    match basic_auth?.is_valid(auth_token?).ok()? {
        AuthVerdict::Allow(principal) => principal.stable_id().map(std::borrow::Cow::into_owned),
        AuthVerdict::Deny => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // `stable_id` is called below on a concrete `ApiKey` rather than through the
    // `AuthPrincipalRef` trait object, so the trait has to be in scope.
    use runtime_auth::AuthPrincipal;
    use runtime_auth::api_key::ApiKeyAuth;
    use runtime_auth::mtls::MtlsPrincipal;
    use spicepod::component::runtime::ApiKey;

    fn api_key_auth(key: &str) -> Arc<dyn FlightBasicAuth + Send + Sync> {
        Arc::new(ApiKeyAuth::new(vec![ApiKey::parse_str(key)]))
            as Arc<dyn FlightBasicAuth + Send + Sync>
    }

    fn mtls_principal(identity: &str) -> AuthPrincipalRef {
        Arc::new(MtlsPrincipal {
            identity: identity.to_string(),
            subject_dn: identity.to_string(),
            cert_fingerprint: [0u8; 32],
        }) as AuthPrincipalRef
    }

    fn stable_id_of(principal: &AuthPrincipalRef) -> String {
        principal
            .stable_id()
            .map(std::borrow::Cow::into_owned)
            .expect("this principal has a stable id")
    }

    /// Under mTLS-as-identity there is no Basic-auth token, and the principal
    /// the mTLS layer put on the request context is the only thing identifying
    /// the client. Missing it mints an unowned session, which any other
    /// certificate may then use.
    #[test]
    fn an_mtls_handshake_is_owned_by_the_certificate_principal() {
        let principal = mtls_principal("CN=client-a");

        let owner = session_owner(Some(&principal), None, None);

        assert_eq!(owner, Some(stable_id_of(&principal)));
    }

    #[test]
    fn a_basic_auth_handshake_is_owned_by_the_principal_the_token_names() {
        let auth = api_key_auth("k:rw");

        let owner = session_owner(None, Some("k"), Some(&auth));

        let expected = ApiKey::parse_str("k")
            .stable_id()
            .map(std::borrow::Cow::into_owned);
        assert_eq!(owner, expected);
    }

    /// Both present is mTLS-as-channel with `runtime.auth` also configured. The
    /// context principal is the one the rest of the request is authorized
    /// against, so the session has to agree with it.
    #[test]
    fn the_context_principal_wins_over_the_token() {
        let principal = mtls_principal("CN=client-a");
        let auth = api_key_auth("k:rw");

        let owner = session_owner(Some(&principal), Some("k"), Some(&auth));

        assert_eq!(owner, Some(stable_id_of(&principal)));
    }

    /// An unauthenticated runtime has no identity to bind a session to, which is
    /// the one case an unowned session is correct.
    #[test]
    fn an_unauthenticated_handshake_has_no_owner() {
        assert_eq!(session_owner(None, None, None), None);
        assert_eq!(session_owner(None, Some("k"), None), None);
    }

    #[test]
    fn a_token_the_validator_rejects_has_no_owner() {
        let auth = api_key_auth("k:rw");

        assert_eq!(session_owner(None, Some("wrong"), Some(&auth)), None);
    }
}
