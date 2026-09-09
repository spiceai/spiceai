/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Session lifecycle for the HTTP SQL API.
//!
//! A session is what makes `PREPARE` in one `POST /v1/sql` request visible to
//! the `EXECUTE` in the next. Clients create one here, name it with
//! `x-session-id` on later requests, and delete it when done.

use axum::{
    Extension, Json,
    extract::Path,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Serialize;
use serde_json::json;

use crate::datafusion::request_context_extension::get_current_datafusion;
use crate::sessions::{SESSION_ID_HEADER, SessionStore, bearer_token};
use runtime_auth::AuthRequestContext;
use runtime_request_context::{AsyncMarker, RequestContext};

/// A session the client can name on later requests.
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SessionResponse {
    /// Send this back as the `x-session-id` header on `/v1/sql`, or as the
    /// Arrow Flight SQL bearer token.
    pub session_id: String,
    /// Seconds of inactivity after which the session and its prepared
    /// statements are dropped. Each request in the session restarts the clock.
    pub expires_in: u64,
}

/// Create a SQL session
///
/// Creates a session and returns its id. `PREPARE`, `EXECUTE` and `DEALLOCATE`
/// span requests made in the same session; without one, each `/v1/sql` request
/// runs against a context that is discarded when it ends.
///
/// The id is also a valid Arrow Flight SQL bearer token for the same runtime, so
/// one session can be shared between the HTTP and Flight endpoints.
#[cfg_attr(feature = "openapi", utoipa::path(
    post,
    path = "/v1/sessions",
    operation_id = "post_session",
    tag = "SQL",
    responses(
        (status = 201, description = "Session created", content((
            SessionResponse = "application/json",
            example = json!({ "session_id": "6f1a9a2e-1c0e-4a5d-9a6c-2f2b1d3e4f50", "expires_in": 3600 })
        ))),
        (status = 401, description = "Missing or invalid credentials")
    )
))]
pub(crate) async fn post(
    Extension(sessions): Extension<SessionStore>,
    headers: HeaderMap,
) -> Response {
    let context = RequestContext::current(AsyncMarker::new().await);
    let df = get_current_datafusion(&context);

    let owner_stable_id = context
        .auth_principal()
        .and_then(|principal| principal.stable_id())
        .map(std::borrow::Cow::into_owned);

    // The credential the new session's id will stand in for. When the caller
    // authenticated with a session id of its own, resolve through to the key
    // behind it so sessions never chain: a session is only ever a stand-in for
    // a real credential.
    let bearer_api_key = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(bearer_token)
        .map(|token| {
            sessions
                .bearer_credential(token)
                .unwrap_or_else(|| token.to_string())
        });

    let session = sessions.issue(&df.ctx, owner_stable_id, bearer_api_key);

    // The id is not logged: it is accepted as a bearer credential, so a debug
    // log carrying it is a credential in the log.
    tracing::debug!("Created a SQL session ({} live)", sessions.count());

    let body = SessionResponse {
        session_id: session.id().to_string(),
        expires_in: sessions.ttl().as_secs(),
    };

    let mut response = (StatusCode::CREATED, Json(body)).into_response();
    if let Ok(value) = session.id().parse() {
        response.headers_mut().insert(SESSION_ID_HEADER, value);
    }
    response
}

/// Delete a SQL session
///
/// Drops the session and every prepared statement it holds. Sessions also lapse
/// on their own after a period of inactivity; deleting one frees its memory
/// immediately.
#[cfg_attr(feature = "openapi", utoipa::path(
    delete,
    path = "/v1/sessions/{session_id}",
    operation_id = "delete_session",
    tag = "SQL",
    params(("session_id" = String, Path, description = "Id returned by POST /v1/sessions")),
    responses(
        (status = 204, description = "Session deleted"),
        (status = 403, description = "The session belongs to a different principal"),
        (status = 404, description = "No such session")
    )
))]
pub(crate) async fn delete(
    Extension(sessions): Extension<SessionStore>,
    Path(session_id): Path<String>,
) -> Response {
    let context = RequestContext::current(AsyncMarker::new().await);

    let Some(session) = sessions.get_issued(&session_id) else {
        return not_found(&session_id);
    };

    if !session.is_owned_by(context.auth_principal()) {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({
                "message": format!(
                    "Session '{session_id}' belongs to a different principal, so it was not \
                    deleted. Delete it with the credentials that created it. \
                    See: https://spiceai.org/docs/api/HTTP/post-sql"
                )
            })),
        )
            .into_response();
    }

    if sessions.remove(&session_id) {
        StatusCode::NO_CONTENT.into_response()
    } else {
        // Lapsed between the lookup and the removal — the caller's intent is
        // satisfied either way, but report it as gone rather than deleted.
        not_found(&session_id)
    }
}

fn not_found(session_id: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({
            "message": format!(
                "Session '{session_id}' was not found, so there was nothing to delete. It expired \
                after a period of inactivity, or it was already deleted. \
                See: https://spiceai.org/docs/api/HTTP/post-sql"
            )
        })),
    )
        .into_response()
}
