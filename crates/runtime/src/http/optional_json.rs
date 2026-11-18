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

//! An extractor for endpoints whose JSON body is optional.
//!
//! `Option<Json<T>>` decides from the `Content-Type` before it looks at the body: a
//! JSON `Content-Type` rejects an empty body as malformed JSON (400). A proxy that adds
//! `Content-Type: application/json` to a body-less `POST` turns a valid request into a
//! 400.
//!
//! [`OptionalJson`] reads the body first. An empty or whitespace-only body is `None`
//! whatever the `Content-Type`, the same as `{}` for an all-optional request type. A
//! non-empty body keeps the `Option<Json<T>>` rules:
//!
//! - JSON `Content-Type` (`application/json` or `application/*+json`): must parse as
//!   `T`, 400 when malformed and 422 when it does not fit `T`.
//! - Another `Content-Type`: 415. Accepting form or `text/plain` bodies would let a
//!   cross-site page send overrides in a request the browser does not preflight.
//! - No `Content-Type`: `None`.

use axum::{
    Json,
    body::Bytes,
    extract::{FromRequest, Request},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::de::DeserializeOwned;

/// A JSON request body that may be absent. See the module docs.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct OptionalJson<T>(pub Option<T>);

/// The `Content-Type` of the request: absent, JSON, or something else.
enum ContentType {
    Missing,
    Json,
    Other,
}

fn content_type(headers: &HeaderMap) -> ContentType {
    let Some(value) = headers.get(header::CONTENT_TYPE) else {
        return ContentType::Missing;
    };
    let Ok(value) = value.to_str() else {
        return ContentType::Other;
    };
    let essence = value
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let is_json = essence
        .strip_prefix("application/")
        .is_some_and(|subtype| subtype == "json" || subtype.ends_with("+json"));
    if is_json {
        ContentType::Json
    } else {
        ContentType::Other
    }
}

impl<T, S> FromRequest<S> for OptionalJson<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let content_type = content_type(req.headers());
        let bytes = Bytes::from_request(req, state)
            .await
            .map_err(IntoResponse::into_response)?;
        if bytes.iter().all(u8::is_ascii_whitespace) {
            return Ok(Self(None));
        }
        match content_type {
            ContentType::Missing => Ok(Self(None)),
            ContentType::Other => Err((
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "Expected request with `Content-Type: application/json`",
            )
                .into_response()),
            ContentType::Json => Json::<T>::from_bytes(&bytes)
                .map(|Json(value)| Self(Some(value)))
                .map_err(IntoResponse::into_response),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::OptionalJson;
    use crate::accelerated::refresh::RefreshOverrides;
    use axum::{
        Router,
        body::{Body, to_bytes},
        http::{Request, StatusCode, header},
        routing::post,
    };
    use tower::ServiceExt;

    /// Answers with what the extractor produced, so each test can check it.
    async fn echo(OptionalJson(overrides): OptionalJson<RefreshOverrides>) -> String {
        match overrides {
            None => "none".to_string(),
            Some(o) => format!(
                "sql={:?} mode={:?} jitter={:?}",
                o.sql, o.mode, o.max_jitter
            ),
        }
    }

    async fn send(content_type: Option<&str>, body: &'static str) -> (StatusCode, String) {
        let app = Router::new().route("/refresh", post(echo));
        let mut req = Request::builder().method("POST").uri("/refresh");
        if let Some(content_type) = content_type {
            req = req.header(header::CONTENT_TYPE, content_type);
        }
        let response = app
            .oneshot(req.body(Body::from(body)).expect("request"))
            .await
            .expect("response");
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    #[tokio::test]
    async fn no_body_no_content_type_is_none() {
        assert_eq!(send(None, "").await, (StatusCode::OK, "none".to_string()));
    }

    #[tokio::test]
    async fn empty_body_with_json_content_type_is_none() {
        assert_eq!(
            send(Some("application/json"), "").await,
            (StatusCode::OK, "none".to_string())
        );
    }

    #[tokio::test]
    async fn whitespace_body_with_json_content_type_is_none() {
        assert_eq!(
            send(Some("application/json; charset=utf-8"), " \r\n\t").await,
            (StatusCode::OK, "none".to_string())
        );
    }

    #[tokio::test]
    async fn empty_object_is_default_overrides() {
        assert_eq!(
            send(Some("application/json"), "{}").await,
            (StatusCode::OK, "sql=None mode=None jitter=None".to_string())
        );
    }

    #[tokio::test]
    async fn valid_overrides_are_read() {
        let (status, body) = send(
            Some("application/json"),
            r#"{"refresh_mode":"append","refresh_sql":"SELECT * FROM t","refresh_jitter_max":"10s"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body.contains(r#"sql=Some("SELECT * FROM t")"#), "{body}");
        assert!(body.contains("mode=Some(Append)"), "{body}");
        assert!(body.contains("jitter=Some(10s)"), "{body}");
    }

    #[tokio::test]
    async fn suffixed_json_content_type_is_read() {
        let (status, body) = send(
            Some("application/vnd.api+json"),
            r#"{"refresh_mode":"append"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body.contains("mode=Some(Append)"), "{body}");
    }

    #[tokio::test]
    async fn empty_body_with_other_content_type_is_none() {
        assert_eq!(
            send(Some("application/x-www-form-urlencoded"), "").await,
            (StatusCode::OK, "none".to_string())
        );
    }

    #[tokio::test]
    async fn body_with_other_content_type_is_415() {
        // A form or text/plain body is a request a browser sends cross-site without a
        // preflight, so it must not carry overrides.
        for content_type in ["application/x-www-form-urlencoded", "text/plain"] {
            let (status, body) = send(Some(content_type), r#"{"refresh_mode":"append"}"#).await;
            assert_eq!(
                status,
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "{content_type}: {body}"
            );
        }
    }

    #[tokio::test]
    async fn body_without_content_type_is_none() {
        // Unchanged from `Option<Json<T>>`.
        assert_eq!(
            send(None, r#"{"refresh_mode":"append"}"#).await,
            (StatusCode::OK, "none".to_string())
        );
    }

    #[tokio::test]
    async fn malformed_body_is_400() {
        let (status, body) = send(Some("application/json"), "{not json").await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(
            body.contains("Failed to parse the request body as JSON"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn wrong_shape_is_422() {
        let (status, body) = send(Some("application/json"), r#"{"refresh_mode":"bogus"}"#).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    }
}
