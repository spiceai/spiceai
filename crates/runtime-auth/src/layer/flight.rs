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

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::{AuthRequestContext, AuthVerdict, FlightBasicAuth};
use base64::{prelude::BASE64_STANDARD, Engine};
use pin_project::pin_project;
use tonic::{
    metadata::{MetadataMap, GRPC_CONTENT_TYPE},
    Status,
};
use tower::{Layer, Service};

/// The handshake request for Flight basic auth is a base64 encoded token that is calculated like:
///
/// ```rust,no_run
/// let val = BASE64_STANDARD.encode(format!("{username}:{password}"));
/// let val = format!("Basic {val}");
/// ```
///
/// The returned token is used on subsequent requests as the bearer token.
///
/// # Errors
///
/// Returns an error if the authorization header is missing, the header is not a valid bearer token, or the credentials are invalid.
pub fn validate_basic_auth_handshake(
    metadata: &MetadataMap,
    basic_auth: Option<&Arc<dyn FlightBasicAuth + Send + Sync>>,
) -> Result<Option<String>, Status> {
    let Some(basic_auth) = basic_auth else {
        return Ok(None);
    };

    let auth_value = get_authorization_value(metadata, "Basic")?;
    let Ok(decoded_auth) = BASE64_STANDARD.decode(auth_value) else {
        return Err(Status::unauthenticated("Invalid handshake request"));
    };
    let Ok(decoded_auth_str) = String::from_utf8(decoded_auth) else {
        return Err(Status::unauthenticated("Invalid handshake request"));
    };

    let [username, password] = decoded_auth_str.splitn(2, ':').collect::<Vec<&str>>()[..2] else {
        return Err(Status::unauthenticated("Invalid credentials"));
    };
    match basic_auth.validate(username, password) {
        Ok(token) => Ok(Some(token)),
        Err(_) => Err(Status::unauthenticated("Invalid credentials")),
    }
}

fn get_authorization_value<'a>(
    metadata: &'a MetadataMap,
    prefix: &'static str,
) -> Result<&'a str, Status> {
    let Some(auth_header) = metadata.get("authorization") else {
        return Err(Status::unauthenticated("Missing authorization header"));
    };
    let Ok(auth_header_str) = auth_header.to_str() else {
        return Err(Status::unauthenticated("Invalid authorization header"));
    };
    let auth_header_split = auth_header_str.splitn(2, ' ').collect::<Vec<&str>>();
    if auth_header_split.len() != 2 || auth_header_split[0] != prefix {
        return Err(Status::unauthenticated("Invalid authorization header"));
    }
    Ok(auth_header_split[1])
}

#[derive(Clone)]
pub struct BasicAuthLayer {
    auth_verifier: Option<Arc<dyn FlightBasicAuth + Send + Sync>>,
}

impl BasicAuthLayer {
    #[must_use]
    pub fn new(auth_verifier: Option<Arc<dyn FlightBasicAuth + Send + Sync>>) -> Self {
        Self { auth_verifier }
    }
}

impl<S> Layer<S> for BasicAuthLayer {
    type Service = BasicAuthMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BasicAuthMiddleware {
            inner,
            auth_verifier: self.auth_verifier.clone(),
        }
    }
}

#[derive(Clone)]
pub struct BasicAuthMiddleware<S> {
    inner: S,
    auth_verifier: Option<Arc<dyn FlightBasicAuth + Send + Sync>>,
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for BasicAuthMiddleware<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ResBody: Default,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let Some(auth_verifier) = &self.auth_verifier else {
            return ResponseFuture::future(self.inner.call(req));
        };
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        // Tonic requests do not preserve the URI, HTTP version, and
        // HTTP method of the HTTP request, so we extract them here and then add them back in below.
        let uri = req.uri().clone();

        // If this request is for the handshake endpoint, we need to skip the auth check.
        if uri.path() == "/arrow.flight.protocol.FlightService/Handshake" {
            return ResponseFuture::future(inner.call(req));
        }

        let method = req.method().clone();
        let version = req.version();
        let req = tonic::Request::from_http(req);

        let bearer_token = match get_authorization_value(req.metadata(), "Bearer") {
            Ok(bearer_token) => bearer_token,
            Err(e) => return ResponseFuture::status(e),
        };

        match auth_verifier.is_valid(bearer_token) {
            Ok(AuthVerdict::Allow(principal)) => {
                if let Some(auth_context) = req
                    .extensions()
                    .get::<Arc<dyn AuthRequestContext + Send + Sync>>()
                {
                    if let Err(e) = auth_context.set_auth_principal(principal) {
                        tracing::error!(
                            "Failed to associate authentication information with the request: {e}"
                        );
                    }
                } else {
                    tracing::error!("Failed to associate authentication information with the request: the flight request is missing an authentication context.");
                }

                let (metadata, extensions, msg) = req.into_parts();

                // Tonic has an `into_http` method that does this, but its private.
                let mut request = http::Request::new(msg);
                *request.version_mut() = version;
                *request.method_mut() = method;
                *request.uri_mut() = uri;
                *request.headers_mut() = metadata.into_headers();
                *request.extensions_mut() = extensions;
                ResponseFuture::future(inner.call(request))
            }
            Ok(AuthVerdict::Deny) => {
                ResponseFuture::status(Status::unauthenticated("Invalid credentials"))
            }
            Err(e) => ResponseFuture::status(Status::internal(e.to_string())),
        }
    }
}

#[pin_project]
#[derive(Debug)]
pub struct ResponseFuture<F> {
    #[pin]
    kind: Kind<F>,
}

impl<F> ResponseFuture<F> {
    fn future(future: F) -> Self {
        Self {
            kind: Kind::Future(future),
        }
    }

    fn status(status: Status) -> Self {
        Self {
            kind: Kind::Status(Some(status)),
        }
    }
}

#[pin_project(project = KindProj)]
#[derive(Debug)]
enum Kind<F> {
    Future(#[pin] F),
    Status(Option<Status>),
}

impl<F, E, ResBody> Future for ResponseFuture<F>
where
    F: Future<Output = Result<http::Response<ResBody>, E>>,
    ResBody: Default,
{
    type Output = Result<http::Response<ResBody>, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().kind.project() {
            KindProj::Future(future) => future.poll(cx),
            KindProj::Status(status) => {
                let response = match status.take() {
                    Some(status) => {
                        let mut response = http::Response::new(ResBody::default());
                        response
                            .headers_mut()
                            .insert(http::header::CONTENT_TYPE, GRPC_CONTENT_TYPE);
                        let _ = status.add_header(response.headers_mut());
                        response
                    }
                    None => return Poll::Pending,
                };
                Poll::Ready(Ok(response))
            }
        }
    }
}
