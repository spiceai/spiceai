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

use crate::{AuthVerdict, HttpAuth};
use axum::{
    body::Body,
    http::StatusCode,
    http::{Request, Response},
};
use futures::future::BoxFuture;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::{Layer, Service};

#[derive(Clone)]
pub struct AuthLayer {
    auth_verifier: Arc<dyn HttpAuth + Send + Sync>,
}

impl AuthLayer {
    pub fn new(auth_verifier: Arc<dyn HttpAuth + Send + Sync>) -> Self {
        Self { auth_verifier }
    }
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthMiddleware {
            inner,
            auth_verifier: Arc::clone(&self.auth_verifier),
        }
    }
}

#[derive(Clone)]
pub struct AuthMiddleware<S> {
    inner: S,
    auth_verifier: Arc<dyn HttpAuth + Send + Sync>,
}

impl<S> Service<Request<Body>> for AuthMiddleware<S>
where
    S: Service<Request<Body>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Response<Body>, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let auth_verifier = Arc::clone(&self.auth_verifier);
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let (parts, body) = req.into_parts();

            match auth_verifier.http_verify(&parts) {
                Ok(AuthVerdict::Allow(_)) => inner.call(Request::from_parts(parts, body)).await,
                Ok(AuthVerdict::Deny) => Ok(unauthorized_response()),
                Err(e) => {
                    tracing::error!("{e}");
                    Ok(internal_server_error_response())
                }
            }
        })
    }
}

fn unauthorized_response() -> Response<Body> {
    match Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .body(Body::from("Unauthorized"))
    {
        Ok(response) => response,
        Err(e) => panic!("Failed to build unauthorized response: {e}"),
    }
}

fn internal_server_error_response() -> Response<Body> {
    match Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(Body::from("Internal Server Error"))
    {
        Ok(response) => response,
        Err(e) => panic!("Failed to build internal server error response: {e}"),
    }
}
