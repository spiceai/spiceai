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

//! Requests without credentials on the Flight listener of a runtime without `runtime.auth`.

use std::{
    sync::Arc,
    task::{Context, Poll},
};

use runtime_auth::{AnonymousAccess, AuthRequestContext};
use tower::{Layer, Service};

/// Gives a request without credentials the access [`AnonymousAccess`] decides for this
/// listener: the local operator's principal on a loopback-only listener (unless the request
/// carries forwarding metadata), and none on any other. Runs after the mTLS layer, so a
/// client certificate's principal stands. With `runtime.auth` it does nothing: authentication
/// refuses a request without credentials.
#[derive(Clone, Copy)]
pub struct AnonymousAccessLayer {
    access: Option<AnonymousAccess>,
}

impl AnonymousAccessLayer {
    /// `access` is the listener's, or `None` on a runtime with `runtime.auth`.
    #[must_use]
    pub fn new(access: Option<AnonymousAccess>) -> Self {
        Self { access }
    }
}

impl<S> Layer<S> for AnonymousAccessLayer {
    type Service = AnonymousAccessMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AnonymousAccessMiddleware {
            inner,
            access: self.access,
        }
    }
}

#[derive(Clone)]
pub struct AnonymousAccessMiddleware<S> {
    inner: S,
    access: Option<AnonymousAccess>,
}

impl<S, ReqBody> Service<http::Request<ReqBody>> for AnonymousAccessMiddleware<S>
where
    S: Service<http::Request<ReqBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        if let Some(access) = self.access
            && let Some(context) = req
                .extensions()
                .get::<Arc<dyn AuthRequestContext + Send + Sync>>()
        {
            access.apply(req.headers(), context.as_ref());
        }
        self.inner.call(req)
    }
}
