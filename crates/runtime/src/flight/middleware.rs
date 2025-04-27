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

use crate::request::{Protocol, RequestContext};
use app::App;
use governor::{
    RateLimiter,
    state::{InMemoryState, NotKeyed},
};
use runtime_auth::AuthRequestContext;
use tonic::{Status, metadata::MetadataValue};
use tower::{Layer, Service};

/// Extracts the request context from the HTTP headers and adds it to the task-local context.
#[derive(Clone)]
pub struct RequestContextLayer {
    app: Option<Arc<App>>,
}

impl RequestContextLayer {
    #[must_use]
    pub fn new(app: Option<Arc<App>>) -> Self {
        Self { app }
    }
}

impl<S> Layer<S> for RequestContextLayer {
    type Service = RequestContextMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RequestContextMiddleware {
            inner,
            app: self.app.clone(),
        }
    }
}

#[derive(Clone)]
pub struct RequestContextMiddleware<S> {
    inner: S,
    app: Option<Arc<App>>,
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for RequestContextMiddleware<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ResBody: Default,
    ReqBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<S::Response, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<ReqBody>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        let headers = req.headers();
        let request_context = Arc::new(
            RequestContext::builder(Protocol::Flight)
                .with_app_opt(self.app.clone())
                .from_headers(headers)
                .build(),
        );

        req.extensions_mut()
            .insert::<Arc<dyn AuthRequestContext + Send + Sync>>(
                Arc::clone(&request_context) as Arc<dyn AuthRequestContext + Send + Sync>
            );

        Box::pin(async move { request_context.scope(inner.call(req)).await })
    }
}

type DirectRateLimiter = RateLimiter<
    NotKeyed,
    InMemoryState,
    governor::clock::DefaultClock,
    governor::middleware::NoOpMiddleware,
>;

/// Enforces a rate limit on the number of Flight `DoPut` requests the underlying service can handle over a period of time.
#[derive(Clone)]
pub struct WriteRateLimitLayer {
    rate_limiter: Arc<DirectRateLimiter>,
}

impl WriteRateLimitLayer {
    #[must_use]
    pub fn new(rate_limiter: DirectRateLimiter) -> Self {
        Self {
            rate_limiter: Arc::new(rate_limiter),
        }
    }
}

impl<S> Layer<S> for WriteRateLimitLayer {
    type Service = WriteRateLimitMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        WriteRateLimitMiddleware::new(inner, Arc::clone(&self.rate_limiter))
    }
}

#[derive(Clone)]
pub struct WriteRateLimitMiddleware<S> {
    inner: S,
    rate_limiter: Arc<DirectRateLimiter>,
}

impl<S> WriteRateLimitMiddleware<S> {
    fn new(inner: S, rate_limiter: Arc<DirectRateLimiter>) -> Self {
        WriteRateLimitMiddleware {
            inner,
            rate_limiter,
        }
    }
}

type RateLimitResult = Result<(), Status>;
type RateLimitCheckFn = dyn Fn() -> RateLimitResult + Send + Sync;

/// A rate limit check that returns the error response on rate limit violation
#[derive(Clone)]
pub struct RateLimiterExtension {
    check: Arc<RateLimitCheckFn>,
}

impl RateLimiterExtension {
    pub fn new(check: Arc<RateLimitCheckFn>) -> Self {
        Self { check }
    }

    pub fn check_fn(&self) -> Arc<RateLimitCheckFn> {
        Arc::clone(&self.check)
    }
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for WriteRateLimitMiddleware<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ResBody: Default,
    ReqBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<S::Response, S::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<ReqBody>) -> Self::Future {
        // Apply rate limiting to the Flight DoPut only
        if req.uri().path() != "/arrow.flight.protocol.FlightService/DoPut" {
            return Box::pin(self.inner.call(req));
        }

        let rate_limiter = Arc::clone(&self.rate_limiter);

        // Create a rate limit check that the downstream service can use to check if the request should be rate limited.
        // We don't directly check the rate limit here because not all DoPut requests should be rate limited
        // (i.e. binding parameters to parameterized queries).
        let check: Arc<RateLimitCheckFn> = Arc::new(move || {
            if let Err(wait_time) = rate_limiter.check() {
                let retry_after_secs = wait_time
                    .wait_time_from(wait_time.earliest_possible())
                    .as_secs();

                tracing::trace!(
                    "Request rate-limited, must retry after {retry_after_secs} seconds."
                );

                let mut status = Status::resource_exhausted(
                    "Too many requests. Try again after {retry_after_secs} seconds.",
                );

                let header_map = status.metadata_mut();

                if let Ok(retry_after) = MetadataValue::try_from(&retry_after_secs.to_string()) {
                    header_map.insert("retry-after", retry_after);
                }

                return Err(status);
            }

            Ok(())
        });

        req.extensions_mut()
            .insert(RateLimiterExtension::new(check));

        Box::pin(self.inner.call(req))
    }
}
