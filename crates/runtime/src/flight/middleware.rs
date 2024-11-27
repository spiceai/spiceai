/*
Copyright 2024 The Spice.ai OSS Authors

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
use pin_project::pin_project;
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
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = RequestContextFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        let headers = req.headers();
        let request_context = Arc::new(
            RequestContext::builder(Protocol::Flight)
                .from_headers(headers)
                .build(),
        );

        let req: tonic::Request<ReqBody> = tonic::Request::from_http(req);

        let future = request_context.scope(inner.call(req));

        RequestContextFuture { future }
    }
}

// Define a future type that will handle the scoped context
#[pin_project]
struct RequestContextFuture<F> {
    #[pin]
    future: F,
}

impl<F, T, E> Future for RequestContextFuture<F>
where
    F: Future<Output = Result<T, E>>,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.future.poll(cx)
    }
}
