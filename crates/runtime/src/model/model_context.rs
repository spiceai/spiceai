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

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use axum::body::Body;
use axum::http::Request;
use futures::future::BoxFuture;
use opentelemetry::KeyValue;
use std::task::{Context, Poll};
use tower::{Layer, Service};

use runtime_request_context::{AsyncMarker, Extension, RequestContext};

#[derive(Clone)]
pub struct ModelContextExtension {
    used_tools: Arc<AtomicUsize>,
}

impl ModelContextExtension {
    #[must_use]
    pub fn new() -> Self {
        Self {
            used_tools: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[must_use]
    pub fn tools_used(&self) -> usize {
        self.used_tools.load(Ordering::SeqCst)
    }

    pub fn add_tools_used(&self, value: usize) {
        self.used_tools.fetch_add(value, Ordering::SeqCst);
    }
}

impl Default for ModelContextExtension {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct ModelContextService<S> {
    inner: S,
}

impl<S> Service<Request<Body>> for ModelContextService<S>
where
    S: Service<Request<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let inner = self.inner.clone();

        Box::pin(async move {
            let context = RequestContext::current(AsyncMarker::new().await);
            context.insert_extension(ModelContextExtension::new());

            let mut inner_service = inner;
            inner_service.call(req).await
        })
    }
}

// The layer that will apply our service
#[derive(Clone)]
pub struct ModelContextLayer;

impl<S> Layer<S> for ModelContextLayer {
    type Service = ModelContextService<S>;

    fn layer(&self, service: S) -> Self::Service {
        ModelContextService { inner: service }
    }
}

/// Emit the `ai_inference_count` metric with the `tools_used` dimension set to the number of called tools.
/// It requires the model extension to be set for the request context.
///
/// # Panics
///
/// Panics if the model extension is not found in the request context.
pub fn track_ai_inferences_with_spice_count(context: &Arc<RequestContext>) {
    if let Some(model_context) = context.extension::<ModelContextExtension>() {
        let tools_used: i64 = model_context.tools_used().try_into().unwrap_or_default();
        let dimensions = vec![KeyValue::new("tools_used", tools_used)];
        crate::metrics::telemetry::track_ai_inferences_with_spice_count(&dimensions);
    } else if cfg!(feature = "dev") {
        panic!("ModelContextExtension not found in request context");
    }
}

/// Set the `tools_used` flag in the model context extension for further metric tracking.
/// It requires the model extension to be set for the request context.
///
/// # Panics
///
/// Panics if the model extension is not found in the request context.
pub fn add_tools_used(context: &Arc<RequestContext>, value: usize) {
    if let Some(model_context) = context.extension::<ModelContextExtension>() {
        model_context.add_tools_used(value);
    } else if cfg!(feature = "dev") {
        panic!("ModelContextExtension not found in request context");
    }
}

#[async_trait::async_trait]
impl Extension for ModelContextExtension {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
