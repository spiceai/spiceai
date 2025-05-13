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

use crate::request::{AsyncMarker, RequestContext};
use axum::body::Body;
use axum::http::Request;
use futures::future::BoxFuture;
use std::{
    sync::Arc,
    task::{Context, Poll},
};
use tower::{Layer, Service};

use super::DataFusion;

#[derive(Clone)]
pub struct DataFusionContextExtension {
    df: Arc<DataFusion>,
}

impl DataFusionContextExtension {
    #[must_use]
    pub fn new(df: Arc<DataFusion>) -> Self {
        Self { df }
    }

    #[must_use]
    pub fn datafusion(&self) -> Arc<DataFusion> {
        Arc::clone(&self.df)
    }
}

#[derive(Clone)]
pub struct DataFusionContextService<S> {
    df: Arc<super::DataFusion>,
    inner: S,
}

impl<S> Service<Request<Body>> for DataFusionContextService<S>
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
        let df = Arc::clone(&self.df);

        Box::pin(async move {
            let context = RequestContext::current(AsyncMarker::new().await);
            context.insert_extension(DataFusionContextExtension::new(Arc::clone(&df)));

            let mut inner_service = inner;
            inner_service.call(req).await
        })
    }
}

#[derive(Clone)]
pub struct DataFusionContextLayer {
    df: Arc<super::DataFusion>,
}

impl DataFusionContextLayer {
    #[must_use]
    pub fn new(df: Arc<DataFusion>) -> Self {
        Self { df }
    }
}

impl<S> Layer<S> for DataFusionContextLayer {
    type Service = DataFusionContextService<S>;

    fn layer(&self, service: S) -> Self::Service {
        DataFusionContextService {
            inner: service,
            df: Arc::clone(&self.df),
        }
    }
}

pub fn get_datafusion(context: &Arc<RequestContext>) -> Option<Arc<DataFusion>> {
    context
        .extension::<DataFusionContextExtension>()
        .map(|d| d.datafusion())
}
