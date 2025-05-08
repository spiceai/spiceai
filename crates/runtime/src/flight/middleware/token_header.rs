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

use http::{Request, Response};
use token_providers::registry::TokenProviderRegistry;
use tower::{Layer, Service};

/// A Layer that injects tokens from the `Spice-Databricks-Auth` header into
/// the [`TokenProviderRegistry`] before dispatching the request.
#[derive(Clone)]
pub struct TokenProviderLayer {
    registry: Arc<TokenProviderRegistry>,
}

impl TokenProviderLayer {
    pub fn new(registry: Arc<TokenProviderRegistry>) -> Self {
        Self { registry }
    }
}

impl<S> Layer<S> for TokenProviderLayer {
    type Service = TokenProviderMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TokenProviderMiddleware {
            inner,
            registry: Arc::clone(&self.registry),
        }
    }
}

#[derive(Clone)]
pub struct TokenProviderMiddleware<S> {
    inner: S,
    registry: Arc<TokenProviderRegistry>,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for TokenProviderMiddleware<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = Response<ResBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let mut inner = self.inner.clone();
        let registry = Arc::clone(&self.registry);

        Box::pin(async move {
            for (header_name, header_value) in req.headers() {
                if header_name != "Spice-Databricks-Auth" {
                    continue;
                }
                let Ok(Some((client_id, access_token))) =
                    header_value.to_str().map(|v| v.split_once(':'))
                else {
                    continue;
                };
                if let Some(token_provider) =
                    registry.get(format!("databricks_u2m_{client_id}")).await
                {
                    token_provider.set_token(access_token.to_string());
                };
            }

            inner.call(req).await
        })
    }
}
