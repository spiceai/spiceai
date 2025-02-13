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
    marker::PhantomData,
    sync::{atomic::AtomicU8, Arc, LazyLock, OnceLock},
};

use app::App;
use http::HeaderMap;
use opentelemetry::KeyValue;
use runtime_auth::{AuthPrincipalRef, AuthRequestContext};
use spicepod::component::runtime::UserAgentCollection;

use super::{baggage, CacheControl, Protocol, UserAgent};

pub struct RequestContext {
    // Use an AtomicU8 to allow updating the protocol without locking
    protocol: AtomicU8,
    cache_control: CacheControl,
    dimensions: Vec<KeyValue>,
    auth_principal: OnceLock<AuthPrincipalRef>,
}

tokio::task_local! {
    static REQUEST_CONTEXT: Arc<RequestContext>;
}

/// An internal request context that is used outside the context of a client request.
static INTERNAL_REQUEST_CONTEXT: LazyLock<Arc<RequestContext>> =
    LazyLock::new(|| Arc::new(RequestContext::builder(Protocol::Internal).build()));

#[derive(Copy, Clone)]
pub struct AsyncMarker {
    marker: PhantomData<()>,
}

impl AsyncMarker {
    // This can only be called in async contexts due to .await
    #[must_use]
    #[allow(clippy::unused_async)]
    pub async fn new() -> Self {
        AsyncMarker {
            marker: PhantomData,
        }
    }
}

impl RequestContext {
    #[must_use]
    pub fn builder(protocol: Protocol) -> RequestContextBuilder {
        RequestContextBuilder::new(protocol)
    }

    /// Returns the current request context, or an internal context if this is called outside of a request.
    ///
    /// The `AsyncMarker` is required because this function MUST only be called from asynchronous code.
    ///
    /// Usage:
    /// ```rust,no_run
    /// let ctx = RequestContext::current(AsyncMarker::new().await);
    /// ```
    ///
    /// Additionally, the request context is lost on `tokio::spawn` - to keep the context across a spawned task boundary,
    /// wrap the asynchronous code in a `scope` call.
    ///
    /// ```rust,no_run
    /// let ctx = RequestContext::current(AsyncMarker::new().await);
    /// tokio::spawn(
    ///     ctx.scope(async move {
    ///             // ...
    ///         })
    /// );
    /// ```
    #[must_use]
    pub fn current(_marker: AsyncMarker) -> Arc<Self> {
        REQUEST_CONTEXT
            .try_with(Arc::clone)
            .ok()
            .unwrap_or_else(|| Arc::clone(&INTERNAL_REQUEST_CONTEXT))
    }

    /// Runs the provided future with the current request context.
    pub async fn scope<F>(self: Arc<Self>, f: F) -> F::Output
    where
        F: Future,
    {
        REQUEST_CONTEXT.scope(self, f).await
    }

    /// Retries the provided future from the closure `r` times until it fails or succeeds.
    pub async fn scope_retry<F, Fut, T, E>(self: Arc<Self>, r: u16, f: F) -> Fut::Output
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let mut try_count = 0;
        loop {
            let fut = f();
            match REQUEST_CONTEXT.scope(Arc::clone(&self), fut).await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    try_count += 1;
                    if try_count >= r {
                        return Err(e);
                    }
                }
            }
        }
    }

    #[must_use]
    pub fn to_dimensions(&self) -> Vec<KeyValue> {
        let mut dimensions = vec![KeyValue::new("protocol", self.protocol().as_str())];
        dimensions.extend(self.dimensions.iter().cloned());
        dimensions
    }

    #[must_use]
    pub fn protocol(&self) -> Protocol {
        Protocol::from(self.protocol.load(std::sync::atomic::Ordering::Relaxed))
    }

    pub fn update_protocol(&self, protocol: Protocol) {
        self.protocol
            .store(protocol as u8, std::sync::atomic::Ordering::Relaxed);
    }

    #[must_use]
    pub fn cache_control(&self) -> CacheControl {
        self.cache_control
    }
}

impl AuthRequestContext for RequestContext {
    fn set_auth_principal(
        &self,
        auth_principal: AuthPrincipalRef,
    ) -> Result<(), super::GenericError> {
        self.auth_principal
            .set(auth_principal)
            .map_err(|_| "Failed to set auth principal".into())
    }

    #[must_use]
    fn auth_principal(&self) -> Option<&AuthPrincipalRef> {
        self.auth_principal.get()
    }
}

pub struct RequestContextBuilder {
    protocol: Protocol,
    cache_control: CacheControl,
    app: Option<Arc<App>>,
    user_agent: UserAgent,
    baggage: Vec<KeyValue>,
}

impl RequestContextBuilder {
    #[must_use]
    pub fn new(protocol: Protocol) -> Self {
        Self {
            protocol,
            cache_control: CacheControl::Cache,
            app: None,
            user_agent: UserAgent::Absent,
            baggage: vec![],
        }
    }

    #[must_use]
    pub fn with_app_opt(mut self, app: Option<Arc<App>>) -> Self {
        self.app = app;
        self
    }

    #[must_use]
    pub fn from_headers(mut self, headers: &HeaderMap) -> Self {
        let user_agent_collection = self
            .app
            .as_ref()
            .map_or(UserAgentCollection::default(), |app| {
                app.user_agent_collection()
            });
        self.user_agent = match user_agent_collection {
            UserAgentCollection::Full => UserAgent::from_headers(headers),
            UserAgentCollection::Disabled => UserAgent::Absent,
        };
        self.cache_control = CacheControl::from_headers(headers);
        self.baggage.extend(baggage::from_headers(headers));
        self
    }

    #[must_use]
    pub fn with_user_agent(mut self, user_agent: UserAgent) -> Self {
        self.user_agent = user_agent;
        self
    }

    #[must_use]
    pub fn with_cache_control(mut self, cache_control: CacheControl) -> Self {
        self.cache_control = cache_control;
        self
    }

    #[must_use]
    pub fn with_baggage(mut self, baggage: Vec<KeyValue>) -> Self {
        self.baggage = baggage;
        self
    }

    #[must_use]
    pub fn baggage_mut(&mut self) -> &mut Vec<KeyValue> {
        &mut self.baggage
    }

    #[must_use]
    pub fn build(self) -> RequestContext {
        let mut dimensions = self.baggage;

        let add_runtime_dimensions = |dimensions: &mut Vec<KeyValue>| {
            dimensions.push(KeyValue::new("runtime", super::RUNTIME_NAME));
            dimensions.push(KeyValue::new("runtime_version", super::RUNTIME_VERSION));
            dimensions.push(KeyValue::new(
                "runtime_system",
                super::RUNTIME_SYSTEM.to_string(),
            ));
        };

        match self.user_agent {
            UserAgent::Absent => (),
            UserAgent::Raw(raw) => {
                dimensions.push(KeyValue::new("user_agent", UserAgent::Raw(raw).to_string()));
                add_runtime_dimensions(&mut dimensions);
            }
            UserAgent::Parsed(parsed) => {
                dimensions.push(KeyValue::new("client", Arc::clone(&parsed.client_name)));
                dimensions.push(KeyValue::new(
                    "client_version",
                    Arc::clone(&parsed.client_version),
                ));

                if let Some(client_system) = &parsed.client_system {
                    dimensions.push(KeyValue::new("client_system", Arc::clone(client_system)));
                }
                dimensions.push(KeyValue::new(
                    "user_agent",
                    UserAgent::Parsed(parsed).to_string(),
                ));
                add_runtime_dimensions(&mut dimensions);
            }
        }

        RequestContext {
            protocol: AtomicU8::new(self.protocol as u8),
            cache_control: self.cache_control,
            dimensions,
            auth_principal: OnceLock::new(),
        }
    }
}
