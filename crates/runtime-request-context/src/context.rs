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
#![allow(clippy::missing_errors_doc)]

use super::{CacheControl, CacheKeyType, Protocol, UserAgent, baggage};
use crate::TraceParent;
use app::App;
use futures::{Stream, StreamExt};
use http::HeaderMap;
use opentelemetry::KeyValue;
use regex::Regex;
use runtime_auth::{AuthPrincipalRef, AuthRequestContext};
use spicepod::component::runtime::UserAgentCollection;
use std::sync::atomic::Ordering;
use std::{
    any::TypeId,
    collections::HashMap,
    future::Future,
    marker::PhantomData,
    sync::{
        Arc, LazyLock, OnceLock, RwLock,
        atomic::{AtomicI16, AtomicU8},
    },
};

static HTTP_DIMENSIONS: OnceLock<Vec<KeyValue>> = OnceLock::new();
static FLIGHT_DIMENSIONS: OnceLock<Vec<KeyValue>> = OnceLock::new();
static FLIGHTSQL_DIMENSIONS: OnceLock<Vec<KeyValue>> = OnceLock::new();
static INTERNAL_DIMENSIONS: OnceLock<Vec<KeyValue>> = OnceLock::new();

type Extensions = HashMap<TypeId, Arc<dyn Extension + Send + Sync>>;

pub struct RequestContext {
    // Use an AtomicU8 to allow updating the protocol without locking
    protocol: AtomicU8,
    cache_control: CacheControl,
    client_supplied_cache_key: Option<String>,
    dimensions: Vec<KeyValue>,
    auth_principal: OnceLock<AuthPrincipalRef>,
    extensions: RwLock<Extensions>,
    trace_parent: Option<TraceParent>,
    nested_query_level: AtomicI16,
}

#[async_trait::async_trait]
pub trait Extension: std::any::Any + Send + Sync {
    async fn load(&self) {
        // no-op
    }

    fn as_any(&self) -> &dyn std::any::Any;
}

tokio::task_local! {
    static REQUEST_CONTEXT: Arc<RequestContext>;
}

/// An internal request context that is used outside the context of a client request.
static INTERNAL_REQUEST_CONTEXT: LazyLock<Arc<RequestContext>> =
    LazyLock::new(|| Arc::new(RequestContext::builder(Protocol::Internal).build()));

static CLIENT_CACHE_KEY_REGEX: LazyLock<Regex> =
    LazyLock::new(|| match Regex::new(r"^([\w-]{1,128})$") {
        Ok(compiled) => compiled,
        Err(e) => unreachable!("Unable to compile regexp: {}", e),
    });

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

    /// **UNSAFE: Use `RequestContext::current` instead.**
    ///
    /// Returns the current request context, or an internal context if this is called outside of a request.
    ///
    /// # Safety
    /// This method is unsafe and should not be used in most cases. It allows access to the request context from synchronous code,
    /// which can easily lead to subtle bugs and undefined behavior if the context is not actually present.
    /// Always prefer using [`RequestContext::current`] with an [`AsyncMarker`] in async code to ensure correct context handling.
    #[must_use]
    pub unsafe fn current_sync() -> Arc<Self> {
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

    /// Wraps provided stream with the current request context.
    pub fn scope_stream<S>(self: Arc<Self>, stream: S) -> impl Stream<Item = S::Item>
    where
        S: Stream,
    {
        let pinned = Box::pin(stream);
        futures::stream::unfold((pinned, self), |(mut stream, ctx)| {
            let ctx_clone = Arc::clone(&ctx);
            ctx_clone.scope(async move { stream.next().await.map(|item| (item, (stream, ctx))) })
        })
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
    pub fn to_protocol_dimensions(&self) -> &'static [KeyValue] {
        let protocol = self.protocol();
        match protocol {
            Protocol::Http => {
                HTTP_DIMENSIONS.get_or_init(|| vec![KeyValue::new("protocol", protocol.as_str())])
            }
            Protocol::Flight => {
                FLIGHT_DIMENSIONS.get_or_init(|| vec![KeyValue::new("protocol", protocol.as_str())])
            }
            Protocol::FlightSQL => FLIGHTSQL_DIMENSIONS
                .get_or_init(|| vec![KeyValue::new("protocol", protocol.as_str())]),
            Protocol::Internal => INTERNAL_DIMENSIONS
                .get_or_init(|| vec![KeyValue::new("protocol", protocol.as_str())]),
            Protocol::Invalid => &[],
        }
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

    #[must_use]
    pub fn client_supplied_cache_key(&self) -> &Option<String> {
        &self.client_supplied_cache_key
    }

    #[must_use]
    pub fn trace_parent(&self) -> &Option<TraceParent> {
        &self.trace_parent
    }

    pub fn extension<T>(&self) -> Option<T>
    where
        T: Extension + Clone,
    {
        let extensions = self.extensions.read().ok()?;
        let type_id = TypeId::of::<T>();
        extensions
            .get(&type_id)?
            .as_any()
            .downcast_ref::<T>()
            .cloned()
    }

    pub fn insert_extension<T: Extension + Send + Sync>(&self, extension: T) {
        if let Ok(mut extensions) = self.extensions.write() {
            extensions.insert(extension.type_id(), Arc::new(extension));
        }
    }

    pub async fn load_extensions(&self) {
        // Cannot hold `RwLockReadGuard` across async boundary.
        let extensions = {
            let Ok(guard) = self.extensions.read() else {
                return;
            };
            guard.values().cloned().collect::<Vec<_>>()
        };

        for ext in extensions {
            ext.load().await;
        }
    }

    pub fn entered_top_level_query(&self) -> bool {
        self.nested_query_level.fetch_add(1, Ordering::Relaxed) == 0
    }

    pub fn exited_top_level_query(&self) -> bool {
        self.nested_query_level.fetch_add(-1, Ordering::Relaxed) == 1
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

    fn auth_principal(&self) -> Option<&AuthPrincipalRef> {
        self.auth_principal.get()
    }
}

pub struct RequestContextBuilder {
    protocol: Protocol,
    cache_control: CacheControl,
    client_supplied_cache_key: Option<String>,
    app: Option<Arc<App>>,
    user_agent: UserAgent,
    baggage: Vec<KeyValue>,
    extensions: Extensions,
    trace_parent: Option<TraceParent>,
}

impl RequestContextBuilder {
    #[must_use]
    pub fn new(protocol: Protocol) -> Self {
        Self {
            protocol,
            cache_control: CacheControl::Cache(CacheKeyType::Default),
            client_supplied_cache_key: None,
            app: None,
            user_agent: UserAgent::Absent,
            baggage: vec![],
            extensions: Extensions::default(),
            trace_parent: None,
        }
    }

    #[must_use]
    pub fn with_extension(mut self, extension: impl Extension) -> Self {
        self.extensions
            .insert(extension.type_id(), Arc::new(extension));
        self
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
        self.client_supplied_cache_key = match self.cache_control {
            CacheControl::Cache(CacheKeyType::ClientSupplied) => headers
                .get("Spice-Cache-Key")
                .and_then(|h| h.to_str().ok())
                .map(str::to_string),
            _ => None,
        };

        self.baggage.extend(baggage::from_headers(headers));

        match super::extract_trace_parent(headers) {
            Ok(trace_parent) => {
                self.trace_parent = trace_parent;
            }
            Err(e) => {
                tracing::warn!("Received invalid `traceparent` HTTP header: {e}");
            }
        }

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
    pub fn with_client_supplied_cache_key(mut self, cache_key: Option<String>) -> Self {
        self.client_supplied_cache_key = cache_key;
        self
    }

    #[must_use]
    pub fn with_baggage(mut self, baggage: Vec<KeyValue>) -> Self {
        self.baggage = baggage;
        self
    }

    #[must_use]
    pub fn with_trace_parent(mut self, trace_parent: Option<TraceParent>) -> Self {
        self.trace_parent = trace_parent;
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

        let user_cache_key = self
            .client_supplied_cache_key
            .and_then(Self::sanitize_cache_key);

        // Apply the runtime parameter `runtime.results_cache.cache_key_type` to the cache control if set.
        let cache_control = match self.cache_control {
            CacheControl::Cache(CacheKeyType::Default) => {
                let cache_key_type = CacheKeyType::from_app_runtime(self.app.as_ref());
                CacheControl::Cache(cache_key_type)
            }
            CacheControl::CacheWithStaleWhileRevalidate(CacheKeyType::Default, duration) => {
                let cache_key_type = CacheKeyType::from_app_runtime(self.app.as_ref());
                CacheControl::CacheWithStaleWhileRevalidate(cache_key_type, duration)
            }
            // If sanitized out, fall back to default
            CacheControl::Cache(CacheKeyType::ClientSupplied) if user_cache_key.is_none() => {
                CacheControl::Cache(CacheKeyType::Default)
            }
            CacheControl::CacheWithStaleWhileRevalidate(CacheKeyType::ClientSupplied, duration)
                if user_cache_key.is_none() =>
            {
                CacheControl::CacheWithStaleWhileRevalidate(CacheKeyType::Default, duration)
            }
            cache_control => cache_control,
        };

        RequestContext {
            protocol: AtomicU8::new(self.protocol as u8),
            cache_control,
            client_supplied_cache_key: user_cache_key,
            dimensions,
            auth_principal: OnceLock::new(),
            extensions: RwLock::new(self.extensions),
            trace_parent: self.trace_parent,
            nested_query_level: AtomicI16::new(0),
        }
    }

    fn sanitize_cache_key(key: String) -> Option<String> {
        if CLIENT_CACHE_KEY_REGEX.is_match(&key) {
            Some(key)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use http::{HeaderMap, HeaderValue};

    use crate::{CacheControl, CacheKeyType, Protocol, RequestContextBuilder};

    #[test]
    fn test_bind_client_supplied_cache_key() {
        let mut headers = HeaderMap::new();
        headers.append("cache-control", HeaderValue::from_static("cache"));

        // Test user-provided cache key
        headers.append("Spice-Cache-Key", HeaderValue::from_static("foo"));
        let ctx_happy_path = RequestContextBuilder::new(Protocol::Http)
            .from_headers(&headers)
            .build();

        assert_eq!(
            ctx_happy_path.cache_control,
            CacheControl::Cache(CacheKeyType::ClientSupplied)
        );
        assert_eq!(
            ctx_happy_path.client_supplied_cache_key,
            Some(String::from("foo"))
        );

        // Test invalid user cache key falling back to default behavior
        headers.remove("Spice-Cache-Key");
        headers.append("Spice-Cache-Key", HeaderValue::from_static("foo$$"));

        let ctx_bad_user_key = RequestContextBuilder::new(Protocol::Http)
            .from_headers(&headers)
            .build();

        assert_eq!(
            ctx_bad_user_key.cache_control,
            CacheControl::Cache(CacheKeyType::Default)
        );
        assert_eq!(ctx_bad_user_key.client_supplied_cache_key, None);
    }
}
