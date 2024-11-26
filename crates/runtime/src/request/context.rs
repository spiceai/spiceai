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
    sync::{Arc, LazyLock},
};

use app::App;
use http::HeaderMap;
use opentelemetry::KeyValue;
use spicepod::component::runtime::UserAgentCollection;

use super::{Protocol, UserAgent};

pub struct RequestContext {
    dimensions: Vec<KeyValue>,
}

tokio::task_local! {
    static REQUEST_CONTEXT: Arc<RequestContext>;
}

/// An internal request context that is used outside the context of a client request.
static INTERNAL_REQUEST_CONTEXT: LazyLock<Arc<RequestContext>> =
    LazyLock::new(|| Arc::new(RequestContext::builder(Protocol::Internal).build()));

impl RequestContext {
    pub fn builder(protocol: Protocol) -> RequestContextBuilder {
        RequestContextBuilder::new(protocol)
    }

    /// Returns the current request context, or an internal context if this is called outside of a request.
    pub fn current() -> Arc<Self> {
        REQUEST_CONTEXT
            .try_with(|ctx| Arc::clone(ctx))
            .ok()
            .unwrap_or_else(|| Arc::clone(&INTERNAL_REQUEST_CONTEXT))
    }

    /// Runs the provided future with the current request context.
    pub async fn scope<F>(self: Arc<Self>, f: F)
    where
        F: Future<Output = ()>,
    {
        REQUEST_CONTEXT.scope(self, f).await
    }

    #[must_use]
    pub fn to_dimensions(&self) -> &[KeyValue] {
        &self.dimensions
    }
}

pub struct RequestContextBuilder {
    protocol: Protocol,
    app: Option<Arc<App>>,
    user_agent: UserAgent,
}

impl RequestContextBuilder {
    pub fn new(protocol: Protocol) -> Self {
        Self {
            protocol,
            app: None,
            user_agent: UserAgent::Absent,
        }
    }

    pub fn with_app_opt(mut self, app: Arc<App>) -> Self {
        self.app = Some(app);
        self
    }

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
        self
    }

    pub fn build(self) -> RequestContext {
        let mut dimensions = vec![KeyValue::new("protocol", self.protocol.as_arc_str())];

        let add_platform_dimensions = |dimensions: &mut Vec<KeyValue>| {
            dimensions.push(KeyValue::new("platform", super::PLATFORM_NAME));
            dimensions.push(KeyValue::new("platform_version", super::PLATFORM_VERSION));
            dimensions.push(KeyValue::new(
                "platform_system",
                super::PLATFORM_SYSTEM.to_string(),
            ));
        };

        match self.user_agent {
            UserAgent::Absent => (),
            UserAgent::Raw(raw) => {
                dimensions.push(KeyValue::new("user_agent", UserAgent::Raw(raw).to_string()));
                add_platform_dimensions(&mut dimensions);
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
                add_platform_dimensions(&mut dimensions);
            }
        }

        RequestContext { dimensions }
    }
}
