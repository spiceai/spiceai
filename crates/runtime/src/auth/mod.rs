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

use crate::{
    secrets::{ParamStr, Secrets},
    App,
};
use runtime_auth::{api_key::ApiKeyAuth, FlightBasicAuth, GrpcAuth, HttpAuth};
use secrecy::ExposeSecret;
use spicepod::component::runtime::{ApiKey, ApiKeyAuth as SpicepodApiKeyAuth};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct EndpointAuth {
    pub http_auth: Option<Arc<dyn HttpAuth + Send + Sync>>,
    pub flight_basic_auth: Option<Arc<dyn FlightBasicAuth + Send + Sync>>,
    pub grpc_auth: Option<Arc<dyn GrpcAuth + Send + Sync>>,
}

impl EndpointAuth {
    #[must_use]
    pub async fn new(secrets: Arc<RwLock<Secrets>>, app: &App) -> Self {
        let secrets = &*secrets.read().await;
        let Some(auth) = app.runtime.auth.as_ref() else {
            return Self::no_auth();
        };

        if let Some(api_key_auth_config) = auth.api_key.as_ref() {
            if !api_key_auth_config.enabled {
                return Self::no_auth();
            }

            let api_key_auth = api_key_auth(secrets, api_key_auth_config).await;
            let http_auth = Arc::clone(&api_key_auth) as Arc<dyn HttpAuth + Send + Sync>;
            let flight_basic_auth =
                Arc::clone(&api_key_auth) as Arc<dyn FlightBasicAuth + Send + Sync>;
            let grpc_auth = Arc::clone(&api_key_auth) as Arc<dyn GrpcAuth + Send + Sync>;
            return Self {
                http_auth: Some(http_auth),
                flight_basic_auth: Some(flight_basic_auth),
                grpc_auth: Some(grpc_auth),
            };
        }

        Self::no_auth()
    }

    #[must_use]
    pub fn no_auth() -> Self {
        Self {
            http_auth: None,
            flight_basic_auth: None,
            grpc_auth: None,
        }
    }

    #[must_use]
    pub fn with_http_auth(mut self, auth: Arc<dyn HttpAuth + Send + Sync>) -> Self {
        self.http_auth = Some(auth);
        self
    }

    #[must_use]
    pub fn with_flight_basic_auth(mut self, auth: Arc<dyn FlightBasicAuth + Send + Sync>) -> Self {
        self.flight_basic_auth = Some(auth);
        self
    }

    #[must_use]
    pub fn with_grpc_auth(mut self, auth: Arc<dyn GrpcAuth + Send + Sync>) -> Self {
        self.grpc_auth = Some(auth);
        self
    }
}

impl Default for EndpointAuth {
    fn default() -> Self {
        Self::no_auth()
    }
}

#[must_use]
async fn api_key_auth(secrets: &Secrets, api_key_auth: &SpicepodApiKeyAuth) -> Arc<ApiKeyAuth> {
    let mut keys = Vec::with_capacity(api_key_auth.keys.len());
    for key in &api_key_auth.keys {
        let secret_key = secrets
            .inject_secrets("keys", ParamStr(key.as_ref()))
            .await
            .expose_secret()
            .clone();

        let key = match key {
            ApiKey::ReadOnly { key: _ } => ApiKey::ReadOnly { key: secret_key },
            ApiKey::ReadWrite { key: _ } => ApiKey::ReadWrite { key: secret_key },
        };

        keys.push(key);
    }

    Arc::new(ApiKeyAuth::new(keys))
}

impl std::fmt::Debug for EndpointAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const PRESENT: &str = "PRESENT";
        const ABSENT: &str = "ABSENT";
        let mut builder = f.debug_struct("EndpointAuth");
        if self.http_auth.is_some() {
            builder.field("http_auth", &PRESENT);
        } else {
            builder.field("http_auth", &ABSENT);
        }
        if self.flight_basic_auth.is_some() {
            builder.field("flight_basic_auth", &PRESENT);
        } else {
            builder.field("flight_basic_auth", &ABSENT);
        }
        if self.grpc_auth.is_some() {
            builder.field("grpc_auth", &PRESENT);
        } else {
            builder.field("grpc_auth", &ABSENT);
        }
        builder.finish()
    }
}
