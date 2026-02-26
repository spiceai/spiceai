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
    App,
    secrets::{ParamStr, Secrets},
};
use runtime_auth::{FlightBasicAuth, GrpcAuth, HttpAuth, api_key::ApiKeyAuth};
use secrecy::ExposeSecret;
use spicepod::component::runtime::{ApiKey, ApiKeyAuth as SpicepodApiKeyAuth};
use std::sync::Arc;
use tokio::sync::RwLock;

mod anonymous;
pub mod no_auth;

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
        let secret_key_box = secrets.inject_secrets("keys", ParamStr(key.as_ref())).await;
        let secret_key = secret_key_box.expose_secret();

        let key = match key {
            ApiKey::ReadOnly { key: _ } => ApiKey::ReadOnly {
                key: secret_key.to_string(),
            },
            ApiKey::ReadWrite { key: _ } => ApiKey::ReadWrite {
                key: secret_key.to_string(),
            },
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::request::Builder;
    use runtime_auth::AuthVerdict;

    fn create_request_parts(api_key: Option<&str>) -> axum::http::request::Parts {
        let mut builder = Builder::new().uri("https://example.com");

        if let Some(key) = api_key {
            builder = builder.header("X-API-Key", key);
        }

        let request = builder.body(()).expect("Failed to build request");
        request.into_parts().0
    }

    #[tokio::test]
    async fn test_api_key_secret_replacement() {
        let mut secrets = Secrets::new();
        secrets
            .load_from(&[])
            .await
            .expect("to load secrets successfully");

        let secret_key = format!("TEST_API_KEY_SECRET_{}", rand::random::<u64>());
        let secret_value = "my-super-secret-api-key";

        // SAFETY: Setting environment variable for test purposes only
        unsafe { std::env::set_var(&secret_key, secret_value) };

        // Test read-write key with secret replacement
        let api_key_auth_config = SpicepodApiKeyAuth {
            enabled: true,
            keys: vec![ApiKey::parse_str(&format!("${{env:{secret_key}}}:rw"))],
        };

        let auth = api_key_auth(&secrets, &api_key_auth_config).await;

        // Verify the secret was replaced and the key works
        let parts = create_request_parts(Some(secret_value));
        let result = auth.http_verify(&parts);
        assert!(
            matches!(result, Ok(AuthVerdict::Allow(_))),
            "API key with secret replacement should authenticate successfully"
        );

        // Verify the original secret placeholder does NOT work
        let parts_with_placeholder = create_request_parts(Some(&format!("${{env:{secret_key}}}")));
        let result_placeholder = auth.http_verify(&parts_with_placeholder);
        assert!(
            matches!(result_placeholder, Ok(AuthVerdict::Deny)),
            "Unexpanded secret placeholder should be denied"
        );

        // SAFETY: Cleaning up environment variable
        unsafe { std::env::remove_var(&secret_key) };
    }

    #[tokio::test]
    async fn test_api_key_secret_replacement_read_only() {
        let mut secrets = Secrets::new();
        secrets
            .load_from(&[])
            .await
            .expect("to load secrets successfully");

        let secret_key = format!("TEST_API_KEY_RO_SECRET_{}", rand::random::<u64>());
        let secret_value = "my-readonly-api-key";

        // SAFETY: Setting environment variable for test purposes only
        unsafe { std::env::set_var(&secret_key, secret_value) };

        // Test read-only key with secret replacement (no :rw suffix)
        let api_key_auth_config = SpicepodApiKeyAuth {
            enabled: true,
            keys: vec![ApiKey::parse_str(&format!("${{env:{secret_key}}}:ro"))],
        };

        let auth = api_key_auth(&secrets, &api_key_auth_config).await;

        // Verify the secret was replaced and the key works
        let parts = create_request_parts(Some(secret_value));
        let result = auth.http_verify(&parts);
        assert!(
            matches!(result, Ok(AuthVerdict::Allow(_))),
            "Read-only API key with secret replacement should authenticate successfully"
        );

        // SAFETY: Cleaning up environment variable
        unsafe { std::env::remove_var(&secret_key) };
    }

    #[tokio::test]
    async fn test_api_key_multiple_secrets_replacement() {
        let mut secrets = Secrets::new();
        secrets
            .load_from(&[])
            .await
            .expect("to load secrets successfully");

        let secret_key_1 = format!("TEST_API_KEY_1_{}", rand::random::<u64>());
        let secret_key_2 = format!("TEST_API_KEY_2_{}", rand::random::<u64>());
        let secret_value_1 = "first-api-key";
        let secret_value_2 = "second-api-key";

        // SAFETY: Setting environment variables for test purposes only
        unsafe {
            std::env::set_var(&secret_key_1, secret_value_1);
            std::env::set_var(&secret_key_2, secret_value_2);
        };

        let api_key_auth_config = SpicepodApiKeyAuth {
            enabled: true,
            keys: vec![
                ApiKey::parse_str(&format!("${{env:{secret_key_1}}}:rw")),
                ApiKey::parse_str(&format!("${{env:{secret_key_2}}}:ro")),
            ],
        };

        let auth = api_key_auth(&secrets, &api_key_auth_config).await;

        // Verify first key works
        let parts_1 = create_request_parts(Some(secret_value_1));
        let result_1 = auth.http_verify(&parts_1);
        assert!(
            matches!(result_1, Ok(AuthVerdict::Allow(_))),
            "First API key should authenticate successfully"
        );

        // Verify second key works
        let parts_2 = create_request_parts(Some(secret_value_2));
        let result_2 = auth.http_verify(&parts_2);
        assert!(
            matches!(result_2, Ok(AuthVerdict::Allow(_))),
            "Second API key should authenticate successfully"
        );

        // SAFETY: Cleaning up environment variables
        unsafe {
            std::env::remove_var(&secret_key_1);
            std::env::remove_var(&secret_key_2);
        };
    }
}
