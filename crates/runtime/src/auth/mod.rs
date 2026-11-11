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
use runtime_auth::{FlightBasicAuth, GrpcAuth, HttpAuth, IdentitySource, api_key::ApiKeyAuth};
use secrecy::ExposeSecret;
use spicepod::component::access::AccessMode;
use spicepod::component::runtime::{ApiKey, ApiKeyAuth as SpicepodApiKeyAuth, ClientAuthMode};
use std::sync::Arc;
use tokio::sync::RwLock;

mod anonymous;
pub mod no_auth;

#[derive(Clone)]
pub struct EndpointAuth {
    pub http_auth: Option<Arc<dyn HttpAuth + Send + Sync>>,
    pub flight_basic_auth: Option<Arc<dyn FlightBasicAuth + Send + Sync>>,
    pub grpc_auth: Option<Arc<dyn GrpcAuth + Send + Sync>>,
    /// Where the request's auth principal originates. Computed once
    /// at startup by the binary entrypoint from the combination of
    /// `runtime.auth` and `runtime.tls.client_auth`. Threaded into
    /// the per-protocol mTLS layers so HTTP and Flight know whether
    /// to promote a verified client cert to the request's auth
    /// principal (`Channel`) or just record it as a
    /// `ChannelIdentity` for audit (`RuntimeAuth`).
    /// Defaults to [`IdentitySource::Anonymous`] so existing tests
    /// and callers that don't set it explicitly preserve today's
    /// behavior.
    pub identity_source: IdentitySource,
}

impl EndpointAuth {
    #[must_use]
    pub async fn new(secrets: Arc<RwLock<Secrets>>, app: &App) -> Self {
        let Some(auth) = app.runtime.auth.as_ref() else {
            return Self::no_auth();
        };

        if let Some(api_key_auth_config) = auth.api_key.as_ref() {
            if !api_key_auth_config.enabled {
                return Self::no_auth();
            }

            let secrets = Secrets::snapshot(&secrets).await;
            let api_key_auth = api_key_auth(&secrets, api_key_auth_config).await;
            let http_auth = Arc::clone(&api_key_auth) as Arc<dyn HttpAuth + Send + Sync>;
            let flight_basic_auth =
                Arc::clone(&api_key_auth) as Arc<dyn FlightBasicAuth + Send + Sync>;
            let grpc_auth = Arc::clone(&api_key_auth) as Arc<dyn GrpcAuth + Send + Sync>;
            return Self {
                http_auth: Some(http_auth),
                flight_basic_auth: Some(flight_basic_auth),
                grpc_auth: Some(grpc_auth),
                identity_source: IdentitySource::RuntimeAuth,
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
            identity_source: IdentitySource::Anonymous,
        }
    }

    /// Override the [`IdentitySource`] decided by [`Self::new`]. The
    /// binary entrypoint calls this after computing the effective
    /// `client_auth` mode: `client_auth: required` with no
    /// `runtime.auth` flips an `Anonymous` posture into
    /// [`IdentitySource::Channel`] (mTLS-as-identity).
    /// `runtime.auth` plus `client_auth: required` stays
    /// [`IdentitySource::RuntimeAuth`] (mTLS-as-channel) — the cert
    /// only contributes a `ChannelIdentity` for audit.
    #[must_use]
    pub fn with_identity_source(mut self, source: IdentitySource) -> Self {
        self.identity_source = source;
        self
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

    /// The startup warning for an `app` whose data any caller can change, or `None` when this
    /// runtime authenticates callers or nothing in `app` is writable. A dataset or catalog with
    /// write access is writable, and so is an accelerated dataset, through its refresh and CDC
    /// endpoints.
    #[must_use]
    pub fn unauthenticated_writes_warning(
        &self,
        app: &App,
        client_auth_mode: ClientAuthMode,
    ) -> Option<String> {
        if self.http_auth.is_some() || client_auth_mode == ClientAuthMode::Required {
            return None;
        }

        let datasets = app.datasets.iter().filter_map(|dataset| {
            if dataset.access != AccessMode::Read {
                Some(format!(
                    "dataset '{}' (`access: {}`)",
                    dataset.name,
                    dataset.access.as_str()
                ))
            } else if dataset.acceleration.as_ref().is_some_and(|a| a.enabled) {
                Some(format!("dataset '{}' (accelerated)", dataset.name))
            } else {
                None
            }
        });
        let catalogs = app
            .catalogs
            .iter()
            .filter(|catalog| catalog.access != AccessMode::Read)
            .map(|catalog| {
                format!(
                    "catalog '{}' (`access: {}`)",
                    catalog.name,
                    catalog.access.as_str()
                )
            });
        let writable: Vec<String> = datasets.chain(catalogs).collect();
        if writable.is_empty() {
            return None;
        }

        Some(format!(
            "`runtime.auth` is not configured, so any caller who can reach this runtime can modify {}. Configure `runtime.auth` with a read-write API key, or restrict network access to authorized callers. See https://spiceai.org/docs/api/auth",
            writable.join(", ")
        ))
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
            ApiKey::Admin { key: _ } => ApiKey::Admin {
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
        builder.field("identity_source", &self.identity_source);
        builder.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use app::AppBuilder;
    use axum::http::request::Builder;
    use runtime_auth::AuthVerdict;
    use spicepod::acceleration::Acceleration;
    use spicepod::component::catalog::Catalog;
    use spicepod::component::dataset::Dataset;
    use spicepod::component::runtime::{Auth, Runtime as SpicepodRuntime};

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

    fn dataset(name: &str, access: AccessMode, accelerated: bool) -> Dataset {
        let mut dataset = Dataset::new(format!("duckdb:{name}"), name);
        dataset.access = access;
        dataset.acceleration = accelerated.then(Acceleration::default);
        dataset
    }

    fn catalog(name: &str, access: AccessMode) -> Catalog {
        let mut catalog = Catalog::new(format!("unity_catalog:{name}"), name.to_string());
        catalog.access = access;
        catalog
    }

    /// One writable dataset, one accelerated dataset, one writable catalog, and a read-only
    /// dataset and catalog.
    fn writable_app() -> AppBuilder {
        AppBuilder::new("test")
            .with_dataset(dataset("orders", AccessMode::ReadWrite, false))
            .with_dataset(dataset("trips", AccessMode::Read, true))
            .with_dataset(dataset("lookup", AccessMode::Read, false))
            .with_catalog(catalog("lake", AccessMode::ReadWriteCreate))
            .with_catalog(catalog("reference", AccessMode::Read))
    }

    fn with_api_key_auth(app: AppBuilder) -> App {
        app.with_runtime(SpicepodRuntime {
            auth: Some(Auth {
                api_key: Some(SpicepodApiKeyAuth {
                    enabled: true,
                    keys: vec![ApiKey::parse_str("rw-key:rw")],
                }),
            }),
            ..SpicepodRuntime::default()
        })
        .build()
    }

    async fn warning_for(app: &App, client_auth_mode: ClientAuthMode) -> Option<String> {
        let secrets = Arc::new(RwLock::new(Secrets::new()));
        EndpointAuth::new(secrets, app)
            .await
            .unauthenticated_writes_warning(app, client_auth_mode)
    }

    #[tokio::test]
    async fn warns_when_callers_can_write_without_auth() {
        let app = writable_app().build();
        assert_eq!(
            warning_for(&app, ClientAuthMode::None).await.as_deref(),
            Some(
                "`runtime.auth` is not configured, so any caller who can reach this runtime can modify dataset 'orders' (`access: read_write`), dataset 'trips' (accelerated), catalog 'lake' (`access: read_write_create`). Configure `runtime.auth` with a read-write API key, or restrict network access to authorized callers. See https://spiceai.org/docs/api/auth"
            )
        );
    }

    #[tokio::test]
    async fn warns_for_an_accelerated_dataset_alone() {
        let app = AppBuilder::new("test")
            .with_dataset(dataset("trips", AccessMode::Read, true))
            .build();
        assert_eq!(
            warning_for(&app, ClientAuthMode::Request).await.as_deref(),
            Some(
                "`runtime.auth` is not configured, so any caller who can reach this runtime can modify dataset 'trips' (accelerated). Configure `runtime.auth` with a read-write API key, or restrict network access to authorized callers. See https://spiceai.org/docs/api/auth"
            )
        );
    }

    #[tokio::test]
    async fn no_warning_when_auth_is_configured() {
        let app = with_api_key_auth(writable_app());
        assert_eq!(warning_for(&app, ClientAuthMode::None).await, None);
    }

    #[tokio::test]
    async fn no_warning_when_client_certificates_are_required() {
        let app = writable_app().build();
        assert_eq!(warning_for(&app, ClientAuthMode::Required).await, None);
    }

    #[tokio::test]
    async fn no_warning_when_nothing_is_writable() {
        let app = AppBuilder::new("test")
            .with_dataset(dataset("lookup", AccessMode::Read, false))
            .with_catalog(catalog("reference", AccessMode::Read))
            .build();
        assert_eq!(warning_for(&app, ClientAuthMode::None).await, None);
    }
}
