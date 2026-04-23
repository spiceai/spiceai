/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Standalone builders for cloud object stores (Azure ADLS, GCS).
//!
//! These helpers centralize the logic for constructing `object_store`
//! clients from a URL plus a parameter map. Callers can reuse them without
//! going through [`crate::registry::SpiceObjectStoreRegistry`], which is
//! important for code paths (like acceleration snapshots) that construct
//! object stores outside the `DataFusion` registry flow.
//!
//! Both builders:
//! * Seed the underlying builder from environment variables
//!   (`MicrosoftAzureBuilder::from_env` / `GoogleCloudStorageBuilder::from_env`),
//!   so standard credentials (`AZURE_STORAGE_ACCOUNT_KEY`,
//!   `AZURE_CLIENT_ID`/`AZURE_TENANT_ID`/`AZURE_FEDERATED_TOKEN_FILE`,
//!   `GOOGLE_APPLICATION_CREDENTIALS`, Workload Identity, etc.) work by default.
//! * Wire an [`SpawnedReqwestConnector`] onto the shared IO runtime to avoid
//!   blocking the main Tokio runtime on HTTP traffic.
//! * Apply optional configuration from a `params` map (auth overrides,
//!   retry/backoff tuning, proxy settings, etc.).

use std::{collections::HashMap, sync::Arc};

use datafusion::error::DataFusionError;
use object_store::{
    ClientOptions, ObjectStore, RetryConfig, azure::MicrosoftAzureBuilder,
    client::SpawnedReqwestConnector, gcp::GoogleCloudStorageBuilder,
};
use tokio::runtime::Handle;
use url::Url;

/// Build a Microsoft Azure ADLS Gen2 object store from a URL and params.
///
/// Accepted URL shapes:
/// * Fully-qualified ADLS Gen2: `abfss://<container>@<account>.dfs.core.windows.net/<path>`
/// * Simplified container-only form (used by the Azurite emulator and some
///   connector tests): `abfs://<container>/<path>` together with
///   `use_emulator=true` and/or an explicit `account` param.
///
/// Any `#key=value` URL fragment is **not** read by this function; callers
/// that encode params in the URL fragment must parse it themselves and pass
/// the result via `params`. The URL's query string is left untouched except
/// when a `sas_string` param is present, in which case it replaces the query.
///
/// Credentials are sourced from:
/// 1. `params` (explicit overrides such as `access_key`, `bearer_token`,
///    `client_id`/`client_secret`/`tenant_id`, `federated_token_file`, ...)
/// 2. Environment variables via [`MicrosoftAzureBuilder::from_env`]
///    (`AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ACCOUNT_KEY`,
///    `AZURE_STORAGE_ACCESS_KEY`, `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`,
///    `AZURE_FEDERATED_TOKEN_FILE`, ...).
/// 3. Azure Managed Identity / IMDS (fallback, only reached if the above
///    are absent).
///
/// # Errors
///
/// Returns a `DataFusionError::Configuration` if any boolean/duration param
/// fails to parse, or `DataFusionError::ObjectStore` if the underlying
/// builder rejects the resulting configuration.
pub fn build_azure_object_store<S: ::std::hash::BuildHasher>(
    url: &Url,
    params: &HashMap<String, String, S>,
    io_runtime: Handle,
) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
    let mut url = url.clone();

    let mut builder = MicrosoftAzureBuilder::from_env()
        .with_http_connector(SpawnedReqwestConnector::new(io_runtime));

    if let Some(sas) = params.get("sas_string") {
        url.set_query(Some(sas));
    }

    if let Some(use_emulator) = params.get("use_emulator") {
        let as_bool = use_emulator.parse::<bool>().map_err(|_| {
            DataFusionError::Configuration(format!(
                "{use_emulator} is not a valid boolean for use_emulator"
            ))
        })?;
        builder = builder.with_use_emulator(as_bool);
    }

    // Always seed the builder with the URL so it can extract account /
    // container / path. The Azure builder tolerates this even when
    // `with_use_emulator(true)` is set — emulator mode is what callers like
    // the Azurite-based ABFS integration tests rely on.
    builder = builder.with_url(url.to_string());

    if let Some(account) = params.get("account") {
        builder = builder.with_account(account);
    }
    if let Some(container_name) = params.get("container_name") {
        builder = builder.with_container_name(container_name);
    }
    if let Some(access_key) = params.get("access_key") {
        builder = builder.with_access_key(access_key);
    }
    if let Some(bearer_token) = params.get("bearer_token") {
        builder = builder.with_bearer_token_authorization(bearer_token);
    }
    if let Some(client_id) = params.get("client_id") {
        builder = builder.with_client_id(client_id);
    }
    if let Some(client_secret) = params.get("client_secret") {
        builder = builder.with_client_secret(client_secret);
    }
    if let Some(tenant_id) = params.get("tenant_id") {
        builder = builder.with_tenant_id(tenant_id);
    }
    if let Some(endpoint) = params.get("endpoint") {
        builder = builder.with_endpoint(endpoint.clone());
    }

    if let Some(use_fabric_endpoint) = params.get("use_fabric_endpoint") {
        let as_bool = use_fabric_endpoint.parse::<bool>().map_err(|_| {
            DataFusionError::Configuration(format!(
                "{use_fabric_endpoint} is not a valid boolean for use_fabric_endpoint"
            ))
        })?;
        builder = builder.with_use_fabric_endpoint(as_bool);
    }
    if let Some(allow_http) = params.get("allow_http") {
        let as_bool = allow_http.parse::<bool>().map_err(|_| {
            DataFusionError::Configuration(format!(
                "{allow_http} is not a valid boolean for allow_http"
            ))
        })?;
        builder = builder.with_allow_http(as_bool);
    }
    if let Some(authority_host) = params.get("authority_host") {
        builder = builder.with_authority_host(authority_host);
    }

    builder = builder.with_retry(parse_retry_config(params)?);

    if let Some(proxy_url) = params.get("proxy_url") {
        builder = builder.with_proxy_url(proxy_url);
    }
    if let Some(proxy_ca_certificate) = params.get("proxy_ca_certificate") {
        builder = builder.with_proxy_ca_certificate(proxy_ca_certificate);
    }
    if let Some(proxy_excludes) = params.get("proxy_excludes") {
        builder = builder.with_proxy_excludes(proxy_excludes);
    }
    if let Some(msi_endpoint) = params.get("msi_endpoint") {
        builder = builder.with_msi_endpoint(msi_endpoint);
    }
    if let Some(federated_token_file) = params.get("federated_token_file") {
        builder = builder.with_federated_token_file(federated_token_file);
    }
    if let Some(use_cli) = params.get("use_cli") {
        let as_bool = use_cli.parse::<bool>().map_err(|_| {
            DataFusionError::Configuration(format!("{use_cli} is not a valid boolean for use_cli"))
        })?;
        builder = builder.with_use_azure_cli(as_bool);
    }
    if let Some(skip_signature) = params.get("skip_signature") {
        let as_bool = skip_signature.parse::<bool>().map_err(|_| {
            DataFusionError::Configuration(format!(
                "{skip_signature} is not a valid boolean for skip_signature"
            ))
        })?;
        builder = builder.with_skip_signature(as_bool);
    }
    if let Some(disable_tagging) = params.get("disable_tagging") {
        let as_bool = disable_tagging.parse::<bool>().map_err(|_| {
            DataFusionError::Configuration(format!(
                "{disable_tagging} is not a valid boolean for disable_tagging"
            ))
        })?;
        builder = builder.with_disable_tagging(as_bool);
    }

    let azure_store = Arc::new(
        builder
            .build()
            .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))?,
    );

    Ok(azure_store as Arc<dyn ObjectStore>)
}

/// Build a Google Cloud Storage object store.
///
/// `bucket_name` is the GCS bucket; callers should extract it from their URL
/// (typically `url.host_str()`). Params follow the same conventions as the
/// registry fragment keys: `service_account_path` / `service_account_key`,
/// `application_default_credentials`, plus retry and client-option tuning.
///
/// Credentials are sourced from:
/// 1. `params` (explicit overrides).
/// 2. Environment variables via [`GoogleCloudStorageBuilder::from_env`]
///    (`GOOGLE_SERVICE_ACCOUNT`, `GOOGLE_APPLICATION_CREDENTIALS`, ...).
///
/// When `skip_signature=true` is set, credential loading is disabled
/// (equivalent to anonymous access).
///
/// # Errors
///
/// Returns a `DataFusionError::Configuration` if any boolean/duration param
/// fails to parse, or `DataFusionError::ObjectStore` if the underlying
/// builder rejects the resulting configuration.
pub fn build_gcs_object_store<S: ::std::hash::BuildHasher>(
    bucket_name: &str,
    params: &HashMap<String, String, S>,
    io_runtime: Handle,
) -> datafusion::error::Result<Arc<dyn ObjectStore>> {
    // Check skip_signature first - if true, use new() instead of from_env() to avoid
    // automatic credential loading attempts
    let skip_signature = match params.get("skip_signature") {
        Some(value) => value.parse::<bool>().map_err(|_| {
            DataFusionError::Configuration(format!(
                "{value} is not a valid boolean for skip_signature"
            ))
        })?,
        None => false,
    };

    let mut builder = if skip_signature {
        GoogleCloudStorageBuilder::new()
            .with_skip_signature(true)
            .with_bucket_name(bucket_name)
            .with_http_connector(SpawnedReqwestConnector::new(io_runtime))
    } else {
        GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(bucket_name)
            .with_http_connector(SpawnedReqwestConnector::new(io_runtime))
    };

    let mut client_options = ClientOptions::default();

    // Service account authentication (only if not skip_signature)
    if !skip_signature {
        // Prefer explicit service_account_path, but also accept legacy aliases:
        // - service_account (canonicalized from google_service_account by Parameters::canonicalize_gcs_fragments)
        // - google_service_account (for direct compatibility if not canonicalized)
        if let Some(service_account_path) = params
            .get("service_account_path")
            .or_else(|| params.get("service_account"))
            .or_else(|| params.get("google_service_account"))
        {
            builder = builder.with_service_account_path(service_account_path);
        }
        if let Some(service_account_key) = params.get("service_account_key") {
            builder = builder.with_service_account_key(service_account_key);
        }

        // Application default credentials - use GOOGLE_APPLICATION_CREDENTIALS env var path
        // with_application_credentials takes a path to the credentials file
        if let Some(application_default_credentials) = params.get("application_default_credentials")
        {
            let as_bool = application_default_credentials.parse::<bool>().map_err(|_| {
                DataFusionError::Configuration(format!(
                    "{application_default_credentials} is not a valid boolean for application_default_credentials"
                ))
            })?;
            if as_bool {
                // Use GOOGLE_APPLICATION_CREDENTIALS environment variable if set
                if let Ok(creds_path) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
                    builder = builder.with_application_credentials(creds_path);
                }
                // If not set, the builder will attempt to use default credentials automatically
            }
        }
    }

    // Client options
    if let Some(timeout) = params.get("client_timeout") {
        client_options =
            client_options.with_timeout(fundu::parse_duration(timeout).map_err(|_| {
                DataFusionError::Configuration(format!("Unable to parse timeout: {timeout}"))
            })?);
    }

    if let Some(allow_http) = params.get("allow_http") {
        let as_bool = allow_http.parse::<bool>().map_err(|_| {
            DataFusionError::Configuration(format!(
                "{allow_http} is not a valid boolean for allow_http"
            ))
        })?;
        client_options = client_options.with_allow_http(as_bool);
    }

    builder = builder.with_retry(parse_retry_config(params)?);
    builder = builder.with_client_options(client_options);

    let gcs_store = Arc::new(
        builder
            .build()
            .map_err(|e| DataFusionError::ObjectStore(Box::new(e)))?,
    );

    Ok(gcs_store as Arc<dyn ObjectStore>)
}

/// Parse the common retry/backoff params that both Azure and GCS builders
/// accept. Unknown keys are ignored; recognized keys with values that fail
/// to parse return `DataFusionError::Configuration`.
fn parse_retry_config<S: ::std::hash::BuildHasher>(
    params: &HashMap<String, String, S>,
) -> datafusion::error::Result<RetryConfig> {
    let mut retry_config = RetryConfig::default();

    if let Some(retry_timeout) = params.get("retry_timeout") {
        let as_duration = fundu::parse_duration(retry_timeout).map_err(|_| {
            DataFusionError::Configuration(format!(
                "{retry_timeout} is not a valid duration for retry_timeout"
            ))
        })?;
        retry_config.retry_timeout = as_duration;
    }
    if let Some(max_retries) = params.get("max_retries") {
        let as_usize = max_retries.parse::<usize>().map_err(|_| {
            DataFusionError::Configuration(format!(
                "{max_retries} is not a valid usize for max_retries"
            ))
        })?;
        retry_config.max_retries = as_usize;
    }
    if let Some(backoff_initial_duration) = params.get("backoff_initial_duration") {
        let as_duration = fundu::parse_duration(backoff_initial_duration).map_err(|_| {
            DataFusionError::Configuration(format!(
                "{backoff_initial_duration} is not a valid duration for backoff_initial_duration"
            ))
        })?;
        retry_config.backoff.init_backoff = as_duration;
    }
    if let Some(backoff_max_duration) = params.get("backoff_max_duration") {
        let as_duration = fundu::parse_duration(backoff_max_duration).map_err(|_| {
            DataFusionError::Configuration(format!(
                "{backoff_max_duration} is not a valid duration for backoff_max_duration"
            ))
        })?;
        retry_config.backoff.max_backoff = as_duration;
    }
    if let Some(backoff_base) = params.get("backoff_base") {
        let as_f64 = backoff_base.parse::<f64>().map_err(|_| {
            DataFusionError::Configuration(format!(
                "{backoff_base} is not a valid f64 for backoff_base"
            ))
        })?;
        retry_config.backoff.base = as_f64;
    }

    Ok(retry_config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn handle() -> Handle {
        // Tests run inside a tokio runtime via #[tokio::test], so a Handle is available.
        Handle::current()
    }

    #[tokio::test]
    async fn azure_builder_accepts_abfss_url_with_explicit_account_key() {
        let url = Url::parse("abfss://container@account.dfs.core.windows.net/prefix/")
            .expect("valid url");
        let mut params = HashMap::new();
        params.insert("account".to_string(), "account".to_string());
        params.insert(
            "access_key".to_string(),
            // base64 of "dummykey" — just needs to be valid base64 for the builder to accept it.
            "ZHVtbXlrZXk=".to_string(),
        );

        assert!(
            build_azure_object_store(&url, &params, handle()).is_ok(),
            "azure object store should build with explicit credentials"
        );
    }

    #[tokio::test]
    async fn azure_builder_accepts_emulator_url_without_account() {
        // Mirrors the connector emulator pattern: `abfs://testcontainer/...` +
        // `use_emulator=true`, no explicit account.
        let url = Url::parse("abfs://testcontainer/path/").expect("valid url");
        let mut params = HashMap::new();
        params.insert("use_emulator".to_string(), "true".to_string());

        assert!(
            build_azure_object_store(&url, &params, handle()).is_ok(),
            "azure emulator object store should build without explicit account"
        );
    }

    #[tokio::test]
    async fn azure_builder_rejects_invalid_bool_param() {
        let url = Url::parse("abfss://container@account.dfs.core.windows.net/prefix/")
            .expect("valid url");
        let mut params = HashMap::new();
        params.insert("use_emulator".to_string(), "not-a-bool".to_string());

        let err = build_azure_object_store(&url, &params, handle())
            .expect_err("invalid bool should fail");
        assert!(
            matches!(err, DataFusionError::Configuration(_)),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn gcs_builder_with_skip_signature_anonymous() {
        let mut params = HashMap::new();
        params.insert("skip_signature".to_string(), "true".to_string());

        assert!(
            build_gcs_object_store("my-bucket", &params, handle()).is_ok(),
            "gcs object store should build with skip_signature"
        );
    }

    #[tokio::test]
    async fn gcs_builder_rejects_invalid_bool_param() {
        let mut params = HashMap::new();
        params.insert(
            "application_default_credentials".to_string(),
            "not-a-bool".to_string(),
        );

        let err = build_gcs_object_store("my-bucket", &params, handle())
            .expect_err("invalid bool should fail");
        assert!(
            matches!(err, DataFusionError::Configuration(_)),
            "unexpected error: {err}"
        );
    }
}
