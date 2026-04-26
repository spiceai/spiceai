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

use std::collections::HashMap;

use async_trait::async_trait;
use base64::{Engine, engine::general_purpose};
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};

/// Characters that must be percent-encoded in Kubernetes secret names to prevent path traversal.
/// This encodes control characters plus `/` and `\` to prevent path traversal attacks,
/// while preserving safe characters like `-`, `_`, and `.` that are valid in secret names.
const PATH_SEGMENT_ENCODE_SET: &AsciiSet = &CONTROLS.add(b'/').add(b'\\');
use reqwest;
use runtime_parameter_spec::ParameterSpec;
use secrecy::{ExposeSecret, SecretString, zeroize::Zeroizing};
use snafu::{ResultExt, Snafu};

/// Parameters accepted by the `kubernetes` secret store.
pub const PARAMETERS: &[ParameterSpec] = &[ParameterSpec::runtime("namespace")
    .description(
        "Kubernetes namespace containing the secret. Defaults to the namespace of the \
         current pod (read from the service-account mount).",
    )
    .examples(&["spice", "my-team"])];

/// Resolved configuration for the `kubernetes` secret store.
#[derive(Debug, Clone)]
pub struct KubernetesConfig {
    pub secret_name: String,
    pub namespace: Option<String>,
}

impl KubernetesConfig {
    /// Builds a [`KubernetesConfig`] from the parsed selector and a
    /// validated parameter map.
    #[must_use]
    pub fn from_params(secret_name: String, params: &HashMap<String, String>) -> Self {
        Self {
            secret_name,
            namespace: params.get("namespace").cloned(),
        }
    }
}

use crate::SecretStore;

const SPICE_KEY_PREFIX: &str = "spice_";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to read K8S token: {source}"))]
    UnableToReadK8SToken { source: std::io::Error },

    #[snafu(display("Unable to read K8S namespace: {source}"))]
    UnableToReadK8SNamespace { source: std::io::Error },

    #[snafu(display("Unable to read K8S CA certificate: {source}"))]
    UnableToReadCACertificate { source: std::io::Error },

    #[snafu(display("Unable to read K8S credentials"))]
    UnableToReadKubernetesCredentials {},

    #[snafu(display("Unable to create K8S http client: {source}"))]
    UnableToCreateK8SClient { source: reqwest::Error },

    #[snafu(display("Unable to get secret from K8S: {source}"))]
    UnableToGetK8SSecret { source: reqwest::Error },
}

#[derive(Debug, Snafu)]
pub enum StoreError {
    #[snafu(display("Unable to init kubernetes store: {source}"))]
    UnableToInitKubernetesClient { source: Error },

    #[snafu(display("Unable to get secret from: {source}"))]
    UnableToGetSecret { source: Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

const KUBERNETES_ACCOUNT_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount";
const KUBERNETES_API_SERVER: &str = "https://kubernetes.default.svc";

struct KubernetesClient {
    client: Option<reqwest::Client>,
    /// Service-account bearer token. Held as a [`SecretString`] so the
    /// in-memory buffer is zeroized when the client drops and so stray
    /// `{:?}` prints (e.g. via `Debug` on an outer struct) cannot leak it.
    token: Option<SecretString>,
    namespace: Option<String>,
    /// Optional namespace override supplied via `params: { namespace: ... }`.
    /// When set, takes precedence over the namespace from the service-account mount.
    namespace_override: Option<String>,
}

fn secret_url(namespace: &str, secret_name: &str) -> String {
    let encoded_secret_name = utf8_percent_encode(secret_name, PATH_SEGMENT_ENCODE_SET);
    format!("{KUBERNETES_API_SERVER}/api/v1/namespaces/{namespace}/secrets/{encoded_secret_name}")
}

fn trim_single_trailing_newline(value: &str) -> &str {
    value.strip_suffix('\n').unwrap_or(value)
}

impl KubernetesClient {
    fn new(namespace_override: Option<String>) -> Self {
        Self {
            client: None,
            token: None,
            namespace: None,
            namespace_override,
        }
    }

    async fn init(&mut self) -> Result<(), Error> {
        let override_ns = self.namespace_override.clone();
        // Perform blocking file I/O in a separate thread to avoid blocking the async runtime.
        // When a namespace override is supplied, skip reading the namespace file so the store
        // works in environments where it is unavailable (e.g. local dev).
        let (token, namespace, ca_cert) = tokio::task::spawn_blocking(move || {
            let token = std::fs::read_to_string(format!("{KUBERNETES_ACCOUNT_PATH}/token"))
                .context(UnableToReadK8STokenSnafu)?;

            let namespace = match override_ns {
                Some(ns) => ns,
                None => std::fs::read_to_string(format!("{KUBERNETES_ACCOUNT_PATH}/namespace"))
                    .context(UnableToReadK8SNamespaceSnafu)?,
            };

            let ca_cert = std::fs::read_to_string(format!("{KUBERNETES_ACCOUNT_PATH}/ca.crt"))
                .context(UnableToReadCACertificateSnafu)?;

            Ok::<_, Error>((token, namespace, ca_cert))
        })
        .await
        .map_err(|_| Error::UnableToReadKubernetesCredentials {})??;

        self.token = Some(SecretString::from(token));
        self.namespace = Some(namespace);

        let Ok(certificate) = reqwest::Certificate::from_pem(ca_cert.as_bytes()) else {
            return Err(Error::UnableToReadKubernetesCredentials {});
        };

        self.client = Some(
            reqwest::Client::builder()
                .add_root_certificate(certificate)
                .connect_timeout(std::time::Duration::from_secs(10))
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .context(UnableToCreateK8SClientSnafu)?,
        );

        Ok(())
    }

    /// Fetches the named Kubernetes secret and returns its `data` map with
    /// every value wrapped in a [`SecretString`] so each entry owns a
    /// zeroize-on-drop allocation. Callers pull exactly the key they care
    /// about; everything else drops with its bytes scrubbed when the map
    /// goes out of scope.
    async fn get_secret(&self, secret_name: &str) -> Result<HashMap<String, SecretString>, Error> {
        let Some(client) = &self.client else {
            return Err(Error::UnableToReadKubernetesCredentials {});
        };

        let Some(token) = &self.token else {
            return Err(Error::UnableToReadKubernetesCredentials {});
        };

        let Some(namespace) = &self.namespace else {
            return Err(Error::UnableToReadKubernetesCredentials {});
        };

        let url = secret_url(namespace, secret_name);

        let kubernetes_secret = client
            .get(url.clone())
            .bearer_auth(token.expose_secret())
            .send()
            .await
            .context(UnableToGetK8SSecretSnafu)?
            .json::<HashMap<String, serde_json::value::Value>>()
            .await
            .context(UnableToGetK8SSecretSnafu)?;

        let mut secret: HashMap<String, SecretString> = HashMap::new();

        let Some(data) = kubernetes_secret.get("data") else {
            return Ok(secret);
        };

        let Some(obj) = data.as_object() else {
            return Ok(secret);
        };

        for (key, value) in obj {
            let Some(b64_value) = value.as_str() else {
                continue;
            };

            // `Zeroizing` scrubs the decoded bytes when the temporary drops,
            // covering the window between base64 decode and UTF-8 conversion.
            let Ok(decoded_bytes) = general_purpose::STANDARD.decode(b64_value) else {
                continue;
            };
            let decoded = Zeroizing::new(decoded_bytes);

            let Ok(decoded_str) = std::str::from_utf8(&decoded) else {
                continue;
            };

            // Trim a single trailing newline (common artifact of `echo | base64`)
            // via a string slice; avoids the intermediate plain-String
            // allocation that the previous `.to_string().trim()` path produced.
            let trimmed = trim_single_trailing_newline(decoded_str);
            secret.insert(key.clone(), SecretString::from(trimmed.to_string()));
        }

        Ok(secret)
    }
}

pub struct KubernetesSecretStore {
    secret_name: String,
    kubernetes_client: KubernetesClient,
}

impl KubernetesSecretStore {
    #[must_use]
    pub fn new(secret_name: String, namespace_override: Option<String>) -> Self {
        Self {
            secret_name,
            kubernetes_client: KubernetesClient::new(namespace_override),
        }
    }

    /// Initializes the Kubernetes secret store.
    ///
    /// # Errors
    ///
    /// Returns an error if unable to read Kubernetes credentials.
    pub async fn init(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Err(err) = self.kubernetes_client.init().await {
            return Err(Box::new(StoreError::UnableToInitKubernetesClient {
                source: err,
            }));
        }

        Ok(())
    }
}

#[async_trait]
impl SecretStore for KubernetesSecretStore {
    async fn get_secret(&self, key: &str) -> crate::AnyErrorResult<Option<SecretString>> {
        // First try looking for `spice_my_key` and then `my_key`
        let prefixed_key = format!("{SPICE_KEY_PREFIX}{key}");
        let mut secret = self
            .kubernetes_client
            .get_secret(&self.secret_name)
            .await
            .map_err(|err| Box::new(StoreError::UnableToGetSecret { source: err }))?;

        // Hand the `SecretString` back by removing it from the map so its
        // zeroize-on-drop guarantee transfers to the caller rather than
        // leaving a duplicate allocation behind.
        if let Some(value) = secret.remove(&prefixed_key) {
            return Ok(Some(value));
        }
        Ok(secret.remove(key))
    }
}

#[cfg(test)]
mod tests {
    use super::{secret_url, trim_single_trailing_newline};

    #[test]
    fn secret_url_encodes_secret_name() {
        let url = secret_url("default", "../configmaps/sensitive");
        assert!(url.ends_with("secrets/..%2Fconfigmaps%2Fsensitive"));
    }

    #[test]
    fn secret_url_handles_regular_name_without_changes() {
        let url = secret_url("default", "my-secret");
        assert!(url.ends_with("secrets/my-secret"));
    }

    #[test]
    fn trim_single_trailing_newline_removes_one_newline() {
        assert_eq!(trim_single_trailing_newline("secret\n"), "secret");
        assert_eq!(trim_single_trailing_newline("secret\n\n"), "secret\n");
    }

    #[test]
    fn trim_single_trailing_newline_preserves_other_whitespace() {
        assert_eq!(trim_single_trailing_newline(" secret \t"), " secret \t");
        assert_eq!(trim_single_trailing_newline("secret"), "secret");
    }
}
