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
use base64::{engine::general_purpose, Engine};
use reqwest;
use secrecy::SecretString;
use snafu::{ResultExt, Snafu};

use crate::secrets::SecretStore;

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
    token: Option<String>,
    namespace: Option<String>,
}

impl KubernetesClient {
    fn new() -> Self {
        Self {
            client: None,
            token: None,
            namespace: None,
        }
    }

    fn init(&mut self) -> Result<(), Error> {
        self.token = Some(
            std::fs::read_to_string(format!("{KUBERNETES_ACCOUNT_PATH}/token"))
                .context(UnableToReadK8STokenSnafu)?,
        );

        self.namespace = Some(
            std::fs::read_to_string(format!("{KUBERNETES_ACCOUNT_PATH}/namespace"))
                .context(UnableToReadK8SNamespaceSnafu)?,
        );

        let ca_cert = std::fs::read_to_string(format!("{KUBERNETES_ACCOUNT_PATH}/ca.crt"))
            .context(UnableToReadCACertificateSnafu)?;

        let Ok(certificate) = reqwest::Certificate::from_pem(ca_cert.as_bytes()) else {
            return Err(Error::UnableToReadKubernetesCredentials {});
        };

        self.client = Some(
            reqwest::Client::builder()
                .add_root_certificate(certificate)
                .build()
                .context(UnableToCreateK8SClientSnafu)?,
        );

        Ok(())
    }

    async fn get_secret(&self, secret_name: &str) -> Result<HashMap<String, String>, Error> {
        let Some(client) = &self.client else {
            return Err(Error::UnableToReadKubernetesCredentials {});
        };

        let Some(token) = &self.token else {
            return Err(Error::UnableToReadKubernetesCredentials {});
        };

        let Some(namespace) = &self.namespace else {
            return Err(Error::UnableToReadKubernetesCredentials {});
        };

        let url =
            format!("{KUBERNETES_API_SERVER}/api/v1/namespaces/{namespace}/secrets/{secret_name}");

        let kubernetes_secret = client
            .get(url.clone())
            .bearer_auth(token.clone())
            .send()
            .await
            .context(UnableToGetK8SSecretSnafu)?
            .json::<HashMap<String, serde_json::value::Value>>()
            .await
            .context(UnableToGetK8SSecretSnafu)?;

        let mut secret: HashMap<String, String> = HashMap::new();

        let Some(data) = kubernetes_secret.get("data") else {
            return Ok(secret);
        };

        let Some(obj) = data.as_object() else {
            return Ok(secret);
        };

        obj.iter().for_each(|(key, value)| {
            let Some(value) = value.as_str() else {
                return;
            };

            let Ok(decoded_string) = general_purpose::STANDARD.decode(value) else {
                return;
            };

            let Ok(secret_value) = String::from_utf8(decoded_string) else {
                return;
            };

            secret.insert(key.clone(), secret_value.trim_end_matches('\n').to_string());
        });

        Ok(secret)
    }
}

pub struct KubernetesSecretStore {
    secret_name: String,
    kubernetes_client: KubernetesClient,
}

impl KubernetesSecretStore {
    #[must_use]
    pub fn new(secret_name: String) -> Self {
        Self {
            secret_name,
            kubernetes_client: KubernetesClient::new(),
        }
    }

    /// Initializes the Kubernetes secret store.
    ///
    /// # Errors
    ///
    /// Returns an error if unable to read Kubernetes credentials.
    pub fn init(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Err(err) = self.kubernetes_client.init() {
            return Err(Box::new(StoreError::UnableToInitKubernetesClient {
                source: err,
            }));
        }

        Ok(())
    }
}

#[async_trait]
impl SecretStore for KubernetesSecretStore {
    #[must_use]
    async fn get_secret(&self, key: &str) -> crate::secrets::AnyErrorResult<Option<SecretString>> {
        // First try looking for `spice_my_key` and then `my_key`
        let prefixed_key = format!("{SPICE_KEY_PREFIX}{key}");
        match self.kubernetes_client.get_secret(&self.secret_name).await {
            Ok(secret) => {
                if let Some(value) = secret.get(&prefixed_key) {
                    return Ok(Some(SecretString::new(value.clone())));
                }
                Ok(secret.get(key).cloned().map(SecretString::new))
            }
            Err(err) => Err(Box::new(StoreError::UnableToGetSecret { source: err })),
        }
    }
}
