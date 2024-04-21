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

use std::collections::HashMap;

use async_trait::async_trait;
use base64::{engine::general_purpose, Engine};
use reqwest;
use snafu::Snafu;

use super::{Secret, SecretStore};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to read kubernetes credentials"))]
    UnableToReadKubernetesCredentials {},
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

    fn init(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.token = Some(std::fs::read_to_string(format!(
            "{KUBERNETES_ACCOUNT_PATH}/token"
        ))?);

        self.namespace = Some(std::fs::read_to_string(format!(
            "{KUBERNETES_ACCOUNT_PATH}/namespace"
        ))?);

        let ca_cert = std::fs::read_to_string(format!("{KUBERNETES_ACCOUNT_PATH}/ca.crt"))?;

        let Ok(certificate) = reqwest::Certificate::from_pem(ca_cert.as_bytes()) else {
            return Err(Box::new(Error::UnableToReadKubernetesCredentials {}));
        };

        self.client = Some(
            reqwest::Client::builder()
                .add_root_certificate(certificate)
                .build()?,
        );

        Ok(())
    }

    async fn get_secret(
        &self,
        secret_name: &str,
    ) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
        let Some(client) = &self.client else {
            return Err(Box::new(Error::UnableToReadKubernetesCredentials {}));
        };

        let Some(token) = &self.token else {
            return Err(Box::new(Error::UnableToReadKubernetesCredentials {}));
        };

        let Some(namespace) = &self.namespace else {
            return Err(Box::new(Error::UnableToReadKubernetesCredentials {}));
        };

        let url =
            format!("{KUBERNETES_API_SERVER}/api/v1/namespaces/{namespace}/secrets/{secret_name}");

        let kubernetes_secret = match client
            .get(url.clone())
            .bearer_auth(token.clone())
            .send()
            .await?
            .json::<HashMap<String, serde_json::value::Value>>()
            .await
        {
            Ok(response) => response,
            Err(e) => return Err(Box::new(e)),
        };

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

#[allow(clippy::module_name_repetitions)]
pub struct KubernetesSecretStore {
    kubernetes_client: KubernetesClient,
}

impl Default for KubernetesSecretStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KubernetesSecretStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            kubernetes_client: KubernetesClient::new(),
        }
    }

    /// Initializes the Kubernetes secret store.
    ///
    /// # Errors
    ///
    /// Returns an error if unable to read Kubernetes credentials.
    pub fn init(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Err(_e) = self.kubernetes_client.init() {
            return Err(Box::new(Error::UnableToReadKubernetesCredentials {}));
        }

        Ok(())
    }
}

#[async_trait]
impl SecretStore for KubernetesSecretStore {
    #[must_use]
    async fn get_secret(&self, secret_name: &str) -> Option<Secret> {
        match self.kubernetes_client.get_secret(secret_name).await {
            Ok(secret) => Some(Secret::new(secret.clone())),
            Err(err) => {
                tracing::warn!(
                    "Failed to get secret {} from kubernetes store, {}",
                    secret_name,
                    err
                );
                None
            }
        }
    }
}
