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

use runtime_parameters::TypedParams;
use secrecy::SecretString;

/// Parameters for `from: azure` embedding models.
#[derive(TypedParams)]
#[params(prefix = "azure")]
pub struct AzureEmbeddingParams {
    /// The Azure `OpenAI` resource endpoint, e.g., <https://resource-name.openai.azure.com>.
    #[param(runtime)]
    pub endpoint: Option<String>,
    /// The API version used for the Azure `OpenAI` service.
    pub api_version: Option<String>,
    /// The name of the model deployment.
    pub deployment_name: Option<String>,
    /// The Azure `OpenAI` API key.
    #[param(autoload_secret)]
    pub api_key: Option<SecretString>,
    /// The Azure Entra token for authentication.
    #[param(autoload_secret)]
    pub entra_token: Option<SecretString>,
    /// The Microsoft Entra tenant (directory) ID of the identity the embedding model
    /// authenticates as with workload identity federation, instead of `azure_api_key` or
    /// `azure_entra_token`. Defaults to `AZURE_TENANT_ID` when the model has no other credential.
    pub tenant_id: Option<String>,
    /// The client ID of the application or user-assigned managed identity whose federated
    /// credential trusts the workload's identity token. Defaults to `AZURE_CLIENT_ID` when the
    /// model has no other credential.
    pub client_id: Option<String>,
    /// The audience a GitHub Actions identity token is requested for. It must match the federated
    /// credential's audience. Defaults to `api://AzureADTokenExchange`. It cannot be set when the
    /// identity token comes from a file, whose audience is set where it is issued.
    pub federated_token_audience: Option<String>,
    /// A file that holds the workload's identity token, read again for every exchange, such as a
    /// Kubernetes projected service account token. Defaults to `AZURE_FEDERATED_TOKEN_FILE`; in a
    /// GitHub Actions job, Spice requests the token from GitHub instead.
    pub federated_token_file: Option<String>,
}

impl AzureEmbeddingParams {
    /// The workload identity federation params, and the client settings, borrowed.
    pub(crate) fn federation(&self) -> llms::workload_identity::AzureFederation<'_> {
        llms::workload_identity::AzureFederation {
            tenant_id: self.tenant_id.as_deref(),
            client_id: self.client_id.as_deref(),
            federated_token_audience: self.federated_token_audience.as_deref(),
            federated_token_file: self.federated_token_file.as_deref(),
            endpoint: self.endpoint.as_deref(),
            api_version: self.api_version.as_deref(),
            deployment_name: self.deployment_name.as_deref(),
        }
    }
}
