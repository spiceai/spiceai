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
    /// The Azure OpenAI resource endpoint, e.g., <https://resource-name.openai.azure.com>.
    #[param(runtime)]
    pub endpoint: Option<String>,
    /// The API version used for the Azure OpenAI service.
    pub api_version: Option<String>,
    /// The name of the model deployment.
    pub deployment_name: Option<String>,
    /// The Azure OpenAI API key.
    #[param(autoload_secret)]
    pub api_key: Option<SecretString>,
    /// The Azure Entra token for authentication.
    #[param(autoload_secret)]
    pub entra_token: Option<SecretString>,
}
