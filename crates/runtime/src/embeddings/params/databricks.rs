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

/// Parameters for `from: databricks` embedding models.
#[derive(TypedParams)]
#[params(prefix = "databricks")]
pub struct DatabricksEmbeddingParams {
    /// The Databricks workspace endpoint, e.g., dbc-a12cd3e4-56f7.cloud.databricks.com.
    pub(crate) endpoint: String,
    /// The Databricks API token.
    #[param(autoload_secret)]
    pub(crate) token: Option<SecretString>,
    /// The Databricks Service Principal Client ID. Cannot be used with `databricks_token`.
    pub(crate) client_id: Option<String>,
    /// The Databricks Service Principal Client Secret. Cannot be used with `databricks_token`.
    #[param(autoload_secret)]
    pub(crate) client_secret: Option<SecretString>,
}
