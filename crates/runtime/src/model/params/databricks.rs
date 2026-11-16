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

use runtime_parameters::TypedParams;
use secrecy::SecretString;

use crate::token_providers::databricks::FederationParams;

/// Parameters for `from: databricks` chat models.
#[derive(TypedParams)]
#[params(
    prefix = "databricks",
    passthrough = crate::model::params::common::PREFIXED_COMMON,
    emit_specs
)]
pub struct DatabricksModelParams {
    /// The Databricks workspace endpoint, e.g., dbc-a12cd3e4-56f7.cloud.databricks.com.
    pub endpoint: Option<String>,
    /// The Databricks API token to authenticate with the Databricks Models API.
    pub token: Option<SecretString>,
    /// The Databricks Service Principal Client ID. Can't be used with `databricks_token`. Defaults
    /// to `DATABRICKS_CLIENT_ID` for workload identity federation or beside a client secret.
    #[param(autoload_secret)]
    pub client_id: Option<SecretString>,
    /// The Databricks Service Principal Client Secret. Can't be used with `databricks_token`.
    pub client_secret: Option<SecretString>,
    /// A file that holds the workload's identity token, read again for every exchange, such as a
    /// Kubernetes projected service account token. With `databricks_client_id`, the model
    /// authenticates with workload identity federation. Defaults to `DATABRICKS_OIDC_TOKEN_FILEPATH`.
    #[param(autoload_secret)]
    pub oidc_token_filepath: Option<SecretString>,
    /// The audience of the identity token Spice requests from a GitHub Actions job or the
    /// workload's cloud identity, such as the Databricks account ID. With `databricks_client_id`,
    /// the model authenticates with workload identity federation. Defaults to
    /// `DATABRICKS_TOKEN_AUDIENCE`.
    #[param(autoload_secret)]
    pub token_audience: Option<SecretString>,
}

impl DatabricksModelParams {
    /// The workload identity federation params; `raw_params` are the ones the Spicepod sets.
    pub(crate) fn federation<S: std::hash::BuildHasher>(
        &self,
        raw_params: &std::collections::HashMap<String, SecretString, S>,
        github_actions: bool,
    ) -> FederationParams<'_> {
        crate::token_providers::databricks::component_federation(
            raw_params,
            [
                self.client_id.as_ref(),
                self.oidc_token_filepath.as_ref(),
                self.token_audience.as_ref(),
            ],
            github_actions,
        )
    }
}
