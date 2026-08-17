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

use llms::google::auth::GoogleApi;
use runtime_parameters::TypedParams;
use secrecy::SecretString;

/// Parameters for `from: google` embedding models.
#[derive(TypedParams)]
#[params(prefix = "google")]
pub struct GoogleEmbeddingParams {
    /// Which Google backend to use: `google_ai` (the public Google AI Studio API, the
    /// default) or `vertex_ai` (GCP-project/region-scoped, for enterprise auth/governance).
    pub api: Option<GoogleApi>,
    /// The Google AI Studio API key. Required when `google_api` is `google_ai` (the default).
    #[param(autoload_secret)]
    pub api_key: Option<SecretString>,
    /// The GCP project ID. Required when `google_api` is `vertex_ai`.
    pub project: Option<String>,
    /// The GCP region, e.g. `us-central1`, or `global`. Required when `google_api` is `vertex_ai`.
    pub location: Option<String>,
    /// Path to a GCP service account JSON key file. One of `google_service_account_path`,
    /// `google_service_account_key`, or `google_application_default_credentials` is required
    /// when `google_api` is `vertex_ai`.
    pub service_account_path: Option<String>,
    /// GCP service account JSON key as a string.
    #[param(autoload_secret)]
    pub service_account_key: Option<SecretString>,
    /// Use Google Application Default Credentials for authentication. If the
    /// `GOOGLE_APPLICATION_CREDENTIALS` environment variable is set, uses that path.
    pub application_default_credentials: Option<bool>,
    /// The number of dimensions for the embedding output.
    #[param(runtime)]
    pub dimensions: Option<u32>,
}
