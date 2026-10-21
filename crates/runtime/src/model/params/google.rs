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

/// Parameters for `from: google` chat models. Authenticates via Vertex AI (GCP
/// project/region-scoped, service-account auth).
#[derive(TypedParams)]
#[params(
    prefix = "google",
    passthrough = crate::model::params::common::PREFIXED_COMMON,
    emit_specs
)]
pub struct GoogleModelParams {
    /// The GCP project ID.
    pub project: Option<String>,
    /// The GCP region, e.g. `us-central1`, or `global`.
    pub location: Option<String>,
    /// Path to a GCP service account JSON key file. One of `google_service_account_path`,
    /// `google_service_account_key`, or `google_application_default_credentials` is required.
    pub service_account_path: Option<String>,
    /// GCP service account JSON key as a string.
    #[param(autoload_secret)]
    pub service_account_key: Option<SecretString>,
    /// Use Google Application Default Credentials for authentication. If the
    /// `GOOGLE_APPLICATION_CREDENTIALS` environment variable is set, uses that path.
    pub application_default_credentials: Option<bool>,
}
