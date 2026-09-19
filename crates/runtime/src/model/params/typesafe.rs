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

/// Parameters for `from: typesafe` System One evaluation models (Jev).
///
/// Chat completions are not supported. Use `POST /v1/evaluate`.
#[derive(TypedParams)]
#[params(
    prefix = "typesafe",
    passthrough = crate::model::params::common::PREFIXED_COMMON,
    emit_specs
)]
pub struct TypeSafeModelParams {
    /// `TypeSafe` API key. Autoloads from secret stores as `typesafe_api_key`
    /// (env `TYPESAFE_API_KEY`). Alias `ai_api_key` accepts spicepod param
    /// `typesafe_ai_api_key`; env `TYPESAFE_AI_API_KEY` is also tried at load time.
    #[param(autoload_secret, alias = "ai_api_key")]
    pub api_key: Option<SecretString>,
    /// `TypeSafe` API base URL. Defaults to the direct API (`https://api.typesafe.ai`),
    /// not the Vercel AI gateway. Spicepod key: `typesafe_endpoint`.
    #[param(default = "https://api.typesafe.ai")]
    pub endpoint: String,
}
