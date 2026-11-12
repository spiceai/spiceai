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

/// Parameters for `from: openrouter:<provider>/<model>` decision models, such as
/// `openrouter:baseten/elephant-1`.
///
/// Only decision models are supported. Use `POST /v1/decisions` or the SQL decision
/// functions; chat completions are refused.
#[derive(TypedParams)]
#[params(
    prefix = "openrouter",
    passthrough = crate::model::params::common::EVALUATE_RATE_CONTROLS,
    emit_specs
)]
pub struct OpenRouterModelParams {
    /// `OpenRouter` API key. Autoloads from secret stores as `openrouter_api_key`
    /// (env `OPENROUTER_API_KEY`).
    #[param(autoload_secret)]
    pub api_key: Option<SecretString>,
    /// `OpenRouter` API base URL. Spicepod key: `openrouter_endpoint`.
    #[param(default = "https://openrouter.ai/api")]
    pub endpoint: String,
}
