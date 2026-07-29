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

/// Parameters for `from: xai` chat/responses models.
#[derive(TypedParams)]
#[params(
    prefix = "xai",
    passthrough = crate::model::params::common::PREFIXED_COMMON,
    emit_specs
)]
pub struct XaiModelParams {
    /// The `xAI` API key.
    pub api_key: Option<SecretString>,
    /// `xAI` usage tier (0-4). Used for rate limit defaults.
    pub usage_tier: Option<String>,
}
