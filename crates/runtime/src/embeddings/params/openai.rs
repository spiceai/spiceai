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

use llms::openai::UsageTier;
use runtime_parameters::TypedParams;
use secrecy::SecretString;

/// Parameters for `from: openai` embedding models.
#[derive(TypedParams)]
#[params(prefix = "openai")]
pub struct OpenAiEmbeddingParams {
    /// The `OpenAI` API base endpoint.
    #[param(runtime, default = "https://api.openai.com/v1")]
    pub endpoint: String,
    /// The `OpenAI` API key.
    #[param(autoload_secret)]
    pub api_key: Option<SecretString>,
    /// The `OpenAI` organization ID.
    pub org_id: Option<String>,
    /// The `OpenAI` project ID.
    pub project_id: Option<String>,
    /// The current usage tier for the `OpenAI` account: 'free', 'tier1'-'tier5'.
    #[param(default = "tier1")]
    pub usage_tier: UsageTier,
}
