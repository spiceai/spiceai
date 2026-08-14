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

/// Parameters for `from: jina` rerankers.
#[derive(TypedParams)]
#[params(prefix = "jina")]
pub struct JinaRerankerParams {
    /// The Jina API key.
    #[param(runtime, autoload_secret, alias = "jina_api_key")]
    pub api_key: SecretString,
    /// The Jina API base endpoint.
    #[param(runtime)]
    pub endpoint: Option<String>,
}
