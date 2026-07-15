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

/// Parameters for `from: google` embedding models.
#[derive(TypedParams)]
#[params(prefix = "google")]
pub struct GoogleEmbeddingParams {
    /// The Google API key.
    #[param(autoload_secret)]
    pub api_key: SecretString,
    /// The number of dimensions for the embedding output.
    #[param(runtime)]
    pub dimensions: Option<u32>,
}
