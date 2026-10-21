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

use super::Truncation;

/// Parameters for `from: huggingface` rerankers — a local cross-encoder
/// reranker run in-process via the candle TEI backend.
#[derive(TypedParams)]
#[params(prefix = "huggingface")]
pub struct HuggingFaceRerankerParams {
    /// The Hugging Face access token. Also accepts `api_key`, matching the
    /// key name used by the other reranker providers.
    #[param(runtime, autoload_secret, alias = "api_key")]
    pub hf_token: Option<SecretString>,
    /// The maximum sequence length for the `(query, document)` pair.
    #[param(runtime)]
    pub max_seq_length: Option<usize>,
    /// How to handle a `(query, document)` pair longer than the model's
    /// maximum sequence length: `none` (default) to reject it, `end` to
    /// discard the end of the pair, or `start` to discard the start.
    #[param(runtime)]
    pub truncate: Option<Truncation>,
}
