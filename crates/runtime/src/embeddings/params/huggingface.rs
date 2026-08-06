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

use super::{Pooling, Truncation};

/// Parameters for `from: huggingface` embedding models.
#[derive(TypedParams)]
#[params(prefix = "huggingface")]
pub struct HuggingFaceEmbeddingParams {
    /// The Hugging Face access token.
    #[param(runtime, autoload_secret)]
    pub hf_token: Option<SecretString>,
    /// The pooling strategy for the embedding model.
    #[param(runtime)]
    pub pooling: Option<Pooling>,
    /// The maximum sequence length for the embedding model.
    #[param(runtime)]
    pub max_seq_length: Option<usize>,
    /// How to handle an input longer than the model's maximum sequence length:
    /// `NONE` (default) to reject it, `END` to discard the end of the input,
    /// or `START` to discard the start of the input.
    #[param(runtime)]
    pub truncate: Option<Truncation>,
}
