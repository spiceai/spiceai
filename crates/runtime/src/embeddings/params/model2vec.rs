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

/// Parameters for `from: model2vec` embedding models.
#[derive(TypedParams)]
#[params(prefix = "model2vec")]
pub struct Model2VecEmbeddingParams {
    /// The Hugging Face access token.
    #[param(secret)]
    pub hf_token: Option<SecretString>,
    /// The subfolder within the Hugging Face repo containing the model.
    pub subfolder: Option<String>,
    /// Whether to normalize the embedding output.
    pub normalize: Option<bool>,
    /// The number of threads to use for parallel inference.
    #[param(runtime)]
    pub parallelism: Option<usize>,
    /// The maximum token length for embedding input.
    #[param(runtime)]
    pub embed_max_token_length: Option<usize>,
    /// The custom batch size for embedding inference.
    #[param(runtime)]
    pub embed_custom_batch_size: Option<usize>,
}
