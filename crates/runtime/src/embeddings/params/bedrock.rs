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

use llms::bedrock::embed::{cohere::CohereEmbeddingInputType, nova::NovaEmbeddingPurpose};
use runtime_parameters::TypedParams;

/// Parameters for `from: bedrock` embedding models.
///
/// `truncate_mode` stays a string here because it targets a different enum per
/// model family (`CohereEmbeddingTruncate` vs `NovaTruncationMode`); it is
/// parsed once the model id is known.
#[derive(TypedParams)]
#[params(prefix = "bedrock")]
pub struct BedrockEmbeddingParams {
    /// The number of dimensions for the embedding output.
    pub dimensions: Option<u32>,
    /// Whether to normalize the embedding output.
    pub normalize: Option<bool>,
    /// Truncation mode for input text that exceeds the model's token limit.
    pub truncate_mode: Option<String>,
    /// The input type for Cohere embedding models.
    pub input_type: Option<CohereEmbeddingInputType>,
    /// The embedding purpose for Nova multimodal embedding models.
    pub embedding_purpose: Option<NovaEmbeddingPurpose>,
}
