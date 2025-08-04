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

use async_openai::error::OpenAIError;
use serde::{Deserialize, Serialize};

use crate::bedrock::embed::BedrockEmbeddingConfig;
pub const TITAN_TEXT_EMBED_V2: &str = "amazon.titan-embed-text-v2:0";

#[derive(Debug)]
pub struct TitanConfig {
    pub model_name: String,
    pub normalize: bool,
    pub dimensions: u32,
}
const MAX_TITAN_INPUT_LENGTH: usize = 8192;

impl BedrockEmbeddingConfig<TitanEmbedRequest, TitanEmbedResponse> for TitanConfig {
    fn model_id(&self) -> &String {
        &self.model_name
    }
    fn dimensions(&self) -> i32 {
        match self.dimensions {
            256 => 256,
            512 => 512,
            _ => 1024,
        }
    }

    fn extract_embeddings(
        &self,
        resp: TitanEmbedResponse,
    ) -> Result<(Vec<Vec<f32>>, u32), OpenAIError> {
        Ok((vec![resp.embedding], resp.input_text_token_count))
    }

    fn to_request_blobs(
        &self,
        input_text: Vec<String>,
    ) -> Result<Vec<TitanEmbedRequest>, OpenAIError> {
        input_text
            .into_iter()
            .map(|t| {
                // For Titan models, we need to be more careful about token limits
                // This is still an approximation as we don't have access to the actual tokenizer
                if t.split_whitespace().count() > MAX_TITAN_INPUT_LENGTH {
                    return Err(OpenAIError::InvalidArgument(format!(
                        "Input {} is longer than maximum supported length {}",
                        t.len(),
                        MAX_TITAN_INPUT_LENGTH
                    )));
                }
                Ok(TitanEmbedRequest {
                    input_text: t,
                    normalize: Some(self.normalize),
                    dimensions: Some(self.dimensions),
                    embedding_types: Some(vec!["float".to_string()]),
                })
            })
            .collect::<Result<Vec<TitanEmbedRequest>, OpenAIError>>()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TitanEmbedRequest {
    #[serde(rename = "inputText")]
    pub input_text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub normalize: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "embeddingTypes")]
    pub embedding_types: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TitanEmbedResponse {
    pub embedding: Vec<f32>,
    #[serde(rename = "inputTextTokenCount")]
    pub input_text_token_count: u32,
    #[serde(skip_serializing_if = "Option::is_none", rename = "embeddingsByType")]
    pub embeddings_by_type: Option<serde_json::Value>,
}
