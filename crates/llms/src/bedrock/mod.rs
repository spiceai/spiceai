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

pub mod embed;

use aws_config::SdkConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct BedrockClient {
    pub(crate) client: aws_sdk_bedrockruntime::Client,
}

impl BedrockClient {
    pub fn new(config: &SdkConfig) -> Self {
        let client = aws_sdk_bedrockruntime::Client::new(config);
        Self { client }
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

#[derive(Debug, Serialize, Deserialize)]
pub struct CohereEmbedRequest {
    pub texts: Vec<String>,
    pub input_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncate: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "embeddingTypes")]
    pub embedding_types: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CohereEmbedResponse {
    pub embeddings: Vec<Vec<f32>>,
    pub id: String,
    pub response_type: String,
    pub texts: Vec<String>,
}
