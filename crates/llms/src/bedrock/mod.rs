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

use std::{collections::HashMap, fmt::Display, str::FromStr};

use aws_config::SdkConfig;
use serde::{Deserialize, Serialize};

use crate::embeddings::Error as EmbedError;

#[derive(Debug, Clone)]
pub struct BedrockClient {
    pub(crate) client: aws_sdk_bedrockruntime::Client,
}

impl BedrockClient {
    #[must_use] pub fn new(config: &SdkConfig) -> Self {
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
    pub input_type: CohereEmbeddingInputType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncate: Option<CohereEmbeddingTruncate>,
    pub embedding_types: Option<Vec<CohereEmbeddingType>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CohereEmbedResponse {
    pub embeddings: HashMap<CohereEmbeddingType, Vec<Vec<f32>>>,
    pub id: String,
    pub response_type: String,
    pub texts: Option<Vec<String>>,
    pub images: Option<Vec<String>>,
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CohereEmbeddingType {
    Float,
    Int8,
    Uint8,
    Binary,
    Ubinary,
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CohereEmbeddingInputType {
    SearchDocument,
    SearchQuery,
    Classification,
    Clustering,
    Image,
}

impl Display for CohereEmbeddingInputType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let v: &'static str = match self {
            Self::SearchDocument => "search_document",
            Self::SearchQuery => "search_query",
            Self::Classification => "classification",
            Self::Clustering => "clustering",
            Self::Image => "image",
        };
        write!(f, "{v}")
    }
}

impl CohereEmbeddingInputType {
    #[must_use] pub fn all() -> Vec<CohereEmbeddingInputType> {
        vec![
            Self::SearchDocument,
            Self::SearchQuery,
            Self::Classification,
            Self::Clustering,
            Self::Image,
        ]
    }
}

impl FromStr for CohereEmbeddingInputType {
    type Err = EmbedError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "search_document" => Ok(Self::SearchDocument),
            "search_query" => Ok(Self::SearchQuery),
            "classification" => Ok(Self::Classification),
            "clustering" => Ok(Self::Clustering),
            "image" => Ok(Self::Image),
            _ => Err(EmbedError::InvalidParamError {
                param_key: "input_type",
                value: s.to_string(),
                reason: format!(
                    "For Cohere model, 'input_type' must be one of: {:?}",
                    CohereEmbeddingInputType::all()
                ),
            }),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CohereEmbeddingTruncate {
    None,
    Start,

    #[default]
    End,
}

impl Display for CohereEmbeddingTruncate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let v: &'static str = match self {
            Self::None => "NONE",
            Self::Start => "START",
            Self::End => "END",
        };
        write!(f, "{v}")
    }
}

impl FromStr for CohereEmbeddingTruncate {
    type Err = EmbedError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "NONE" => Ok(Self::None),
            "START" => Ok(Self::Start),
            "END" => Ok(Self::End),
            _ => Err(EmbedError::InvalidParamError {
                param_key: "truncate",
                value: s.to_string(),
                reason: format!(
                    "For Cohere model, 'truncate' must be one of: {}, {} or {}.",
                    Self::End,
                    Self::None,
                    Self::Start,
                ),
            }),
        }
    }
}
