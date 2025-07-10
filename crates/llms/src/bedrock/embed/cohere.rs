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

use std::{collections::HashMap, fmt::Display, str::FromStr};

use async_openai::error::{ApiError, OpenAIError};
use serde::{Deserialize, Serialize};

use crate::{bedrock::embed::BedrockEmbeddingConfig, embeddings::Error as EmbedError};

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

#[derive(Debug, PartialEq, Clone, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CohereEmbeddingType {
    Float,
    Int8,
    Uint8,
    Binary,
    Ubinary,
}

impl Display for CohereEmbeddingType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let v = match self {
            Self::Float => "float",
            Self::Int8 => "int8",
            Self::Uint8 => "uint8",
            Self::Binary => "binary",
            Self::Ubinary => "ubinary",
        };

        write!(f, "{v}")
    }
}

#[derive(Debug, PartialEq, Clone, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CohereEmbeddingInputType {
    #[default]
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
    #[must_use]
    pub fn all() -> Vec<CohereEmbeddingInputType> {
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

#[derive(Debug, PartialEq, Clone, Eq, Hash, Serialize, Deserialize, Default)]
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

#[derive(Debug)]
pub struct CohereConfig {
    pub model_name: String,
    pub truncate: CohereEmbeddingTruncate,
    pub input_type: CohereEmbeddingInputType,
    pub embedding_type: CohereEmbeddingType,
}

const MAX_COHERE_INPUT_CHARACTER_LENGTH: usize = 2048;
const MAX_COHERE_TEXTS_PER_REQUEST: usize = 96;

impl BedrockEmbeddingConfig<CohereEmbedRequest, CohereEmbedResponse> for CohereConfig {
    fn model_id(&self) -> &String {
        &self.model_name
    }

    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss
    )]
    fn extract_embeddings(
        &self,
        mut resp: CohereEmbedResponse,
    ) -> Result<(Vec<Vec<f32>>, u32), OpenAIError> {
        // Estimate token count for Cohere models (approximate)
        let estimated_tokens: u32 = if let Some(texts) = resp.texts {
            texts
                .iter()
                .map(|text| (text.split_whitespace().count() as f32 * 1.3) as u32) // Rough estimate
                .sum()
        } else {
            0
        };

        let Some(float_embedding) = resp.embeddings.remove(&self.embedding_type) else {
            return Err(OpenAIError::ApiError(ApiError {
                message: format!(
                    "No {} vectors found in Cohere response.",
                    self.embedding_type
                ),
                r#type: None,
                param: None,
                code: None,
            }));
        };

        Ok((float_embedding, estimated_tokens))
    }

    fn to_request_blobs(
        &self,
        input_text: Vec<String>,
    ) -> Result<Vec<CohereEmbedRequest>, OpenAIError> {
        input_text
            .chunks(MAX_COHERE_TEXTS_PER_REQUEST)
            .map(|t| {
                if let Some(err) = t.iter().find_map(|t| {
                    if t.len() > MAX_COHERE_INPUT_CHARACTER_LENGTH {
                        Some(OpenAIError::InvalidArgument(format!(
                            "Input {} is longer than maximum supported length {MAX_COHERE_INPUT_CHARACTER_LENGTH}",
                            t.len()
                        )))
                    } else {
                        None
                    }
                }) {
                    return Err(err)
                }

                Ok(CohereEmbedRequest {
                    texts: t.to_vec(),
                    input_type: self.input_type.clone(),
                    truncate: Some(self.truncate.clone()),
                    embedding_types: Some(vec![self.embedding_type.clone()]),
                })
            })
            .collect::<Result<Vec<CohereEmbedRequest>, OpenAIError>>()
    }

    fn dimensions(&self) -> i32 {
        1024
    }
}
