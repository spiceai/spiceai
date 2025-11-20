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

use std::str::FromStr;

use serde::{Deserialize, Serialize};
use snafu::ensure;

use crate::{
    bedrock::embed::BedrockEmbeddingConfig,
    embeddings::{Error as EmbedError, FailedToExtractEmbeddingsSnafu, Result as EmbedResult},
};

pub const NOVA_MULTIMODAL_EMBED_V2: &str = "amazon.nova-2-multimodal-embeddings-v1:0";
const MAX_NOVA_TEXT_LENGTH: usize = 8192;

#[derive(Debug)]
pub struct NovaConfig {
    pub model_name: String,
    pub dimensions: u32,
    pub embedding_purpose: NovaEmbeddingPurpose,
    pub truncation_mode: NovaTruncationMode,
}

impl BedrockEmbeddingConfig<NovaEmbedRequest, NovaEmbedResponse> for NovaConfig {
    fn model_id(&self) -> &String {
        &self.model_name
    }

    fn dimensions(&self) -> i32 {
        match self.dimensions {
            256 => 256,
            384 => 384,
            1024 => 1024,
            _ => 3072,
        }
    }

    fn extract_embeddings(
        &self,
        resp: NovaEmbedResponse,
    ) -> EmbedResult<(Vec<Vec<f32>>, Option<u32>)> {
        Ok((
            resp.embeddings.into_iter().map(|e| e.embedding).collect(),
            None,
        ))
    }

    fn to_request_blobs(&self, input_text: Vec<String>) -> EmbedResult<Vec<NovaEmbedRequest>> {
        input_text
            .into_iter()
            .map(|t| {
                ensure!(
                    t.len() <= MAX_NOVA_TEXT_LENGTH,
                    FailedToExtractEmbeddingsSnafu {
                        message: format!(
                            "Input text length {} exceeds maximum supported length {MAX_NOVA_TEXT_LENGTH}",
                            t.len(),
                        )
                    }
                );

                Ok(NovaEmbedRequest {
                    schema_version: Some("nova-multimodal-embed-v1".to_string()),
                    task_type: NovaTaskType::SingleEmbedding,
                    single_embedding_params: NovaSingleEmbeddingParams {
                        embedding_purpose: self.embedding_purpose.clone(),
                        embedding_dimension: Some(self.dimensions),
                        text: Some(NovaTextInput {
                            truncation_mode: self.truncation_mode.clone(),
                            value: Some(t),
                            source: None,
                        }),
                        image: None,
                        audio: None,
                        video: None,
                    },
                })
            })
            .collect::<EmbedResult<Vec<NovaEmbedRequest>>>()
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NovaEmbeddingPurpose {
    #[default]
    GenericIndex,
    GenericRetrieval,
    TextRetrieval,
    ImageRetrieval,
    VideoRetrieval,
    DocumentRetrieval,
    AudioRetrieval,
    Classification,
    Clustering,
}

impl FromStr for NovaEmbeddingPurpose {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "GENERIC_INDEX" => Ok(NovaEmbeddingPurpose::GenericIndex),
            "GENERIC_RETRIEVAL" => Ok(NovaEmbeddingPurpose::GenericRetrieval),
            "TEXT_RETRIEVAL" => Ok(NovaEmbeddingPurpose::TextRetrieval),
            "IMAGE_RETRIEVAL" => Ok(NovaEmbeddingPurpose::ImageRetrieval),
            "VIDEO_RETRIEVAL" => Ok(NovaEmbeddingPurpose::VideoRetrieval),
            "DOCUMENT_RETRIEVAL" => Ok(NovaEmbeddingPurpose::DocumentRetrieval),
            "AUDIO_RETRIEVAL" => Ok(NovaEmbeddingPurpose::AudioRetrieval),
            "CLASSIFICATION" => Ok(NovaEmbeddingPurpose::Classification),
            "CLUSTERING" => Ok(NovaEmbeddingPurpose::Clustering),
            _ => Err(format!("Invalid NovaEmbeddingPurpose: {s}")),
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NovaTruncationMode {
    Start,
    End,

    #[default]
    None,
}

impl NovaTruncationMode {
    #[must_use]
    pub fn all() -> Vec<NovaTruncationMode> {
        vec![Self::Start, Self::End, Self::None]
    }
}

impl FromStr for NovaTruncationMode {
    type Err = EmbedError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "START" => Ok(NovaTruncationMode::Start),
            "END" => Ok(NovaTruncationMode::End),
            "NONE" => Ok(NovaTruncationMode::None),
            _ => Err(EmbedError::InvalidParamError {
                param_key: "truncation_mode",
                value: s.to_string(),
                reason: format!(
                    "For Nova multi-modal model, 'truncation_mode' must be one of: {:?}",
                    NovaTruncationMode::all()
                ),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NovaTaskType {
    SingleEmbedding,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NovaEmbedRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_version: Option<String>,
    pub task_type: NovaTaskType,
    pub single_embedding_params: NovaSingleEmbeddingParams,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NovaSingleEmbeddingParams {
    pub embedding_purpose: NovaEmbeddingPurpose,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub embedding_dimension: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<NovaTextInput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<NovaImageInput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<NovaAudioInput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub video: Option<NovaVideoInput>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NovaTextInput {
    pub truncation_mode: NovaTruncationMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<NovaSourceObject>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NovaImageInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail_level: Option<NovaImageDetailLevel>,
    pub format: NovaImageFormat,
    pub source: NovaSourceObject,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NovaImageDetailLevel {
    StandardImage,
    DocumentImage,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NovaImageFormat {
    Png,
    Jpeg,
    Gif,
    Webp,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NovaAudioInput {
    pub format: NovaAudioFormat,
    pub source: NovaSourceObject,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NovaAudioFormat {
    Mp3,
    Wav,
    Ogg,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NovaVideoInput {
    pub format: NovaVideoFormat,
    pub source: NovaSourceObject,
    pub embedding_mode: NovaVideoEmbeddingMode,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NovaVideoFormat {
    Mp4,
    Mov,
    Mkv,
    Webm,
    Flv,
    Mpeg,
    Mpg,
    Wmv,
    #[serde(rename = "3gp")]
    ThreeGp,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NovaVideoEmbeddingMode {
    AudioVideoCombined,
    AudioVideoSeparate,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NovaSourceObject {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub s3_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NovaEmbedResponse {
    pub embeddings: Vec<NovaEmbeddingResult>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NovaEmbeddingResult {
    pub embedding_type: NovaEmbeddingType,
    pub embedding: Vec<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated_char_length: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NovaEmbeddingType {
    Text,
    Image,
    Video,
    Audio,
    AudioVideoCombined,
}
