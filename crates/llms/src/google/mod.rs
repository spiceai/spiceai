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
#![allow(clippy::missing_errors_doc)]

mod chat;
mod embed;

use std::sync::Arc;

use async_openai::{
    error::{ApiError, OpenAIError},
    types::chat::{CompletionTokensDetails, CompletionUsage, PromptTokensDetails},
};
use cache::{CacheProvider, result::embeddings::CachedEmbeddingResult};
use google_genai::types::UsageMetadata;
use secrecy::{ExposeSecret, SecretString};

use crate::google::embed::EmbedGoogle;

#[derive(Debug)]
pub struct Google {
    client: google_genai::Client,
    model: String,
}

impl Google {
    pub fn new(api_key: &SecretString, model: &str) -> Result<Self, google_genai::Error> {
        Ok(Self {
            client: google_genai::Client::new(api_key.expose_secret().to_string())?,
            model: model.to_string(),
        })
    }

    pub fn new_embeddings(
        api_key: &SecretString,
        model: &str,
        dimensions: Option<u32>,
        embeddings_cache: Option<Arc<dyn CacheProvider<CachedEmbeddingResult> + Send + Sync>>,
    ) -> Result<EmbedGoogle, google_genai::Error> {
        Ok(EmbedGoogle {
            g: Self::new(api_key, model)?,
            dimensions,
            embeddings_cache,
        })
    }
}

#[must_use]
pub fn to_completion_usage(usage_metadata: &UsageMetadata) -> CompletionUsage {
    // Extract audio tokens
    let prompt_audio_tokens = usage_metadata
        .prompt_tokens_details
        .as_ref()
        .and_then(|details| {
            details
                .iter()
                .find(|d| d.modality.as_deref() == Some("AUDIO"))
                .and_then(|d| d.token_count)
        });
    let completion_audio_tokens =
        usage_metadata
            .candidates_tokens_details
            .as_ref()
            .and_then(|details| {
                details
                    .iter()
                    .find(|d| d.modality.as_deref() == Some("AUDIO"))
                    .and_then(|d| d.token_count)
            });

    let completion_tokens_details =
        if completion_audio_tokens.is_some() || usage_metadata.thoughts_token_count.is_some() {
            Some(CompletionTokensDetails {
                accepted_prediction_tokens: None,
                audio_tokens: completion_audio_tokens,
                reasoning_tokens: usage_metadata.thoughts_token_count,
                rejected_prediction_tokens: None,
            })
        } else {
            None
        };
    let prompt_tokens_details =
        if prompt_audio_tokens.is_some() || usage_metadata.cached_content_token_count.is_some() {
            Some(PromptTokensDetails {
                audio_tokens: prompt_audio_tokens,
                cached_tokens: usage_metadata.cached_content_token_count,
            })
        } else {
            None
        };

    CompletionUsage {
        prompt_tokens: usage_metadata.prompt_token_count
            + prompt_tokens_details.as_ref().map_or(0, |c| {
                c.audio_tokens.unwrap_or_default() + c.cached_tokens.unwrap_or_default()
            }),
        completion_tokens: usage_metadata.candidates_token_count.unwrap_or_default()
            + completion_tokens_details.as_ref().map_or(0, |c| {
                c.accepted_prediction_tokens.unwrap_or_default()
                    + c.audio_tokens.unwrap_or_default()
                    + c.reasoning_tokens.unwrap_or_default()
                    + c.rejected_prediction_tokens.unwrap_or_default()
            }),
        total_tokens: usage_metadata.total_token_count,
        prompt_tokens_details,
        completion_tokens_details,
    }
}

pub(super) fn openai_api_error(msg: impl Into<String>) -> OpenAIError {
    OpenAIError::ApiError(ApiError {
        message: msg.into(),
        r#type: None,
        param: None,
        code: None,
    })
}
