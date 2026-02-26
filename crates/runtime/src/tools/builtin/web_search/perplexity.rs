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

use async_openai::{
    error::OpenAIError,
    types::chat::{
        ChatCompletionRequestMessage, ChatCompletionRequestUserMessageArgs,
        CreateChatCompletionRequestArgs,
    },
};
use llms::perplexity::types::{PerplexityRequest, PerplexityResponse};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{WebSearchParams, WebSearchResponse, WebSearchResult};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PerplexityWebSearchParams {
    query: String,
}

impl TryFrom<WebSearchParams> for PerplexityRequest {
    type Error = OpenAIError;

    fn try_from(web_search: WebSearchParams) -> Result<PerplexityRequest, Self::Error> {
        match web_search {
            WebSearchParams::Perplexity(perplexity_params) => {
                let openai_request = CreateChatCompletionRequestArgs::default()
                    .messages(vec![ChatCompletionRequestMessage::User(
                        ChatCompletionRequestUserMessageArgs::default()
                            .content(perplexity_params.query)
                            .build()?,
                    )])
                    .build()?;

                Ok(openai_request.into())
            }
        }
    }
}

impl From<PerplexityResponse> for WebSearchResponse {
    fn from(resp: PerplexityResponse) -> Self {
        WebSearchResponse {
            summary: resp
                .response
                .choices
                .first()
                .and_then(|c| c.message.content.clone()),
            results: resp
                .citations
                .into_iter()
                .map(WebSearchResult::webpage)
                .collect(),
        }
    }
}
