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
    types::{
        ChatCompletionRequestMessage, ChatCompletionRequestUserMessageArgs,
        CreateChatCompletionRequestArgs,
    },
};
use llms::perplexity::types::{PerplexityRequest, PerplexityResponse};

use super::{WebSearchParams, WebSearchResponse, WebSearchResult};

impl TryFrom<WebSearchParams> for PerplexityRequest {
    type Error = OpenAIError;

    fn try_from(params: WebSearchParams) -> Result<PerplexityRequest, Self::Error> {
        let WebSearchParams { query, limit: None } = params else {
            return Err(OpenAIError::InvalidArgument(
                "Perplexity web search does not support 'limit'".to_string(),
            ));
        };

        let openai_request = CreateChatCompletionRequestArgs::default()
            .messages(vec![ChatCompletionRequestMessage::User(
                ChatCompletionRequestUserMessageArgs::default()
                    .content(query)
                    .build()?,
            )])
            .build()?;

        Ok(openai_request.into())
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
