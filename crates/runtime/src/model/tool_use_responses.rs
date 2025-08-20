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
    types::responses::{
        CodeInterpreter, CodeInterpreterContainer, CodeInterpreterContainerKind, CreateResponse,
        CreateResponseArgs, Response, ResponseStream, ToolDefinition, WebSearchPreview,
    },
};
use async_trait::async_trait;
use llms::chat::Error as LlmError;
use llms::responses::Responses;
use llms::responses::{Error as ResponsesError, FailedToLoadModelSnafu};
use snafu::ResultExt;
use std::sync::Arc;
use tracing_futures::Instrument;

#[derive(Clone, Debug)]
pub enum OpenAIResponsesTools {
    CodeInterpreter,
    WebSearch,
}

impl Into<ToolDefinition> for OpenAIResponsesTools {
    fn into(self) -> ToolDefinition {
        match self {
            OpenAIResponsesTools::CodeInterpreter => {
                ToolDefinition::CodeInterpreter(CodeInterpreter {
                    container: CodeInterpreterContainer::Container(
                        CodeInterpreterContainerKind::Auto { file_ids: None },
                    ),
                })
            }
            OpenAIResponsesTools::WebSearch => ToolDefinition::WebSearchPreview(WebSearchPreview {
                search_context_size: None,
                user_location: None,
            }),
        }
    }
}

impl TryFrom<&str> for OpenAIResponsesTools {
    type Error = LlmError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "code_interpreter" => Ok(OpenAIResponsesTools::CodeInterpreter),
            "web_search" => Ok(OpenAIResponsesTools::WebSearch),
            _ => Err(LlmError::ToolNotFound {
                tool: value.to_string(),
            }),
        }
    }
}

pub struct ToolUsingResponses {
    inner_responses: Arc<dyn Responses>,
    openai_tools: Vec<OpenAIResponsesTools>,
}

impl ToolUsingResponses {
    #[must_use]
    pub fn new(
        inner_responses: Arc<dyn Responses>,
        openai_tools: Vec<OpenAIResponsesTools>,
    ) -> Self {
        Self {
            inner_responses,
            openai_tools,
        }
    }

    fn prepare_req(&self, mut req: CreateResponse) -> CreateResponse {
        let tool_definitions: Vec<ToolDefinition> = self
            .openai_tools
            .clone()
            .into_iter()
            .map(Into::into)
            .collect();
        req.tools = Some(tool_definitions);
        req
    }
}

#[async_trait]
impl Responses for ToolUsingResponses {
    async fn health(&self) -> Result<(), ResponsesError> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "health", input = "health");

        let mut req = CreateResponseArgs::default()
            .input("ping")
            .build()
            .boxed()
            .context(FailedToLoadModelSnafu)?;

        req.max_output_tokens = Some(150);

        if let Err(e) = self.responses_request(req).instrument(span.clone()).await {
            tracing::error!(target: "task_history", parent: &span, "{e}");
            return Err(ResponsesError::HealthCheckError { source: e.into() });
        }
        Ok(())
    }

    async fn responses_stream(&self, req: CreateResponse) -> Result<ResponseStream, OpenAIError> {
        self.inner_responses
            .responses_stream(self.prepare_req(req))
            .await
    }

    async fn responses_request(&self, req: CreateResponse) -> Result<Response, OpenAIError> {
        self.inner_responses
            .responses_request(self.prepare_req(req))
            .await
    }
}
