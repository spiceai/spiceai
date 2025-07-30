/*
Copyright 2024 The Spice.ai OSS Authors

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

use async_openai::{
    Client,
    error::OpenAIError,
    types::{
        ChatChoiceStream, ChatCompletionRequestMessage, ChatCompletionRequestUserMessage,
        ChatCompletionRequestUserMessageContent, ChatCompletionResponseStream,
        CompletionTokensDetails, CompletionUsage, CreateChatCompletionRequest,
        CreateChatCompletionResponse, CreateChatCompletionStreamResponse, CreateEmbeddingRequest,
        CreateEmbeddingResponse, EmbeddingInput, PromptTokensDetails, ServiceTierResponse,
    },
};
use async_trait::async_trait;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use snafu::ResultExt;
use std::sync::Arc;
use token_provider::TokenProvider;
use tracing::Instrument;

use crate::{
    HealthCheck,
    chat::{Chat, nsql::SqlGeneration},
    config::{GenericAuthMechanism, HostedModelConfig},
    embeddings::Embed,
};

/// [`Databricks`] is provides both [`Chat`] and [`Embed`] capabilities for Databricks models.
pub struct Databricks {
    pub model: String,
    client: Client<HostedModelConfig>,
    health_check: HealthCheck,
}
impl Databricks {
    /// Changes to `req` to accomodate Databricks not being `OpenAI` compatible.
    fn alter_request(&self, mut req: CreateChatCompletionRequest) -> CreateChatCompletionRequest {
        req.model.clone_from(&self.model);
        // Databricks should set Option::None parameters to a schema with no inputs, but doesn't.
        // Must be done explicitly.
        if let Some(ref mut tools) = req.tools {
            for t in tools.iter_mut() {
                if t.function.parameters.is_none() {
                    t.function.parameters.replace(json!(
                        {
                            "$schema": "http://json-schema.org/draft-07/schema#",
                            "properties": {},
                            "required": [],
                            "title": "",
                            "type": "object"
                        }
                    ));
                }

                // For tools that want to have Uint as inputs, they will set `minimum=0`.
                // This is valid JSON schema, but not supported in Databricks.
                if let Some(Some(serde_json::Value::Object(properties))) = t
                    .function
                    .parameters
                    .as_mut()
                    .map(|v| v.get_mut("properties"))
                {
                    for (_field, value) in properties.iter_mut() {
                        if let Some(Value::String(value_type)) = value.get("type") {
                            if value_type != "integer" {
                                continue;
                            }
                            if let Some(value_map) = value.as_object_mut() {
                                value_map.remove("minimum");
                            }
                        }
                    }
                }
            }
        }
        req
    }
}

#[must_use]
pub fn from_access_token(
    endpoint: &str,
    model: &str,
    access_token: &str,
    user_agent: Option<&'static str>,
) -> Databricks {
    let mut cfg = HostedModelConfig::from_url(
        format!("https://token:{access_token}@{endpoint}/serving-endpoints/{model}/invocations")
            .as_str(),
    );

    if let Some(user_agent) = user_agent {
        cfg = cfg.with_header("user-agent", user_agent);
    }

    Databricks {
        model: model.to_string(),
        client: Client::with_config(cfg),
        health_check: HealthCheck::Required,
    }
}

pub fn from_token_provider(
    endpoint: &str,
    model: &str,
    token_provider: Arc<dyn TokenProvider>,
    user_agent: Option<&'static str>,
    health_check: HealthCheck,
) -> Databricks {
    let mut cfg = HostedModelConfig::from_url(
        format!("https://{endpoint}/serving-endpoints/{model}/invocations").as_str(),
    )
    .with_auth(GenericAuthMechanism::from_bearer_token_provider(
        token_provider,
    ));

    if let Some(user_agent) = user_agent {
        cfg = cfg.with_header("user-agent", user_agent);
    }

    Databricks {
        model: model.to_string(),
        client: Client::with_config(cfg),
        health_check,
    }
}

#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
pub struct DatabricksCreateChatCompletionStreamResponse {
    /// The same as [`CreateChatCompletionStreamResponse`]
    pub id: String,
    pub choices: Vec<ChatChoiceStream>,
    pub created: u32,
    pub model: String,
    pub service_tier: Option<ServiceTierResponse>,
    pub system_fingerprint: Option<String>,
    pub object: String,

    /// Usage is different in Databricks
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<DatabricksCompletionUsage>,
}

impl From<DatabricksCreateChatCompletionStreamResponse> for CreateChatCompletionStreamResponse {
    fn from(val: DatabricksCreateChatCompletionStreamResponse) -> Self {
        let DatabricksCreateChatCompletionStreamResponse {
            id,
            choices,
            created,
            model,
            service_tier,
            system_fingerprint,
            object,
            usage,
        } = val;
        CreateChatCompletionStreamResponse {
            id,
            choices,
            created,
            model,
            service_tier,
            system_fingerprint,
            object,
            usage: usage.map(Into::into),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct DatabricksCompletionUsage {
    pub prompt_tokens: Option<u32>,
    pub completion_tokens: Option<u32>,
    pub total_tokens: Option<u32>,
    pub prompt_tokens_details: Option<PromptTokensDetails>,
    pub completion_tokens_details: Option<CompletionTokensDetails>,
}

impl From<DatabricksCompletionUsage> for CompletionUsage {
    fn from(val: DatabricksCompletionUsage) -> Self {
        CompletionUsage {
            prompt_tokens: val.prompt_tokens.unwrap_or_default(),
            completion_tokens: val.completion_tokens.unwrap_or_default(),
            total_tokens: val.total_tokens.unwrap_or_default(),
            prompt_tokens_details: val.prompt_tokens_details,
            completion_tokens_details: val.completion_tokens_details,
        }
    }
}

#[async_trait]
impl Chat for Databricks {
    fn as_sql(&self) -> Option<&dyn SqlGeneration> {
        None
    }

    /// [`Databricks`] doesn't support `max_completion_tokens`. Must define own health function.
    #[allow(deprecated)]
    async fn health(&self) -> super::chat::Result<()> {
        if matches!(self.health_check, HealthCheck::Skip) {
            return Ok(());
        }

        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "health", input = "health");

        if let Err(e) = self
            .chat_request(CreateChatCompletionRequest {
                // Cannot be set too low. Some providers will error if it cannot complete in < `max_tokens`.
                max_tokens: Some(100),
                messages: vec![ChatCompletionRequestMessage::User(
                    ChatCompletionRequestUserMessage {
                        name: None,
                        content: ChatCompletionRequestUserMessageContent::Text("ping.".to_string()),
                    },
                )],
                ..Default::default()
            })
            .instrument(span.clone())
            .await
        {
            tracing::error!(target: "task_history", parent: &span, "{e}");
            return Err(super::chat::Error::HealthCheckError {
                source: Box::new(e),
            });
        }
        Ok(())
    }

    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let mut inner_req = req.clone();
        inner_req.model.clone_from(&self.model);
        inner_req.stream_options = None; // Not supported by Databricks.

        // Must use `post_stream` instead of `chat().create(...` to avoid concatenation of `chat/completions`.
        Ok(Box::pin(
            self.client
                .post_stream::<_, DatabricksCreateChatCompletionStreamResponse, _>(
                    "",
                    self.alter_request(req),
                )
                .await
                .map_ok(Into::into),
        ))
    }

    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        self.client.post("", self.alter_request(req)).await
    }
}

#[async_trait]
impl Embed for Databricks {
    async fn health(&self) -> super::embeddings::Result<()> {
        if matches!(self.health_check, HealthCheck::Skip) {
            return Ok(());
        }

        self.embed(EmbeddingInput::String("health".to_string()))
            .await
            .boxed()
            .map_err(|source| super::embeddings::Error::HealthCheckError { source })?;

        Ok(())
    }

    async fn embed_request(
        &self,
        req: CreateEmbeddingRequest,
    ) -> Result<CreateEmbeddingResponse, OpenAIError> {
        // Must use `post` instead of `embeddings().create(...` to avoid concatenation of `/embeddings`.
        self.client.post("", req).await
    }
    fn size(&self) -> i32 {
        -1
    }

    async fn embed(&self, input: EmbeddingInput) -> crate::embeddings::Result<Vec<Vec<f32>>> {
        let resp = self
            .embed_request(CreateEmbeddingRequest {
                model: self.model.clone(),
                input,
                encoding_format: None,
                user: None,
                dimensions: None,
            })
            .await
            .boxed()
            .map_err(|e| crate::embeddings::Error::FailedToCreateEmbedding { source: e })?;

        Ok(resp
            .data
            .into_iter()
            .map(|emb| emb.embedding.into())
            .collect())
    }
}

impl std::fmt::Debug for Databricks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabricksEmbed")
            .field("inner", &self.client)
            .finish_non_exhaustive()
    }
}
