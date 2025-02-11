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

use std::collections::HashMap;

use async_openai::{error::OpenAIError, Client};
use futures::{StreamExt, TryStreamExt};
use reqwest_eventsource::Error as SseError;
use secrecy::{ExposeSecret, SecretString};
use types::{
    PerplexityRequest, PerplexityRequestParameters, PerplexityResponse, PerplexityResponseStream,
    PerplexityStreamResponse,
};

use crate::config::{GenericAuthMechanism, HostedModelConfig};

pub mod chat;
pub mod types;

pub struct PerplexitySonar {
    client: Client<HostedModelConfig>,
    model: String,
    overrides: Vec<(String, String)>,
}

static PERPLEXITY_SONAR_API_BASE: &str = "https://api.perplexity.ai";
static PERPLEXITY_SONAR_DEFAULT_MODEL: &str = "sonar";

impl PerplexitySonar {
    pub fn from_params(
        model: Option<&str>,
        params: &HashMap<String, SecretString>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let Some(auth_token) = params.get("perplexity_auth_token") else {
            return Err(Box::from(
                "No `perplexity_auth_token` provided for Perplexity model.",
            ));
        };

        let overrides: Vec<(String, String)> = params
            .iter()
            .filter_map(|(k, v)| {
                if k != "perplexity_auth_token" {
                    if let Some(p) = k.strip_prefix("perplexity_") {
                        return Some((p.to_string(), v.expose_secret().clone()));
                    }
                };
                None
            })
            .collect();

        let cfg = HostedModelConfig::default()
            .with_auth(GenericAuthMechanism::BearerToken(auth_token.clone()))
            .with_base_url(PERPLEXITY_SONAR_API_BASE);

        Ok(Self {
            client: Client::<HostedModelConfig>::with_config(cfg),
            model: model.unwrap_or(PERPLEXITY_SONAR_DEFAULT_MODEL).to_string(),
            overrides,
        })
    }

    #[must_use]
    pub fn with_overrides(&self, mut req: PerplexityRequest) -> PerplexityRequest {
        if let Some(params) = req.extra_parameters.as_mut() {
            params.update_overrides(&self.overrides);

        // Still need to set overrides if user-request contains no perplexity parameters.
        } else if !self.overrides.is_empty() {
            let mut p = PerplexityRequestParameters::default();
            p.update_overrides(&self.overrides);
            req.extra_parameters = Some(p);
        }
        req
    }

    pub async fn search_request(
        &self,
        mut req: PerplexityRequest,
    ) -> Result<PerplexityResponse, OpenAIError> {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "citations",
            model = %req.chat.model,
            input = %serde_json::to_string(&req.chat.messages).unwrap_or_default()
        );

        req.chat.model.clone_from(&self.model);
        req = self.with_overrides(req);

        let resp: Result<PerplexityResponse, OpenAIError> =
            self.client.post("/chat/completions", req).await;

        if let Ok(ref r) = resp {
            tracing::info!(target: "task_history", parent: &span, captured_output = %format!("{:?}", r.citations));
        }

        resp
    }

    pub async fn search_stream(&self, mut req: PerplexityRequest) -> PerplexityResponseStream {
        let span = tracing::span!(target: "task_history", tracing::Level::INFO, "citations",
            model = %req.chat.model,
            input = %serde_json::to_string(&req.chat.messages).unwrap_or_default()
        );

        req.chat.model.clone_from(&self.model);
        req = self.with_overrides(req);
        let span_stream = span.clone();

        Box::pin(self
            .client
            .post_stream("/chat/completions", req)
            .await
            .inspect_ok(move |r: &PerplexityStreamResponse|  {
                if !span_stream.has_field("captured_output") {
                    tracing::info!(target: "task_history", parent: &span_stream, captured_output = %format!("{:?}", r.citations));
                }
            })
            // Perplexity does not send "Done" messages as per SSE protocol.
            // Stop stream manually on `Stream ended` error.
            .take_while(|item| {
                let stream_ended = matches!(item, Err(OpenAIError::StreamError(message))
                            if SseError::StreamEnded{}.to_string().eq(message));

                futures::future::ready(!stream_ended)
            }))
    }
}
