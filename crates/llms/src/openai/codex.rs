/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

use std::any::Any;
use std::sync::Arc;

use async_openai::Client;
use async_openai::error::OpenAIError;
use async_openai::types::chat::{
    ChatCompletionResponseStream, CreateChatCompletionRequest, CreateChatCompletionResponse,
};
use async_openai::types::responses::{CreateResponse, Response, ResponseStream};
use async_trait::async_trait;
use reqwest::header::{
    ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue, USER_AGENT,
};
use runtime_rate_control::RateController;
use runtime_request_context::{AsyncMarker, Extension, RequestContext};

use crate::chat::{Chat, Error as ChatError};
use crate::config::HostedModelConfig;
use crate::openai::responses_adapter;
use crate::responses::{Responses, Result as ResponsesResult};

const CHATGPT_ACCOUNT_ID: HeaderName = HeaderName::from_static("chatgpt-account-id");
const ORIGINATOR: HeaderName = HeaderName::from_static("originator");
const SESSION_ID: HeaderName = HeaderName::from_static("session-id");
const THREAD_ID: HeaderName = HeaderName::from_static("thread-id");
const X_CLIENT_REQUEST_ID: HeaderName = HeaderName::from_static("x-client-request-id");
const X_CODEX_BETA_FEATURES: HeaderName = HeaderName::from_static("x-codex-beta-features");
const X_CODEX_TURN_METADATA: HeaderName = HeaderName::from_static("x-codex-turn-metadata");
const X_CODEX_WINDOW_ID: HeaderName = HeaderName::from_static("x-codex-window-id");

const FORWARDED_HEADERS: &[HeaderName] = &[
    ACCEPT,
    AUTHORIZATION,
    CHATGPT_ACCOUNT_ID,
    CONTENT_TYPE,
    ORIGINATOR,
    SESSION_ID,
    THREAD_ID,
    USER_AGENT,
    X_CLIENT_REQUEST_ID,
    X_CODEX_BETA_FEATURES,
    X_CODEX_TURN_METADATA,
    X_CODEX_WINDOW_ID,
];

/// The Codex request headers that may be forwarded to the Codex backend.
///
/// This owns only an explicit allowlist. In particular, transport-controlled
/// headers such as `host` and `content-length` are never forwarded.
#[derive(Clone)]
pub struct CodexRequestHeaders {
    headers: HeaderMap,
}

impl CodexRequestHeaders {
    #[must_use]
    pub fn from_headers(source: &HeaderMap) -> Self {
        let mut headers = HeaderMap::with_capacity(FORWARDED_HEADERS.len());
        for name in FORWARDED_HEADERS {
            for value in source.get_all(name).iter() {
                headers.append(name.clone(), value.clone());
            }
        }
        Self { headers }
    }

    #[must_use]
    fn authorization_present(&self) -> bool {
        self.headers.contains_key(AUTHORIZATION)
    }

    #[must_use]
    fn headers(&self) -> HeaderMap {
        self.headers.clone()
    }
}

#[async_trait]
impl Extension for CodexRequestHeaders {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// An OpenAI Responses API client authenticated by a Codex caller's headers.
pub struct Codex {
    api_base: String,
    model: String,
    rate_controller: Arc<RateController>,
}

impl Codex {
    #[must_use]
    pub(super) fn new(
        model: String,
        api_base: String,
        rate_controller: Arc<RateController>,
    ) -> Self {
        Self {
            api_base,
            model,
            rate_controller,
        }
    }

    async fn client(&self) -> Result<Client<HostedModelConfig>, OpenAIError> {
        let context = RequestContext::current(AsyncMarker::new().await);
        let headers = context.extension::<CodexRequestHeaders>().ok_or_else(|| {
            OpenAIError::InvalidArgument(
                "Codex authentication headers are required. Use Codex to call this gateway."
                    .to_string(),
            )
        })?;

        if !headers.authorization_present() {
            return Err(OpenAIError::InvalidArgument(
                "Codex authorization is required. Sign in with Codex and retry.".to_string(),
            ));
        }

        let config = HostedModelConfig::from_url(&self.api_base).with_headers(headers.headers());
        Ok(Client::with_config(config))
    }
}

#[async_trait]
impl Responses for Codex {
    async fn health(&self) -> ResponsesResult<()> {
        Ok(())
    }

    async fn responses_stream(
        &self,
        mut req: CreateResponse,
    ) -> ResponsesResult<ResponseStream, OpenAIError> {
        req.model = Some(self.model.clone());
        let client = self.client().await?;
        let permit = self
            .rate_controller
            .acquire()
            .await
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;
        let stream = client.responses().create_stream(req).await?;
        drop(permit);
        Ok(Box::pin(stream))
    }

    async fn responses_request(
        &self,
        mut req: CreateResponse,
    ) -> ResponsesResult<Response, OpenAIError> {
        req.model = Some(self.model.clone());
        let client = self.client().await?;
        let permit = self
            .rate_controller
            .acquire()
            .await
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;
        let response = client.responses().create(req).await?;
        drop(permit);
        Ok(response)
    }
}

#[async_trait]
impl Chat for Codex {
    async fn chat_stream(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        let outer_model = req.model.clone();
        let inner_req =
            responses_adapter::responses_request_from_chat_completion_request(req, &self.model)?;
        let client = self.client().await?;
        let permit = self
            .rate_controller
            .acquire()
            .await
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;
        let stream = client.responses().create_stream(inner_req).await?;
        drop(permit);
        Ok(responses_adapter::chat_completion_stream_from_response_stream(stream, outer_model))
    }

    async fn health(&self) -> Result<(), ChatError> {
        Ok(())
    }

    async fn chat_request(
        &self,
        req: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        let outer_model = req.model.clone();
        let inner_req =
            responses_adapter::responses_request_from_chat_completion_request(req, &self.model)?;
        let client = self.client().await?;
        let permit = self
            .rate_controller
            .acquire()
            .await
            .map_err(|e| OpenAIError::InvalidArgument(e.to_string()))?;
        let response = client.responses().create(inner_req).await?;
        drop(permit);
        responses_adapter::chat_completion_response_from_response(response, outer_model)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forwards_only_the_codex_header_allowlist() {
        let mut source = HeaderMap::new();
        source.insert(AUTHORIZATION, HeaderValue::from_static("Bearer token"));
        source.insert(CHATGPT_ACCOUNT_ID, HeaderValue::from_static("account"));
        source.insert("host", HeaderValue::from_static("gateway.example"));
        source.insert("content-length", HeaderValue::from_static("42"));
        source.insert("x-unrelated", HeaderValue::from_static("not-forwarded"));

        let forwarded = CodexRequestHeaders::from_headers(&source).headers();

        assert_eq!(
            forwarded.get(AUTHORIZATION),
            Some(&HeaderValue::from_static("Bearer token"))
        );
        assert_eq!(
            forwarded.get(CHATGPT_ACCOUNT_ID),
            Some(&HeaderValue::from_static("account"))
        );
        assert!(!forwarded.contains_key("host"));
        assert!(!forwarded.contains_key("content-length"));
        assert!(!forwarded.contains_key("x-unrelated"));
    }
}
