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

//! A hosted model's metadata, read from its provider's model API when it loads.

use std::time::Duration;

use async_openai::config::Config;
use chat_api::model_info::{context_window_of, listed_model};
use serde_json::Value;

/// How long the provider has to answer. The metadata is optional, so a slow or unreachable
/// model API costs a model's load this much at most, and leaves its window unknown.
const METADATA_TIMEOUT: Duration = Duration::from_secs(5);

/// The context window the provider's model listing, `GET {base}/models`, reports for
/// `model`: the shape of the `OpenAI` API and the servers compatible with it.
pub(crate) async fn listed_context_window<C: Config>(config: &C, model: &str) -> Option<u64> {
    let listing = get_json(config, "/models").await?;
    let window = listed_model(&listing, model).and_then(context_window_of);
    if window.is_none() {
        tracing::debug!(
            "The model listing of '{}' reports no context window for model '{model}'",
            config.api_base()
        );
    }
    window
}

/// The context window the provider's model object, `GET {base}/models/{model}`, reports:
/// the shape of Anthropic's Models API.
pub(crate) async fn retrieved_context_window<C: Config>(config: &C, model: &str) -> Option<u64> {
    let model_object = get_json(config, &format!("/models/{model}")).await?;
    context_window_of(&model_object)
}

async fn get_json<C: Config>(config: &C, path: &str) -> Option<Value> {
    let client = reqwest::Client::builder()
        .timeout(METADATA_TIMEOUT)
        .build()
        .ok()?;
    let url = config.url(path);
    let response = client
        .get(&url)
        .headers(config.headers())
        .query(&config.query())
        .send()
        .await;
    match response {
        Ok(response) if response.status().is_success() => response.json().await.ok(),
        Ok(response) => {
            tracing::debug!(
                "Model metadata request to '{url}' answered {}",
                response.status()
            );
            None
        }
        Err(error) => {
            tracing::debug!("Model metadata request to '{url}' failed: {error}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_openai::config::OpenAIConfig;
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn config(server: &MockServer) -> OpenAIConfig {
        OpenAIConfig::new().with_api_base(format!("{}/v1", server.uri()))
    }

    #[tokio::test]
    async fn an_openai_compatible_listing_reports_the_window_of_its_model() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                json!({"object": "list", "data": [
                    {"id": "other", "max_model_len": 1},
                    {"id": "qwen", "meta": {"n_ctx": 65_536}},
                ]}),
            ))
            .mount(&server)
            .await;
        assert_eq!(
            listed_context_window(&config(&server), "qwen").await,
            Some(65_536)
        );
        assert_eq!(
            listed_context_window(&config(&server), "missing").await,
            None
        );
    }

    #[tokio::test]
    async fn a_model_object_reports_its_window() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models/claude-opus"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                json!({"id": "claude-opus", "max_input_tokens": 200_000, "max_tokens": 64_000}),
            ))
            .mount(&server)
            .await;
        assert_eq!(
            retrieved_context_window(&config(&server), "claude-opus").await,
            Some(200_000)
        );
    }

    #[tokio::test]
    async fn a_failing_model_api_leaves_the_window_unknown() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        assert_eq!(listed_context_window(&config(&server), "qwen").await, None);
        // Nothing listening at all.
        let unreachable = OpenAIConfig::new().with_api_base("http://127.0.0.1:9/v1");
        assert_eq!(listed_context_window(&unreachable, "qwen").await, None);
    }
}
