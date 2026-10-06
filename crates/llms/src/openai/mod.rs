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

use std::num::NonZeroU32;
use std::str::FromStr;
use std::sync::Arc;

use async_openai::config::{AzureConfig, Config, OPENAI_API_BASE};
use async_openai::{Client, config::OpenAIConfig};
use governor::Quota;
use runtime_rate_control::{JitterConfig, RateController};

pub mod chat;
pub mod embed;
pub mod list_models;
pub mod responses;
mod responses_adapter;

pub use list_models::OpenAiModelLister;

pub const MAX_COMPLETION_TOKENS: u16 = 1024_u16; // Avoid accidentally using infinite tokens. Should think about this more.

pub(crate) const GPT_4O_MINI: &str = "gpt-4o-mini";
pub(crate) const TEXT_EMBED_3_SMALL: &str = "text-embedding-3-small";

pub const DEFAULT_LLM_MODEL: &str = GPT_4O_MINI;
pub const DEFAULT_EMBEDDING_MODEL: &str = TEXT_EMBED_3_SMALL;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ChatBackend {
    #[default]
    ChatCompletions,
    Responses,
}

impl ChatBackend {
    /// The values accepted in a Spicepod. The parameter spec validates against this same
    /// slice, so the documented vocabulary and the parsed one cannot drift.
    pub const VALUES: &'static [&'static str] = &["enabled", "disabled"];
}

impl FromStr for ChatBackend {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "disabled" => Ok(Self::ChatCompletions),
            "enabled" => Ok(Self::Responses),
            other => Err(format!(
                "must be one of: {}. Found {other}",
                Self::VALUES.join(", ")
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UsageTier {
    Free,
    #[default]
    Tier1,
    Tier2,
    Tier3,
    Tier4,
    Tier5,
}

impl FromStr for UsageTier {
    type Err = crate::embeddings::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "free" => Ok(UsageTier::Free),
            "tier1" => Ok(UsageTier::Tier1),
            "tier2" => Ok(UsageTier::Tier2),
            "tier3" => Ok(UsageTier::Tier3),
            "tier4" => Ok(UsageTier::Tier4),
            "tier5" => Ok(UsageTier::Tier5),
            _ => Err(crate::embeddings::Error::InvalidOpenAITier {
                tier: s.to_string(),
            }),
        }
    }
}

impl From<UsageTier> for Arc<RateController> {
    fn from(val: UsageTier) -> Self {
        let max_concurrent_requests = match &val {
            &UsageTier::Free => 1,
            &UsageTier::Tier1 => 35,
            &UsageTier::Tier2 | &UsageTier::Tier3 => 60,
            &UsageTier::Tier4 | &UsageTier::Tier5 => 125,
        };

        let per_minute_quota = match &val {
            &UsageTier::Free => 100,
            &UsageTier::Tier1 => 3000,
            &UsageTier::Tier2 | &UsageTier::Tier3 => 5000,
            &UsageTier::Tier4 | &UsageTier::Tier5 => 10000,
        };

        let Some(per_minute_quota) = NonZeroU32::new(per_minute_quota) else {
            unreachable!("per_minute_quota for usage tiers are non-zero");
        };

        RateController::builder()
            .with_max_concurrent_requests(max_concurrent_requests)
            .add_quota(Quota::per_minute(per_minute_quota))
            .build()
    }
}

#[derive(Debug, Clone)]
pub struct Openai<C: Config + Clone> {
    client: Client<C>,
    model: String,

    rate_controller: Arc<RateController>,
    chat_backend: ChatBackend,
}

pub(crate) fn default_rate_controller() -> Arc<RateController> {
    let Some(default_per_minute_quota) = NonZeroU32::new(500) else {
        unreachable!("Default quota should always be non-zero");
    };

    RateController::builder()
        .with_jitter(JitterConfig::zero())
        .with_max_concurrent_requests(4)
        .add_quota(Quota::per_minute(default_per_minute_quota))
        .build()
}

#[must_use]
pub fn new_azure_client(
    model: String,
    api_base: Option<&str>,
    api_version: Option<&str>,
    deployment_name: Option<&str>,
    entra_token: Option<&str>,
    api_key: Option<&str>,
) -> Openai<AzureConfig> {
    new_azure_client_with_chat_backend(
        model,
        api_base,
        api_version,
        deployment_name,
        entra_token,
        api_key,
        ChatBackend::ChatCompletions,
    )
}

#[must_use]
pub fn new_azure_client_with_chat_backend(
    model: String,
    api_base: Option<&str>,
    api_version: Option<&str>,
    deployment_name: Option<&str>,
    entra_token: Option<&str>,
    api_key: Option<&str>,
    chat_backend: ChatBackend,
) -> Openai<AzureConfig> {
    let mut cfg = AzureConfig::new().with_deployment_id(deployment_name.unwrap_or(model.as_str()));

    if let Some(api_base) = api_base {
        cfg = cfg.with_api_base(api_base);
    }

    if let Some(api_version) = api_version {
        cfg = cfg.with_api_version(api_version);
    }

    if let Some(api_key) = api_key {
        cfg = cfg.with_api_key(api_key);
    }

    if let Some(entra_token) = entra_token {
        cfg = cfg.with_entra_token(entra_token);
    }

    Openai {
        client: Client::with_config(cfg),
        model,
        rate_controller: default_rate_controller(),
        chat_backend,
    }
}

#[must_use]
pub fn new_openai_client(
    model: String,
    api_base: Option<&str>,
    api_key: Option<&str>,
    org_id: Option<&str>,
    project_id: Option<&str>,
    usage_tier: Option<UsageTier>,
) -> Openai<OpenAIConfig> {
    new_openai_client_with_chat_backend(
        model,
        api_base,
        api_key,
        org_id,
        project_id,
        usage_tier,
        ChatBackend::ChatCompletions,
    )
}

#[must_use]
pub fn new_openai_client_with_chat_backend(
    model: String,
    api_base: Option<&str>,
    api_key: Option<&str>,
    org_id: Option<&str>,
    project_id: Option<&str>,
    usage_tier: Option<UsageTier>,
    chat_backend: ChatBackend,
) -> Openai<OpenAIConfig> {
    // Default to empty API key to avoid picking up ENV variable in downstream library.
    let mut cfg = OpenAIConfig::new().with_api_key("");

    if let Some(org_id) = org_id {
        cfg = cfg.with_org_id(org_id);
    }

    if let Some(project_id) = project_id {
        cfg = cfg.with_project_id(project_id);
    }

    if let Some(api_key) = api_key {
        cfg = cfg.with_api_key(api_key);
    }
    if let Some(api_base) = api_base {
        cfg = cfg.with_api_base(api_base);
    }

    Openai {
        client: Client::with_config(cfg),
        model,
        rate_controller: usage_tier.map_or_else(default_rate_controller, Into::into),
        chat_backend,
    }
}

#[must_use]
pub fn new_openai_client_with_config<C: async_openai::config::Config + Clone>(
    model: String,
    cfg: C,
) -> Openai<C> {
    Openai {
        client: Client::with_config(cfg),
        model,
        rate_controller: default_rate_controller(),
        chat_backend: ChatBackend::ChatCompletions,
    }
}

impl<C: Config + Clone> Openai<C> {
    /// Returns true if the `OpenAI` compatible model supports [structured outputs](https://platform.openai.com/docs/guides/structured-outputs/).
    /// This is only supported for GPT-4o models from `OpenAI` (i.e not any other compatible servers).
    fn supports_structured_output(&self) -> bool {
        self.client.config().api_base() == OPENAI_API_BASE && self.model.starts_with("gpt-4o")
    }

    /// Returns true if the `OpenAI` compatible model supports `max_completion_tokens` in [`CreateChatCompletionRequest`].
    ///
    /// This is useful for limiting the number of tokens used in health checks.
    fn supports_max_completion_tokens(&self) -> bool {
        self.client.config().api_base() == OPENAI_API_BASE
    }

    /// Returns true if the `OpenAI` compatible model supports reasoning.
    /// This is only supported for GPT-5 models from `OpenAI` (i.e not any other compatible servers).
    /// <https://platform.openai.com/docs/api-reference/chat/create#chat-create-reasoning_effort>
    fn supports_reasoning_effort(&self) -> bool {
        self.client.config().api_base() == OPENAI_API_BASE
            && (self.model.starts_with("gpt-5")
                || self.model.starts_with("o3")
                || self.model.starts_with("o4"))
    }
}

#[cfg(test)]
mod chat_backend_tests {
    use super::ChatBackend;

    #[test]
    fn parses_the_documented_values_case_insensitively() {
        for (raw, expected) in [
            ("disabled", ChatBackend::ChatCompletions),
            ("Disabled", ChatBackend::ChatCompletions),
            ("  disabled ", ChatBackend::ChatCompletions),
            ("enabled", ChatBackend::Responses),
            ("ENABLED", ChatBackend::Responses),
        ] {
            assert_eq!(
                raw.parse::<ChatBackend>()
                    .unwrap_or_else(|e| panic!("{raw:?} should parse: {e}")),
                expected,
                "{raw:?}"
            );
        }
    }

    #[test]
    fn rejects_values_outside_the_spec() {
        for raw in ["", "legacy", "true", "false"] {
            assert!(
                raw.parse::<ChatBackend>().is_err(),
                "{raw:?} should be rejected"
            );
        }
    }

    /// The parameter spec advertises `VALUES`, so every entry has to parse — otherwise
    /// config validation accepts a value the parser then rejects.
    #[test]
    fn every_advertised_value_parses() {
        for value in ChatBackend::VALUES {
            value
                .parse::<ChatBackend>()
                .unwrap_or_else(|e| panic!("{value:?} is advertised but does not parse: {e}"));
        }
    }
}

/// Guards the `async-openai` fork patch that only sends `Authorization` when there
/// is a key to send.
///
/// Upstream inserts the header unconditionally. Spice builds every `OpenAI` client
/// through [`new_openai_client_with_chat_backend`], which starts from
/// `with_api_key("")` on purpose — so the downstream library cannot pick a key up
/// from the environment — and overrides it only when a key was configured. Without
/// the patch, a dataset or model with no `api_key` therefore sends
/// `Authorization: Bearer ` with an empty value to every request, and an
/// OpenAI-compatible endpoint that needs no key rejects the malformed credential
/// instead of serving the request.
///
/// Both halves are asserted, because the first alone would pass on a client that
/// had stopped sending the header at all: with no key there must be no
/// `Authorization` header, and with a key it must carry it as a bearer token.
#[cfg(test)]
#[expect(
    clippy::expect_used,
    reason = "a failed set-up in a test should name itself and stop"
)]
mod authorization_header_tests {
    use std::io::{Read as _, Write as _};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::time::Duration;

    use super::{ChatBackend, new_openai_client_with_chat_backend};
    use crate::chat::Chat as _;

    /// Stand a one-shot HTTP server up and hand back its base URL together with a
    /// channel carrying the request headers it receives.
    fn capture_one_request() -> (String, mpsc::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind a local port");
        let port = listener
            .local_addr()
            .expect("read the bound address")
            .port();
        let (tx, rx) = mpsc::channel();

        std::thread::spawn(move || {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            // Read to the end of the headers, which is all that is under assertion.
            let mut seen = Vec::new();
            let mut byte = [0_u8; 1];
            while stream.read(&mut byte).unwrap_or(0) == 1 {
                seen.push(byte[0]);
                if seen.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            let _ = tx.send(String::from_utf8_lossy(&seen).into_owned());

            // Any answer will do: the request that arrived is the observation, not
            // what the client makes of the reply.
            let body = "{}";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        });

        (format!("http://127.0.0.1:{port}/v1"), rx)
    }

    /// Drive a health check through a client built the way the runtime builds one,
    /// and return the request headers the endpoint saw.
    async fn request_headers_for(api_key: Option<&str>) -> String {
        let (api_base, rx) = capture_one_request();
        let client = new_openai_client_with_chat_backend(
            "gpt-4o-mini".to_string(),
            Some(&api_base),
            api_key,
            None,
            None,
            None,
            ChatBackend::ChatCompletions,
        );
        // The reply is not a chat completion, so this fails; the request it sent
        // first is what the assertions read.
        let _ = tokio::time::timeout(Duration::from_secs(20), client.health()).await;
        rx.recv_timeout(Duration::from_secs(20))
            .expect("the endpoint received a request")
    }

    #[tokio::test]
    async fn a_client_with_no_api_key_sends_no_authorization_header() {
        let headers = request_headers_for(None).await;
        assert!(
            !headers.to_ascii_lowercase().contains("authorization:"),
            "a model configured with no api_key sent an Authorization header, which carries an \
             empty bearer token and is refused by an endpoint that needs no key:\n{headers}"
        );
    }

    #[tokio::test]
    async fn a_client_with_an_api_key_sends_it_as_a_bearer_token() {
        let headers = request_headers_for(Some("sk-guard-token")).await;
        assert!(
            headers.contains("Bearer sk-guard-token"),
            "the control: a configured api_key has to reach the endpoint as a bearer token, \
             otherwise the assertion above passes on a client that sends no credential at \
             all:\n{headers}"
        );
    }
}
