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

//! Chat completions against the OrcaRouter AI gateway.

use crate::config::HostedModelConfig;
use crate::openai::{Openai, new_openai_client_with_config};

/// The OrcaRouter AI gateway.
pub const DEFAULT_ENDPOINT: &str = "https://api.orcarouter.ai";

/// Resolves an OrcaRouter endpoint to the base URL of its `OpenAI`-compatible API.
///
/// The gateway serves the `OpenAI`-compatible API under `/v1`. An endpoint that already
/// names `/v1` is taken as-is, so either spelling resolves to the same base URL.
#[must_use]
pub fn api_base(endpoint: Option<&str>) -> String {
    let root = endpoint
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .unwrap_or(DEFAULT_ENDPOINT)
        .trim_end_matches('/');

    if root.ends_with("/v1") {
        root.to_string()
    } else {
        format!("{root}/v1")
    }
}

/// Creates a chat client for a model served by the OrcaRouter AI gateway.
///
/// OrcaRouter authenticates with a bearer token (`sk-orca-...`), which `HostedModelConfig`
/// sends as an `Authorization: Bearer` header — matching the gateway's requirement (unlike
/// `x-api-key`, which OrcaRouter rejects).
#[must_use]
pub fn new_orcarouter_client(
    model: String,
    endpoint: Option<&str>,
    api_key: Option<&str>,
) -> Openai<HostedModelConfig> {
    let config = HostedModelConfig::from_url(&api_base(endpoint)).with_bearer_token(api_key);
    new_openai_client_with_config(model, config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_openai::config::Config;

    #[test]
    fn api_base_defaults_to_the_orcarouter_gateway() {
        assert_eq!(api_base(None), "https://api.orcarouter.ai/v1");
        assert_eq!(api_base(Some("")), "https://api.orcarouter.ai/v1");
        assert_eq!(api_base(Some("   ")), "https://api.orcarouter.ai/v1");
    }

    #[test]
    fn api_base_appends_the_openai_compatible_path() {
        assert_eq!(
            api_base(Some("https://api.orcarouter.ai")),
            "https://api.orcarouter.ai/v1"
        );
        assert_eq!(
            api_base(Some("https://api.orcarouter.ai/")),
            "https://api.orcarouter.ai/v1"
        );
        assert_eq!(
            api_base(Some("  https://api.orcarouter.ai  ")),
            "https://api.orcarouter.ai/v1"
        );
    }

    #[test]
    fn api_base_accepts_an_endpoint_that_already_names_v1() {
        assert_eq!(
            api_base(Some("https://api.orcarouter.ai/v1")),
            "https://api.orcarouter.ai/v1"
        );
        assert_eq!(
            api_base(Some("https://api.orcarouter.ai/v1/")),
            "https://api.orcarouter.ai/v1"
        );
    }

    #[test]
    fn client_uses_bearer_authentication() {
        let config = HostedModelConfig::from_url(&api_base(None)).with_bearer_token("sk-orca-test");
        let headers = config.headers();
        let auth = headers.get("authorization").and_then(|v| v.to_str().ok());
        assert_eq!(auth, Some("Bearer sk-orca-test"));
        // OrcaRouter rejects `x-api-key`; the header must not be set.
        assert!(headers.get("x-api-key").is_none());
    }
}
