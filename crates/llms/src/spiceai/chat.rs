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

//! Chat completions against a Spice endpoint.

use crate::config::HostedModelConfig;
use crate::openai::{Openai, new_openai_client_with_config};

/// The Spice.ai Cloud Platform AI gateway.
pub const DEFAULT_ENDPOINT: &str = "https://data.spiceai.io";

/// Resolves a Spice endpoint to the base URL of its `OpenAI`-compatible API.
///
/// `endpoint` is the root of a Spice.ai Cloud Platform or Spice runtime deployment — e.g.
/// `https://data.spiceai.io`, or `http://localhost:8090` for a Spice-to-Spice connection — which
/// serves the `OpenAI`-compatible API under `/v1`. An endpoint that already names `/v1` is taken
/// as-is, so either spelling resolves to the same base URL.
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

/// Whether `endpoint` resolves to the Spice.ai Cloud Platform rather than a self-hosted Spice
/// runtime. The Cloud Platform always authenticates, so a caller can use this to require an API
/// key. Compares resolved base URLs so an explicit `https://data.spiceai.io` is recognized the
/// same as an unset endpoint.
#[must_use]
pub fn is_cloud_platform(endpoint: Option<&str>) -> bool {
    api_base(endpoint) == api_base(None)
}

/// Creates a chat client for a model served by the Spice.ai Cloud Platform, or by another Spice
/// runtime.
///
/// `api_key` is optional: the Spice.ai Cloud Platform requires one, but a Spice runtime reached
/// over a trusted network may not have authentication enabled.
#[must_use]
pub fn new_spiceai_client(
    model: String,
    endpoint: Option<&str>,
    api_key: Option<&str>,
) -> Openai<HostedModelConfig> {
    let config = HostedModelConfig::from_url(&api_base(endpoint)).with_api_key(api_key);
    new_openai_client_with_config(model, config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_base_defaults_to_the_cloud_platform() {
        assert_eq!(api_base(None), "https://data.spiceai.io/v1");
        assert_eq!(api_base(Some("")), "https://data.spiceai.io/v1");
        assert_eq!(api_base(Some("   ")), "https://data.spiceai.io/v1");
    }

    #[test]
    fn api_base_appends_the_openai_compatible_path() {
        assert_eq!(
            api_base(Some("http://localhost:8090")),
            "http://localhost:8090/v1"
        );
        assert_eq!(
            api_base(Some("http://localhost:8090/")),
            "http://localhost:8090/v1"
        );
        assert_eq!(
            api_base(Some("  http://localhost:8090  ")),
            "http://localhost:8090/v1"
        );
    }

    #[test]
    fn api_base_accepts_an_endpoint_that_already_names_v1() {
        assert_eq!(
            api_base(Some("http://localhost:8090/v1")),
            "http://localhost:8090/v1"
        );
        assert_eq!(
            api_base(Some("http://localhost:8090/v1/")),
            "http://localhost:8090/v1"
        );
    }

    #[test]
    fn cloud_platform_is_detected_however_it_is_spelled() {
        // An unset endpoint and the spelled-out default must agree: the runtime substitutes the
        // latter for the former when a model omits `spiceai_endpoint`.
        assert!(is_cloud_platform(None));
        assert!(is_cloud_platform(Some(DEFAULT_ENDPOINT)));
        assert!(is_cloud_platform(Some("https://data.spiceai.io/")));
        assert!(is_cloud_platform(Some("https://data.spiceai.io/v1")));
    }

    #[test]
    fn self_hosted_runtimes_are_not_the_cloud_platform() {
        assert!(!is_cloud_platform(Some("http://localhost:8090")));
        assert!(!is_cloud_platform(Some(
            "https://spice.internal.example.com"
        )));
    }
}
