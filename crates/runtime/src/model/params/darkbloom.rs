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

use runtime_parameters::TypedParams;
use secrecy::SecretString;

/// Parameters for `from: darkbloom` chat models.
#[derive(TypedParams)]
#[params(
    prefix = "darkbloom",
    passthrough = crate::model::params::common::PREFIXED_COMMON,
    emit_specs
)]
pub struct DarkbloomModelParams {
    /// The Darkbloom API key, created in the Darkbloom console. When unset, it is read from the `darkbloom_api_key` secret, such as the `DARKBLOOM_API_KEY` environment variable.
    #[param(autoload_secret)]
    pub api_key: Option<SecretString>,
    /// The base URL of the Darkbloom `OpenAI`-compatible API.
    // The default mirrors `llms::darkbloom::DEFAULT_ENDPOINT`; `endpoint_default_matches_llms`
    // guards against drift between the two.
    #[param(default = "https://api.darkbloom.dev/v1")]
    pub endpoint: String,
}

#[cfg(test)]
mod tests {
    use runtime_parameters_typed::TypedParams;
    use secrecy::{ExposeSecret, SecretString};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    use super::DarkbloomModelParams;

    fn empty_secrets() -> Arc<RwLock<runtime_secrets::Secrets>> {
        Arc::new(RwLock::new(runtime_secrets::Secrets::new()))
    }

    #[tokio::test]
    async fn endpoint_default_matches_llms() {
        let typed = DarkbloomModelParams::try_from_params(
            "model darkbloom",
            HashMap::<String, SecretString>::new(),
            &empty_secrets(),
        )
        .await
        .expect("darkbloom params should deserialize with defaults");
        assert_eq!(typed.endpoint, llms::darkbloom::DEFAULT_ENDPOINT);
        assert!(typed.api_key.is_none());
    }

    #[tokio::test]
    async fn prefixed_params_are_read() {
        let params: HashMap<String, SecretString> = [
            ("darkbloom_api_key", "eigeninference-test"),
            ("darkbloom_endpoint", "http://localhost:8080/v1"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), SecretString::from(v)))
        .collect();
        let typed =
            DarkbloomModelParams::try_from_params("model darkbloom", params, &empty_secrets())
                .await
                .expect("darkbloom params should deserialize");
        assert_eq!(
            typed.api_key.as_ref().map(ExposeSecret::expose_secret),
            Some("eigeninference-test")
        );
        assert_eq!(typed.endpoint, "http://localhost:8080/v1");
    }
}
