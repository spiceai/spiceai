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

/// Parameters for `from: spiceai` chat models.
#[derive(TypedParams)]
#[params(
    prefix = "spiceai",
    passthrough = crate::model::params::common::PREFIXED_COMMON,
    emit_specs
)]
pub struct SpiceAiModelParams {
    /// The API key for the `Spice.ai` Cloud Platform, or for the Spice runtime serving the model. Required for the `Spice.ai` Cloud Platform.
    #[param(autoload_secret)]
    pub api_key: Option<SecretString>,
    /// The endpoint serving the model: the `Spice.ai` Cloud Platform, or another Spice runtime.
    // The default mirrors `llms::spiceai::DEFAULT_ENDPOINT`; `endpoint_default_matches_llms`
    // guards against drift between the two.
    #[param(default = "https://data.spiceai.io")]
    pub endpoint: String,
}

#[cfg(test)]
mod tests {
    use runtime_parameters_typed::TypedParams;
    use secrecy::SecretString;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    use super::SpiceAiModelParams;

    fn empty_secrets() -> Arc<RwLock<runtime_secrets::Secrets>> {
        Arc::new(RwLock::new(runtime_secrets::Secrets::new()))
    }

    #[tokio::test]
    async fn endpoint_default_matches_llms() {
        let typed = SpiceAiModelParams::try_from_params(
            "model spiceai",
            HashMap::<String, SecretString>::new(),
            &empty_secrets(),
        )
        .await
        .expect("spiceai params should deserialize with defaults");
        assert_eq!(typed.endpoint, llms::spiceai::DEFAULT_ENDPOINT);
    }
}
