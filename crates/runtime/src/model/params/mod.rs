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

//! Typed parameter structs for each model provider, deserialized from spicepod
//! `params` via `#[derive(TypedParams)]`.
//!
//! Each provider declares only its provider-specific fields; the parameters
//! common to every provider (runtime tunables + OpenAI-compatible chat
//! overrides, including the legacy `openai_`-prefixed deprecated forms) are
//! shared through the [`common`] passthrough tables via
//! `#[params(passthrough = ...)]`. `#[params(emit_specs)]` regenerates the
//! `ParameterSpec` list the schema generator consumes, keeping the struct the
//! single source of truth for both runtime deserialization and schema.

pub mod anthropic;
pub mod azure;
pub mod bedrock;
pub mod common;
pub mod databricks;
pub mod file;
pub mod google;
pub mod huggingface;
pub mod openai;
pub mod spiceai;
pub mod typesafe;
pub mod xai;

use std::sync::LazyLock;

pub(crate) use crate::parameters::ParameterSpec;
use spicepod::component::model::ModelSource;

macro_rules! source_specs {
    ($name:ident, $ty:ty) => {
        static $name: LazyLock<Vec<ParameterSpec>> = LazyLock::new(<$ty>::parameter_specs);
    };
}

source_specs!(OPENAI_SPEC, openai::OpenAiModelParams);
source_specs!(AZURE_SPEC, azure::AzureModelParams);
source_specs!(FILE_SPEC, file::FileModelParams);
source_specs!(DATABRICKS_SPEC, databricks::DatabricksModelParams);
source_specs!(HUGGINGFACE_SPEC, huggingface::HuggingFaceModelParams);
source_specs!(ANTHROPIC_SPEC, anthropic::AnthropicModelParams);
source_specs!(XAI_SPEC, xai::XaiModelParams);
source_specs!(BEDROCK_SPEC, bedrock::BedrockModelParams);
source_specs!(SPICEAI_SPEC, spiceai::SpiceAiModelParams);
source_specs!(GOOGLE_SPEC, google::GoogleModelParams);
source_specs!(TYPESAFE_SPEC, typesafe::TypeSafeModelParams);

/// Returns the parameter specifications for a given model source, generated
/// from that source's `#[derive(TypedParams)]` struct (the single source of
/// truth for both runtime deserialization and schema). Used by the schema
/// generator to collect all model parameters.
#[must_use]
pub fn get_params_spec(source: &ModelSource) -> &'static [ParameterSpec] {
    match source {
        ModelSource::OpenAi => &OPENAI_SPEC,
        ModelSource::Azure => &AZURE_SPEC,
        ModelSource::File => &FILE_SPEC,
        ModelSource::Databricks => &DATABRICKS_SPEC,
        ModelSource::HuggingFace => &HUGGINGFACE_SPEC,
        ModelSource::Anthropic => &ANTHROPIC_SPEC,
        ModelSource::Xai => &XAI_SPEC,
        ModelSource::Bedrock => &BEDROCK_SPEC,
        ModelSource::SpiceAI => &SPICEAI_SPEC,
        ModelSource::Google => &GOOGLE_SPEC,
        ModelSource::TypeSafe => &TYPESAFE_SPEC,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runtime_parameters::ParameterType;
    use runtime_parameters_typed::TypedParams;
    use secrecy::{ExposeSecret, SecretString};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    fn params(entries: &[(&str, &str)]) -> HashMap<String, SecretString> {
        entries
            .iter()
            .map(|(k, v)| ((*k).to_string(), SecretString::from((*v).to_string())))
            .collect()
    }

    fn empty_secrets() -> Arc<RwLock<runtime_secrets::Secrets>> {
        Arc::new(RwLock::new(runtime_secrets::Secrets::new()))
    }

    /// What the typed-params crate logs while `parse` runs, one line per event.
    async fn typed_params_warnings<F: std::future::Future>(parse: F) -> String {
        use tracing_subscriber::Layer as _;
        use tracing_subscriber::layer::SubscriberExt as _;

        #[derive(Clone, Default)]
        struct CapturedLogs(Arc<parking_lot::Mutex<Vec<u8>>>);

        impl std::io::Write for CapturedLogs {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().extend_from_slice(buf);
                Ok(buf.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
            type Writer = Self;

            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        let logs = CapturedLogs::default();
        let layer = tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .without_time()
            .with_level(false)
            .with_target(false)
            .with_writer(logs.clone())
            .with_filter(
                tracing_subscriber::filter::Targets::new()
                    .with_target("runtime_parameters_typed", tracing::Level::TRACE),
            );
        let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
        parse.await;
        String::from_utf8_lossy(&logs.0.lock()).into_owned()
    }

    #[tokio::test]
    async fn a_bare_override_on_a_prefixed_provider_warns_as_a_deprecated_alias() {
        // The runtime applies `reasoning_effort` for a `hf` model, so the warning must not
        // say it is ignored.
        let warnings = typed_params_warnings(async {
            huggingface::HuggingFaceModelParams::try_from_params(
                "model huggingface",
                params(&[("hf_token", "hf_abc"), ("reasoning_effort", "low")]),
                &empty_secrets(),
            )
            .await
            .expect("huggingface params should deserialize");
        })
        .await;
        assert_eq!(
            warnings.trim_end(),
            format!(
                "Parameter 'reasoning_effort' is deprecated for model huggingface: {}",
                common::UNPREFIXED_DEPRECATED_MESSAGE
            )
        );
    }

    #[tokio::test]
    async fn a_prefixed_override_logs_nothing() {
        let warnings = typed_params_warnings(async {
            huggingface::HuggingFaceModelParams::try_from_params(
                "model huggingface",
                params(&[("hf_token", "hf_abc"), ("hf_reasoning_effort", "low")]),
                &empty_secrets(),
            )
            .await
            .expect("huggingface params should deserialize");
        })
        .await;
        assert_eq!(warnings, "");
    }

    #[tokio::test]
    async fn openai_defaults_and_overrides_are_accepted() {
        // Common override params (temperature, prefixed and legacy openai_ forms)
        // are passthrough: they must deserialize without tripping the unknown-key
        // path, and the provider-specific defaults must apply.
        let typed = openai::OpenAiModelParams::try_from_params(
            "model openai",
            params(&[
                ("openai_api_key", "sk-1"),
                ("temperature", "0.7"),
                ("openai_top_p", "0.9"),
            ]),
            &empty_secrets(),
        )
        .await
        .expect("openai params should deserialize");
        assert_eq!(typed.endpoint, "https://api.openai.com/v1");
        assert_eq!(typed.usage_tier, llms::openai::UsageTier::Tier1);
        assert_eq!(
            typed.responses_api,
            llms::openai::ChatBackend::ChatCompletions
        );
        assert_eq!(
            typed.api_key.as_ref().map(ExposeSecret::expose_secret),
            Some("sk-1")
        );
    }

    #[tokio::test]
    async fn openai_rejects_invalid_usage_tier() {
        let err = openai::OpenAiModelParams::try_from_params(
            "model openai",
            params(&[("openai_usage_tier", "tier9")]),
            &empty_secrets(),
        )
        .await
        .expect_err("an invalid usage_tier should be rejected");
        assert!(
            err.to_string()
                .contains("Invalid value for parameter 'openai_usage_tier'"),
            "unexpected message: {err}"
        );
    }

    /// Regression test for #13932: `hf_token` is the documented key, shared with
    /// the embeddings and reranker components.
    #[tokio::test]
    async fn huggingface_accepts_hf_token_and_runtime_model_type() {
        let typed = huggingface::HuggingFaceModelParams::try_from_params(
            "model huggingface",
            params(&[("hf_token", "hf_abc"), ("model_type", "llama")]),
            &empty_secrets(),
        )
        .await
        .expect("huggingface params should deserialize");
        assert_eq!(
            typed.hf_token.as_ref().map(ExposeSecret::expose_secret),
            Some("hf_abc")
        );
        assert_eq!(typed.model_type.as_deref(), Some("llama"));
        assert_eq!(
            typed.distributed_backend,
            llms::chat::DistributedBackendSetting::None
        );
    }

    #[tokio::test]
    async fn huggingface_accepts_huggingface_token_alias() {
        let typed = huggingface::HuggingFaceModelParams::try_from_params(
            "model huggingface",
            params(&[("huggingface_token", "hf_abc")]),
            &empty_secrets(),
        )
        .await
        .expect("huggingface params should deserialize");
        assert_eq!(
            typed.hf_token.as_ref().map(ExposeSecret::expose_secret),
            Some("hf_abc")
        );
    }

    #[test]
    fn huggingface_spec_advertises_hf_token() {
        // The schema generator renders this spec list, so the key it advertises
        // must be the key `try_from_params` reads.
        let specs = get_params_spec(&ModelSource::HuggingFace);
        let token = specs
            .iter()
            .find(|s| s.name == "hf_token")
            .expect("spec for hf_token");
        assert_eq!(token.r#type, ParameterType::Runtime);
        assert!(
            specs.iter().all(|s| s.name != "token"),
            "only one spec may render to the `hf_token` key"
        );
    }

    /// `construct_model` and the responses API read the prefixed passthrough
    /// overrides (`{prefix}_temperature`, `{prefix}_tools`, …) through
    /// `get_openai_request_overrides(component, source.short_name())`, while
    /// `try_from_params` consumes them under the struct's `prefix`. When the two
    /// disagree, an override spelled the documented way is warned about as unknown
    /// and one spelled the struct's way is consumed but never applied (#13932).
    #[test]
    fn model_param_prefixes_match_the_source_short_name() {
        for (source, prefix) in [
            (ModelSource::OpenAi, openai::OpenAiModelParams::PREFIX),
            (ModelSource::Azure, azure::AzureModelParams::PREFIX),
            (ModelSource::File, file::FileModelParams::PREFIX),
            (
                ModelSource::Databricks,
                databricks::DatabricksModelParams::PREFIX,
            ),
            (
                ModelSource::HuggingFace,
                huggingface::HuggingFaceModelParams::PREFIX,
            ),
            (
                ModelSource::Anthropic,
                anthropic::AnthropicModelParams::PREFIX,
            ),
            (ModelSource::Xai, xai::XaiModelParams::PREFIX),
            (ModelSource::Bedrock, bedrock::BedrockModelParams::PREFIX),
            (ModelSource::SpiceAI, spiceai::SpiceAiModelParams::PREFIX),
            (ModelSource::Google, google::GoogleModelParams::PREFIX),
            (ModelSource::TypeSafe, typesafe::TypeSafeModelParams::PREFIX),
        ] {
            assert_eq!(
                prefix,
                source.short_name(),
                "params struct prefix for {source:?} must match the source short name"
            );
        }
    }

    #[tokio::test]
    async fn bedrock_reads_aws_credentials_into_runtime_params() {
        let typed = bedrock::BedrockModelParams::try_from_params(
            "model bedrock",
            params(&[
                ("aws_access_key_id", "AKIA"),
                ("aws_secret_access_key", "secret"),
                ("aws_region", "us-east-1"),
                ("bedrock_trace", "enabled"),
            ]),
            &empty_secrets(),
        )
        .await
        .expect("bedrock params should deserialize");
        let runtime = typed.runtime_params();
        assert_eq!(
            runtime
                .get("aws_access_key_id")
                .map(ExposeSecret::expose_secret),
            Some("AKIA")
        );
        assert_eq!(
            runtime.get("aws_region").map(ExposeSecret::expose_secret),
            Some("us-east-1")
        );
        assert_eq!(typed.trace, Some(bedrock::GuardrailTraceMode::Enabled));
    }

    #[test]
    fn schema_specs_cover_provider_and_common_params() {
        // The generated schema must include provider-specific keys and the shared
        // common params (with the legacy openai_ deprecated forms).
        let specs = get_params_spec(&ModelSource::OpenAi);
        assert!(specs.iter().any(|s| s.name == "api_key"));
        // OpenAI accepts `temperature` unprefixed (runtime) and the deprecated
        // `openai_temperature` (component form, name "temperature", prefixed).
        assert!(
            specs
                .iter()
                .any(|s| s.name == "temperature" && s.r#type == ParameterType::Runtime)
        );
        assert!(specs.iter().any(|s| s.name == "temperature"
            && s.r#type == ParameterType::Component
            && s.deprecation_message.is_some()));

        let hf = get_params_spec(&ModelSource::HuggingFace);
        assert!(hf.iter().any(|s| s.name == "model_type"));
        // Non-OpenAI providers carry the prefixed component override forms plus the
        // deprecated literal `openai_` forms.
        assert!(hf.iter().any(|s| s.name == "temperature"));
        assert!(
            hf.iter()
                .any(|s| s.name == "openai_temperature" && s.deprecation_message.is_some())
        );
    }
}
