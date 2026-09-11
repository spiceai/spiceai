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

//! Typed parameter structs for each embedding provider, deserialized from
//! spicepod `params` via `#[derive(TypedParams)]`.

pub mod azure;
#[cfg(feature = "bedrock")]
pub mod bedrock;
pub mod databricks;
pub mod file;
pub mod google;
pub mod huggingface;
pub mod model2vec;
pub mod openai;

use std::str::FromStr;

use tokenizers::TruncationDirection;

/// Pooling strategy for local (TEI-based) embedding models.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pooling {
    Cls,
    Mean,
    Splade,
}

impl Pooling {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Pooling::Cls => "cls",
            Pooling::Mean => "mean",
            Pooling::Splade => "splade",
        }
    }
}

impl FromStr for Pooling {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "cls" => Ok(Pooling::Cls),
            "mean" => Ok(Pooling::Mean),
            "splade" => Ok(Pooling::Splade),
            other => Err(format!("must be one of: cls, mean, splade. Found {other}")),
        }
    }
}

/// How local (TEI-based) embedding models handle an input longer than the
/// model's maximum sequence length. Mirrors AWS Bedrock's `truncate` /
/// `truncation_mode` convention (see `CohereEmbeddingTruncate`,
/// `NovaTruncationMode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Truncation {
    /// Reject the whole embedding request with an input-validation error,
    /// rather than silently dropping part of an over-long input. Default.
    #[default]
    None,
    /// Discard the end of the input, keeping the start.
    End,
    /// Discard the start of the input, keeping the end.
    Start,
}

impl Truncation {
    /// `None` when an over-long input should be rejected; `Some(direction)`
    /// when it should instead be truncated in that direction. This is the
    /// value the TEI backend (`TeiEmbed::from_hf`/`from_local`) takes directly.
    #[must_use]
    pub fn direction(self) -> Option<TruncationDirection> {
        match self {
            Truncation::None => None,
            Truncation::End => Some(TruncationDirection::Right),
            Truncation::Start => Some(TruncationDirection::Left),
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Truncation::None => "NONE",
            Truncation::End => "END",
            Truncation::Start => "START",
        }
    }
}

impl FromStr for Truncation {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "NONE" => Ok(Truncation::None),
            "END" => Ok(Truncation::End),
            "START" => Ok(Truncation::Start),
            other => Err(format!("must be one of: NONE, END, START. Found {other}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[tokio::test]
    async fn openai_params_default_endpoint_and_tier() {
        let typed = openai::OpenAiEmbeddingParams::try_from_params(
            "embedding test",
            params(&[("openai_api_key", "sk-1")]),
            &empty_secrets(),
        )
        .await
        .expect("openai params should deserialize");
        assert_eq!(typed.endpoint, "https://api.openai.com/v1");
        assert_eq!(
            typed.api_key.as_ref().map(ExposeSecret::expose_secret),
            Some("sk-1")
        );
        assert_eq!(typed.usage_tier, llms::openai::UsageTier::Tier1);
    }

    #[tokio::test]
    async fn azure_params_accept_prefixed_keys() {
        let typed = azure::AzureEmbeddingParams::try_from_params(
            "embedding test",
            params(&[
                ("endpoint", "https://r.openai.azure.com"),
                ("azure_api_version", "2024-02-01"),
                ("azure_deployment_name", "embed"),
                ("azure_api_key", "key"),
            ]),
            &empty_secrets(),
        )
        .await
        .expect("azure params should deserialize");
        assert_eq!(
            typed.endpoint.as_deref(),
            Some("https://r.openai.azure.com")
        );
        assert_eq!(typed.api_version.as_deref(), Some("2024-02-01"));
        assert_eq!(typed.deployment_name.as_deref(), Some("embed"));
        assert!(typed.api_key.is_some());
        assert!(typed.entra_token.is_none());
    }

    #[tokio::test]
    async fn google_params_allow_missing_project_at_parse_time() {
        // `project`/`location`/auth-method fields are all optional at the parse level; the
        // "required" checks happen in `llms::google::auth::build_client`, not here — see its
        // `vertex_requires_project_and_location` / `vertex_requires_exactly_one_auth_method`
        // tests.
        let typed = google::GoogleEmbeddingParams::try_from_params(
            "embedding test",
            params(&[]),
            &empty_secrets(),
        )
        .await
        .expect("google params should parse with nothing set");
        assert!(typed.project.is_none());
    }

    #[tokio::test]
    async fn google_params_accept_runtime_dimensions() {
        let typed = google::GoogleEmbeddingParams::try_from_params(
            "embedding test",
            params(&[("dimensions", "768")]),
            &empty_secrets(),
        )
        .await
        .expect("google params should deserialize");
        assert_eq!(typed.dimensions, Some(768));
    }

    #[tokio::test]
    async fn huggingface_params_parse_pooling_enum() {
        let typed = huggingface::HuggingFaceEmbeddingParams::try_from_params(
            "embedding test",
            params(&[
                ("hf_token", "hf_abc"),
                ("pooling", "mean"),
                ("max_seq_length", "512"),
                ("truncate", "END"),
            ]),
            &empty_secrets(),
        )
        .await
        .expect("huggingface params should deserialize");
        assert_eq!(
            typed.hf_token.as_ref().map(ExposeSecret::expose_secret),
            Some("hf_abc")
        );
        assert_eq!(typed.pooling, Some(Pooling::Mean));
        assert_eq!(typed.max_seq_length, Some(512));
        assert_eq!(typed.truncate, Some(Truncation::End));
    }

    #[tokio::test]
    async fn huggingface_params_default_truncate_is_absent() {
        // An omitted `truncate` stays `None`, which the call site resolves to
        // the default `NONE` (no silent truncation, over-long inputs error).
        let typed = huggingface::HuggingFaceEmbeddingParams::try_from_params(
            "embedding test",
            params(&[("hf_token", "hf_abc")]),
            &empty_secrets(),
        )
        .await
        .expect("huggingface params should deserialize");
        assert_eq!(typed.truncate, None);
        assert_eq!(typed.truncate.unwrap_or_default().direction(), None);
    }

    #[tokio::test]
    async fn file_params_parse_truncate_enum() {
        let typed = file::FileEmbeddingParams::try_from_params(
            "embedding test",
            params(&[("truncate", "START")]),
            &empty_secrets(),
        )
        .await
        .expect("file params should deserialize");
        assert_eq!(typed.truncate, Some(Truncation::Start));
        assert_eq!(
            typed.truncate.unwrap_or_default().direction(),
            Some(TruncationDirection::Left)
        );
    }

    #[tokio::test]
    async fn file_params_reject_unknown_truncate() {
        let Err(err) = file::FileEmbeddingParams::try_from_params(
            "embedding test",
            params(&[("truncate", "sometimes")]),
            &empty_secrets(),
        )
        .await
        else {
            panic!("an unknown truncate value should error")
        };
        assert!(
            err.to_string().contains("must be one of: NONE, END, START"),
            "unexpected message: {err}"
        );
    }

    #[test]
    fn truncation_from_str_round_trips_and_maps_to_direction() {
        assert_eq!(Truncation::default(), Truncation::None);
        assert_eq!(Truncation::None.direction(), None);
        assert_eq!(
            Truncation::End.direction(),
            Some(TruncationDirection::Right)
        );
        assert_eq!(
            Truncation::Start.direction(),
            Some(TruncationDirection::Left)
        );

        for variant in [Truncation::None, Truncation::End, Truncation::Start] {
            assert_eq!(
                variant.as_str().parse::<Truncation>(),
                Ok(variant),
                "{variant:?} should round-trip through its string form"
            );
        }

        "nonsense"
            .parse::<Truncation>()
            .expect_err("nonsense is not truncation");
    }

    #[tokio::test]
    async fn databricks_params_require_endpoint() {
        let Err(err) = databricks::DatabricksEmbeddingParams::try_from_params(
            "embedding test",
            params(&[("databricks_token", "t")]),
            &empty_secrets(),
        )
        .await
        else {
            panic!("databricks endpoint is required")
        };
        assert!(
            err.to_string()
                .contains("Missing required parameter: databricks_endpoint"),
            "unexpected message: {err}"
        );
    }

    #[tokio::test]
    async fn model2vec_params_reject_malformed_numbers() {
        let Err(err) = model2vec::Model2VecEmbeddingParams::try_from_params(
            "embedding test",
            params(&[("parallelism", "many")]),
            &empty_secrets(),
        )
        .await
        else {
            panic!("malformed parallelism should error")
        };
        assert!(
            err.to_string()
                .contains("Invalid value for parameter 'parallelism'"),
            "unexpected message: {err}"
        );
    }

    #[tokio::test]
    async fn model2vec_params_accept_runtime_keys() {
        let typed = model2vec::Model2VecEmbeddingParams::try_from_params(
            "embedding test",
            params(&[
                ("hf_token", "hf_abc"),
                ("subfolder", "onnx"),
                ("normalize", "true"),
                ("parallelism", "4"),
                ("embed_max_token_length", "512"),
                ("embed_custom_batch_size", "32"),
            ]),
            &empty_secrets(),
        )
        .await
        .expect("model2vec params should deserialize");
        assert_eq!(
            typed.hf_token.as_ref().map(ExposeSecret::expose_secret),
            Some("hf_abc")
        );
        assert_eq!(typed.subfolder.as_deref(), Some("onnx"));
        assert_eq!(typed.normalize, Some(true));
        assert_eq!(typed.parallelism, Some(4));
        assert_eq!(typed.embed_max_token_length, Some(512));
        assert_eq!(typed.embed_custom_batch_size, Some(32));
    }

    #[cfg(feature = "bedrock")]
    #[tokio::test]
    async fn bedrock_params_accept_runtime_keys_and_legacy_truncate_alias() {
        let typed = bedrock::BedrockEmbeddingParams::try_from_params(
            "embedding test",
            params(&[
                ("aws_access_key_id", "AKIA"),
                ("aws_secret_access_key", "secret"),
                ("aws_session_token", "token"),
                ("aws_region", "us-east-1"),
                ("aws_iam_role_source", "auto"),
                ("aws_profile", "default"),
                ("requests_per_min_limit", "1500"),
                ("max_concurrent_invocations", "10"),
                ("dimensions", "1024"),
                ("normalize", "true"),
                ("truncate", "END"),
                ("input_type", "search_document"),
            ]),
            &empty_secrets(),
        )
        .await
        .expect("bedrock params should deserialize");
        assert_eq!(
            typed
                .aws_access_key_id
                .as_ref()
                .map(ExposeSecret::expose_secret),
            Some("AKIA")
        );
        assert_eq!(
            typed
                .aws_secret_access_key
                .as_ref()
                .map(ExposeSecret::expose_secret),
            Some("secret")
        );
        assert_eq!(
            typed
                .aws_session_token
                .as_ref()
                .map(ExposeSecret::expose_secret),
            Some("token")
        );
        assert_eq!(typed.aws_region.as_deref(), Some("us-east-1"));
        assert_eq!(typed.aws_iam_role_source.as_deref(), Some("auto"));
        assert_eq!(typed.aws_profile.as_deref(), Some("default"));
        assert_eq!(typed.requests_per_min_limit, 1500);
        assert_eq!(typed.max_concurrent_invocations, 10);
        assert_eq!(typed.dimensions, Some(1024));
        assert_eq!(typed.normalize, Some(true));
        assert_eq!(typed.truncate_mode, Some(Truncation::End));
        assert!(typed.input_type.is_some());
    }
}
