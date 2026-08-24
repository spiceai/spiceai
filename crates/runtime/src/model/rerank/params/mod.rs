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

//! Typed parameter structs for each reranker provider, deserialized from
//! spicepod `params` via `#[derive(TypedParams)]`. Mirrors
//! `crate::embeddings::params`.

pub mod cohere;
pub mod file;
pub mod http;
pub mod huggingface;
pub mod jina;
pub mod voyage;

use std::str::FromStr;

/// How local (TEI-based) reranker models handle a `(query, document)` pair
/// longer than the model's maximum sequence length. Values are matched
/// case-insensitively, e.g. `none`, `END`, `Start`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Truncation {
    /// Reject the whole rerank request with an input-validation error, rather
    /// than silently dropping part of an over-long `(query, document)` pair.
    /// Default.
    #[default]
    None,
    /// Discard the end of the pair, keeping the start.
    End,
    /// Discard the start of the pair, keeping the end.
    Start,
}

impl Truncation {
    /// `None` when an over-long pair should be rejected; `Some(direction)`
    /// when it should instead be truncated in that direction. This is the
    /// value `TeiRerank::from_hf`/`from_dir` take directly.
    #[must_use]
    pub fn direction(self) -> Option<tokenizers::TruncationDirection> {
        match self {
            Truncation::None => None,
            Truncation::End => Some(tokenizers::TruncationDirection::Right),
            Truncation::Start => Some(tokenizers::TruncationDirection::Left),
        }
    }
}

impl FromStr for Truncation {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "none" => Ok(Truncation::None),
            "end" => Ok(Truncation::End),
            "start" => Ok(Truncation::Start),
            other => Err(format!("must be one of: none, end, start. Found '{other}'")),
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
    async fn cohere_params_accept_bare_and_aliased_api_key() {
        let bare = cohere::CohereRerankerParams::try_from_params(
            "reranker test",
            params(&[("api_key", "co-1")]),
            &empty_secrets(),
        )
        .await
        .expect("bare api_key should deserialize");
        assert_eq!(bare.api_key.expose_secret(), "co-1");

        let aliased = cohere::CohereRerankerParams::try_from_params(
            "reranker test",
            params(&[("cohere_api_key", "co-2")]),
            &empty_secrets(),
        )
        .await
        .expect("cohere_api_key alias should deserialize");
        assert_eq!(aliased.api_key.expose_secret(), "co-2");
    }

    #[tokio::test]
    async fn cohere_params_require_api_key() {
        let Err(err) = cohere::CohereRerankerParams::try_from_params(
            "reranker test",
            params(&[]),
            &empty_secrets(),
        )
        .await
        else {
            panic!("cohere api_key is required")
        };
        assert!(
            err.to_string()
                .contains("Missing required parameter: api_key"),
            "unexpected message: {err}"
        );
    }

    #[tokio::test]
    async fn voyage_params_accept_bare_and_aliased_api_key() {
        let aliased = voyage::VoyageRerankerParams::try_from_params(
            "reranker test",
            params(&[("voyage_api_key", "v-1"), ("endpoint", "https://v.example")]),
            &empty_secrets(),
        )
        .await
        .expect("voyage_api_key alias should deserialize");
        assert_eq!(aliased.api_key.expose_secret(), "v-1");
        assert_eq!(aliased.endpoint.as_deref(), Some("https://v.example"));
    }

    #[tokio::test]
    async fn jina_params_accept_bare_and_aliased_api_key() {
        let aliased = jina::JinaRerankerParams::try_from_params(
            "reranker test",
            params(&[("jina_api_key", "j-1")]),
            &empty_secrets(),
        )
        .await
        .expect("jina_api_key alias should deserialize");
        assert_eq!(aliased.api_key.expose_secret(), "j-1");
    }

    #[tokio::test]
    async fn http_params_are_all_optional() {
        let typed = http::HttpRerankerParams::try_from_params(
            "reranker test",
            params(&[]),
            &empty_secrets(),
        )
        .await
        .expect("http params should deserialize with nothing set");
        assert!(typed.api_key.is_none());
        assert!(typed.model.is_none());
    }

    #[tokio::test]
    async fn huggingface_params_parse_truncate_case_insensitively() {
        let typed = huggingface::HuggingFaceRerankerParams::try_from_params(
            "reranker test",
            params(&[
                ("hf_token", "hf_abc"),
                ("max_seq_length", "512"),
                ("truncate", "End"),
            ]),
            &empty_secrets(),
        )
        .await
        .expect("huggingface reranker params should deserialize");
        assert_eq!(
            typed.hf_token.as_ref().map(ExposeSecret::expose_secret),
            Some("hf_abc")
        );
        assert_eq!(typed.max_seq_length, Some(512));
        assert_eq!(typed.truncate, Some(Truncation::End));
    }

    #[tokio::test]
    async fn huggingface_params_accept_api_key_alias() {
        // `api_key` mirrors the key name used by the other reranker providers,
        // so a `huggingface:` reranker doesn't require a `hf_token`-specific key.
        let typed = huggingface::HuggingFaceRerankerParams::try_from_params(
            "reranker test",
            params(&[("api_key", "hf_abc")]),
            &empty_secrets(),
        )
        .await
        .expect("huggingface reranker params should deserialize");
        assert_eq!(
            typed.hf_token.as_ref().map(ExposeSecret::expose_secret),
            Some("hf_abc")
        );
    }

    #[tokio::test]
    async fn huggingface_params_default_truncate_is_absent() {
        let typed = huggingface::HuggingFaceRerankerParams::try_from_params(
            "reranker test",
            params(&[]),
            &empty_secrets(),
        )
        .await
        .expect("huggingface reranker params should deserialize");
        assert_eq!(typed.truncate, None);
        assert_eq!(typed.truncate.unwrap_or_default().direction(), None);
    }

    #[tokio::test]
    async fn file_params_reject_unknown_truncate() {
        let Err(err) = file::FileRerankerParams::try_from_params(
            "reranker test",
            params(&[("truncate", "sometimes")]),
            &empty_secrets(),
        )
        .await
        else {
            panic!("an unknown truncate value should error")
        };
        assert!(
            err.to_string().contains("must be one of: none, end, start"),
            "unexpected message: {err}"
        );
    }

    #[test]
    fn truncation_from_str_is_case_insensitive_and_maps_to_direction() {
        assert_eq!(Truncation::default(), Truncation::None);
        assert_eq!(Truncation::None.direction(), None);
        assert_eq!("END".parse::<Truncation>(), Ok(Truncation::End));
        assert_eq!("start".parse::<Truncation>(), Ok(Truncation::Start));
        "nonsense"
            .parse::<Truncation>()
            .expect_err("nonsense is not truncation");
    }
}
