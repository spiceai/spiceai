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

//! Spicepod definition for reranker models. Declared as a sibling of
//! `models:` / `embeddings:` rather than overloading one of them, because
//! reranker APIs are their own shape (take `(query, documents)` → scores, not
//! chat completion or embedding generation) and most providers expose
//! rerankers through a dedicated endpoint.

use std::{collections::HashMap, fmt::Display};

use crate::metric::Metrics;

use super::{Nameable, WithDependsOn};
#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Reranker {
    pub from: String,
    pub name: String,

    pub description: Option<String>,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub params: HashMap<String, Value>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "dependsOn", default)]
    pub depends_on: Vec<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Metrics>,
}

impl Nameable for Reranker {
    fn name(&self) -> &str {
        &self.name
    }
}

impl WithDependsOn<Reranker> for Reranker {
    fn depends_on(&self, depends_on: &[String]) -> Reranker {
        Reranker {
            depends_on: depends_on.to_vec(),
            ..self.clone()
        }
    }
}

impl Reranker {
    #[must_use]
    pub fn new(from: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            from: from.into(),
            name: name.into(),
            description: None,
            params: HashMap::default(),
            depends_on: Vec::default(),
            metrics: None,
        }
    }

    #[must_use]
    pub fn get_prefix(&self) -> Option<RerankerPrefix> {
        RerankerPrefix::try_from(self.from.as_str()).ok()
    }

    /// Strip the provider prefix from `from` to get the model id. Returns
    /// `None` when the prefix is unrecognized; returns the full `from` for
    /// HTTP rerankers (the URL *is* the model id).
    #[must_use]
    pub fn get_model_id(&self) -> Option<String> {
        let prefix = self.get_prefix()?;
        match prefix {
            RerankerPrefix::Http => Some(self.from.clone()),
            RerankerPrefix::Cohere => self
                .from
                .strip_prefix("cohere:")
                .map(ToString::to_string)
                .or_else(|| self.from.strip_prefix("cohere/").map(ToString::to_string)),
            RerankerPrefix::Voyage => self
                .from
                .strip_prefix("voyage:")
                .map(ToString::to_string)
                .or_else(|| self.from.strip_prefix("voyage/").map(ToString::to_string)),
            RerankerPrefix::Jina => self
                .from
                .strip_prefix("jina:")
                .map(ToString::to_string)
                .or_else(|| self.from.strip_prefix("jina/").map(ToString::to_string)),
        }
    }
}

/// Recognized reranker provider prefixes.
///
/// - `cohere:<model-id>` — Cohere Rerank (`rerank-v3.5`, `rerank-english-v3.0`, …)
/// - `voyage:<model-id>` — Voyage Rerank (`rerank-2`, `rerank-lite-1`, …)
/// - `jina:<model-id>` — Jina Reranker (`jina-reranker-v2-base-multilingual`, …)
/// - `http://…` / `https://…` — BYO reranker service with a Cohere-compatible
///   response shape (`{ results: [{ index, relevance_score }, ...] }`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RerankerPrefix {
    Cohere,
    Voyage,
    Jina,
    Http,
}

impl TryFrom<&str> for RerankerPrefix {
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        // Require `:` or `/` after the provider name so strings like
        // `cohereXYZ:...` or a bare `cohere` aren't silently routed to the
        // Cohere provider (which would then fail inside `get_model_id()`
        // with a confusing `UnknownSource`-style error).
        let has_provider_delim = |prefix: &str| {
            value
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.starts_with(':') || rest.starts_with('/'))
        };

        if has_provider_delim("cohere") {
            Ok(RerankerPrefix::Cohere)
        } else if has_provider_delim("voyage") {
            Ok(RerankerPrefix::Voyage)
        } else if has_provider_delim("jina") {
            Ok(RerankerPrefix::Jina)
        } else if value.starts_with("http://") || value.starts_with("https://") {
            Ok(RerankerPrefix::Http)
        } else {
            Err("Unknown reranker prefix")
        }
    }
}

impl Display for RerankerPrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RerankerPrefix::Cohere => write!(f, "cohere"),
            RerankerPrefix::Voyage => write!(f, "voyage"),
            RerankerPrefix::Jina => write!(f, "jina"),
            RerankerPrefix::Http => write!(f, "http"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_recognizes_providers() {
        assert_eq!(
            RerankerPrefix::try_from("cohere:rerank-v3.5").expect("cohere: prefix"),
            RerankerPrefix::Cohere
        );
        assert_eq!(
            RerankerPrefix::try_from("voyage:rerank-2").expect("voyage: prefix"),
            RerankerPrefix::Voyage
        );
        assert_eq!(
            RerankerPrefix::try_from("jina:jina-reranker-v2").expect("jina: prefix"),
            RerankerPrefix::Jina
        );
        assert_eq!(
            RerankerPrefix::try_from("http://example.com/rerank").expect("http prefix"),
            RerankerPrefix::Http
        );
        assert_eq!(
            RerankerPrefix::try_from("https://example.com/rerank").expect("https prefix"),
            RerankerPrefix::Http
        );
        RerankerPrefix::try_from("openai:gpt-4").expect_err("unknown provider must error");
    }

    #[test]
    fn prefix_requires_delimiter_after_provider_name() {
        // `cohereXYZ:...` must NOT match Cohere — otherwise `get_model_id()`
        // would strip nothing and we'd dispatch to the wrong client.
        RerankerPrefix::try_from("cohereXYZ:rerank-v3.5").expect_err("stray suffix on cohere");
        RerankerPrefix::try_from("voyager:rerank-2").expect_err("stray suffix on voyage");
        RerankerPrefix::try_from("jinafoo").expect_err("stray suffix on jina");

        // Bare provider name (no delimiter, no model id) is also rejected.
        RerankerPrefix::try_from("cohere").expect_err("bare cohere");
        RerankerPrefix::try_from("voyage").expect_err("bare voyage");
        RerankerPrefix::try_from("jina").expect_err("bare jina");
    }

    #[test]
    fn get_model_id_strips_prefix() {
        let c = Reranker::new("cohere:rerank-v3.5", "c");
        assert_eq!(c.get_model_id().as_deref(), Some("rerank-v3.5"));

        let v = Reranker::new("voyage:rerank-2", "v");
        assert_eq!(v.get_model_id().as_deref(), Some("rerank-2"));

        let j = Reranker::new("jina:jina-reranker-v2", "j");
        assert_eq!(j.get_model_id().as_deref(), Some("jina-reranker-v2"));
    }

    #[test]
    fn get_model_id_returns_full_url_for_http() {
        let http = Reranker::new("https://rerank.internal/v1/rerank", "byo");
        assert_eq!(
            http.get_model_id().as_deref(),
            Some("https://rerank.internal/v1/rerank")
        );
    }

    #[test]
    fn get_model_id_accepts_slash_separator() {
        // `cohere/rerank-v3.5` is the style used by some provider libraries.
        let c = Reranker::new("cohere/rerank-v3.5", "c");
        assert_eq!(c.get_model_id().as_deref(), Some("rerank-v3.5"));
    }
}
