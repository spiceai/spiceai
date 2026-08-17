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

//! The reranking contract.
//!
//! A reranker scores candidate documents against a query and returns one score per
//! document. Implemented by provider crates; called by the runtime's `rerank()` UDTF,
//! which never names a provider.
//!
//! Deliberately tiny — `async-trait` and `snafu`, nothing else. A reranker vendor is
//! usually a small HTTP client, and this crate is what keeps its build that way.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use snafu::Snafu;

/// Name → reranker map. Holds native rerankers (e.g. Cohere, Voyage, BGE) once
/// provider support lands. Users can also use any chat model as a reranker
/// today via the `LlmRerank` adapter, so this store may be empty even in a
/// fully-functional deployment.
pub type RerankerModelStore = std::collections::HashMap<String, Arc<dyn Rerank>>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Reranker model '{model}' failed: {source}"))]
    ModelCallFailed {
        model: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Reranker model '{model}' returned an unparseable response. Expected a JSON array of {{id, score}} (listwise) or object {{score}} (pointwise), but the model returned: {response}"
    ))]
    UnparseableResponse { model: String, response: String },

    #[snafu(display(
        "Reranker model '{model}' returned no scores for the provided documents (expected {expected}, got {actual})."
    ))]
    MismatchedScoreCount {
        model: String,
        expected: usize,
        actual: usize,
    },

    #[snafu(display("Reranker health check failed: {source}"))]
    HealthCheckFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Failed to build HTTP client for reranker '{model}' — standard timeout/TLS defaults are unavailable."
    ))]
    HttpClientCreationFailed { model: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A reranker scores documents against a query. Higher score == more relevant.
///
/// Implementations must return exactly `documents.len()` scores, in the same
/// order as the input: `rerank(query, &docs)[i]` is the score for `docs[i]`.
/// Any mismatch surfaces as [`Error::MismatchedScoreCount`].
///
/// Note: the built-in [`LlmRerank`] adapter is deliberately lenient with
/// partial LLM output — missing ids in a listwise response default to `0.0`
/// (least relevant) rather than erroring — because models occasionally skip
/// entries. Native rerankers are expected to score every document.
#[async_trait]
pub trait Rerank: Send + Sync + Debug {
    async fn rerank(&self, query: &str, documents: &[String]) -> Result<Vec<f32>>;

    /// Name of this reranker model (for tracing / error messages).
    fn model_name(&self) -> Option<&str> {
        None
    }

    /// Whether this reranker runs remotely. UDTF callers can use this to
    /// decide parallelism / rate-limit policy.
    fn is_remote(&self) -> bool {
        true
    }

    async fn health(&self) -> Result<()> {
        let _ = self.rerank("health check", &["ok".to_string()]).await?;
        Ok(())
    }
}
