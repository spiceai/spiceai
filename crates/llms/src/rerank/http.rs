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

//! Generic BYO reranker over HTTP.
//!
//! Talks to any user-hosted reranker service that accepts a Cohere-style
//! request/response shape:
//!
//! Request (POST, JSON body):
//! ```json
//! { "query": "<str>", "documents": ["<doc 1>", "<doc 2>", ...] }
//! ```
//!
//! Response:
//! ```json
//! {
//!   "results": [
//!     {"index": 0, "relevance_score": 0.92},
//!     {"index": 1, "relevance_score": 0.11},
//!     ...
//!   ]
//! }
//! ```
//!
//! This is the same schema Cohere, Jina, and most self-hosted reranker
//! services (BGE, Candle, TEI) use, so one client wraps all of them.

use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use super::{Error, ModelCallFailedSnafu, Rerank, Result};
use crate::provider::create_http_client;
use snafu::ResultExt;

/// Shared Cohere-compatible rerank response. Providers that deviate (e.g.
/// Voyage wraps results under `data`) can parse their own struct and still
/// call [`scores_from_results`] for the final ordering step.
#[derive(Debug, Deserialize)]
pub(crate) struct CohereLikeResponse {
    pub results: Vec<CohereLikeResult>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct CohereLikeResult {
    pub index: usize,
    pub relevance_score: f32,
}

/// Map `{index, relevance_score}[]` back into positional per-document scores.
/// Missing indices default to `0.0` (same lenient contract as [`crate::rerank::LlmRerank`]);
/// out-of-range indices are silently dropped. Callers get a vector whose
/// `i`th entry is the score for the `i`th input document.
///
/// Use this for BYO [`HttpReranker`] endpoints, which may be quirky. Native
/// providers (Cohere, Voyage, Jina) should use [`scores_from_results_strict`]
/// so a partial/invalid provider response fails fast.
pub(crate) fn scores_from_results(results: &[CohereLikeResult], expected: usize) -> Vec<f32> {
    let mut out = vec![0.0_f32; expected];
    for entry in results {
        if entry.index < expected {
            out[entry.index] = entry.relevance_score;
        }
    }
    out
}

/// Strict version of [`scores_from_results`]: every index in `0..expected`
/// must be present exactly once. Any missing, duplicated, or out-of-range
/// index surfaces as [`Error::UnparseableResponse`] so silent mis-ranking
/// doesn't slip through.
pub(crate) fn scores_from_results_strict(
    results: &[CohereLikeResult],
    expected: usize,
    model: &str,
) -> Result<Vec<f32>> {
    if results.len() != expected {
        return Err(Error::MismatchedScoreCount {
            model: model.to_string(),
            expected,
            actual: results.len(),
        });
    }
    let mut out = vec![None; expected];
    for entry in results {
        if entry.index >= expected {
            return Err(Error::UnparseableResponse {
                model: model.to_string(),
                response: format!(
                    "reranker returned out-of-range index {} (expected 0..{expected})",
                    entry.index
                ),
            });
        }
        if out[entry.index].is_some() {
            return Err(Error::UnparseableResponse {
                model: model.to_string(),
                response: format!("reranker returned duplicate index {}", entry.index),
            });
        }
        out[entry.index] = Some(entry.relevance_score);
    }
    out.into_iter()
        .enumerate()
        .map(|(i, s)| {
            s.ok_or_else(|| Error::UnparseableResponse {
                model: model.to_string(),
                response: format!("reranker response missing index {i}"),
            })
        })
        .collect()
}

#[derive(Debug, Serialize)]
struct GenericRerankRequest<'a> {
    query: &'a str,
    documents: &'a [String],
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<&'a str>,
}

pub struct HttpReranker {
    client: Client,
    endpoint: String,
    name: String,
    /// Optional `model` field pushed into the request body. Most BYO
    /// endpoints pin the model server-side and ignore this; Cohere-style
    /// multi-tenant services require it.
    model_id: Option<String>,
    /// Optional API key. If set, sent as `Authorization: Bearer <key>`.
    api_key: Option<String>,
}

impl Debug for HttpReranker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpReranker")
            .field("name", &self.name)
            .field("endpoint", &self.endpoint)
            .field("has_api_key", &self.api_key.is_some())
            .field("model_id", &self.model_id)
            .finish_non_exhaustive()
    }
}

impl HttpReranker {
    pub fn try_new(name: impl Into<String>, endpoint: impl Into<String>) -> Result<Self> {
        let name = name.into();
        let client = create_http_client().ok_or(Error::HttpClientCreationFailed {
            model: name.clone(),
        })?;
        Ok(Self {
            client,
            endpoint: endpoint.into(),
            name,
            model_id: None,
            api_key: None,
        })
    }

    #[must_use]
    pub fn with_api_key(mut self, api_key: Option<String>) -> Self {
        self.api_key = api_key;
        self
    }

    #[must_use]
    pub fn with_model_id(mut self, model_id: Option<String>) -> Self {
        self.model_id = model_id;
        self
    }
}

#[async_trait]
impl Rerank for HttpReranker {
    async fn rerank(&self, query: &str, documents: &[String]) -> Result<Vec<f32>> {
        if documents.is_empty() {
            return Ok(Vec::new());
        }

        let body = GenericRerankRequest {
            query,
            documents,
            model: self.model_id.as_deref(),
        };

        let mut req = self.client.post(&self.endpoint).json(&body);
        if let Some(key) = &self.api_key {
            req = req.bearer_auth(key);
        }

        let resp: CohereLikeResponse = req
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            .context(ModelCallFailedSnafu {
                model: self.name.clone(),
            })?
            .json()
            .await
            .map_err(|e| Error::UnparseableResponse {
                model: self.name.clone(),
                response: format!("failed to decode response as Cohere-compatible JSON: {e}"),
            })?;

        Ok(scores_from_results(&resp.results, documents.len()))
    }

    fn model_name(&self) -> Option<&str> {
        Some(&self.name)
    }

    fn is_remote(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scores_from_results_maps_by_index() {
        let results = vec![
            CohereLikeResult {
                index: 2,
                relevance_score: 0.9,
            },
            CohereLikeResult {
                index: 0,
                relevance_score: 0.5,
            },
        ];
        let scores = scores_from_results(&results, 3);
        assert!((scores[0] - 0.5).abs() < 1e-6);
        assert!((scores[1] - 0.0).abs() < 1e-6);
        assert!((scores[2] - 0.9).abs() < 1e-6);
    }

    #[test]
    fn scores_from_results_drops_out_of_range() {
        let results = vec![
            CohereLikeResult {
                index: 5,
                relevance_score: 0.9,
            },
            CohereLikeResult {
                index: 0,
                relevance_score: 0.5,
            },
        ];
        let scores = scores_from_results(&results, 2);
        assert!((scores[0] - 0.5).abs() < 1e-6);
        assert!((scores[1] - 0.0).abs() < 1e-6);
    }

    #[test]
    fn scores_from_results_strict_happy_path() {
        let results = vec![
            CohereLikeResult {
                index: 1,
                relevance_score: 0.3,
            },
            CohereLikeResult {
                index: 0,
                relevance_score: 0.9,
            },
        ];
        let scores = scores_from_results_strict(&results, 2, "m").expect("should map all indices");
        assert!((scores[0] - 0.9).abs() < 1e-6);
        assert!((scores[1] - 0.3).abs() < 1e-6);
    }

    #[test]
    fn scores_from_results_strict_rejects_missing_index() {
        let results = vec![CohereLikeResult {
            index: 0,
            relevance_score: 0.9,
        }];
        let err = scores_from_results_strict(&results, 2, "m")
            .expect_err("expected=2 but only 1 result, so must error");
        assert!(matches!(err, Error::MismatchedScoreCount { .. }));
    }

    #[test]
    fn scores_from_results_strict_rejects_duplicate_index() {
        let results = vec![
            CohereLikeResult {
                index: 0,
                relevance_score: 0.3,
            },
            CohereLikeResult {
                index: 0,
                relevance_score: 0.9,
            },
        ];
        let err = scores_from_results_strict(&results, 2, "m")
            .expect_err("duplicate index 0 must surface as an error");
        assert!(matches!(err, Error::UnparseableResponse { .. }));
    }

    #[test]
    fn scores_from_results_strict_rejects_out_of_range() {
        let results = vec![
            CohereLikeResult {
                index: 0,
                relevance_score: 0.5,
            },
            CohereLikeResult {
                index: 7,
                relevance_score: 0.9,
            },
        ];
        let err = scores_from_results_strict(&results, 2, "m")
            .expect_err("out-of-range index must surface as an error");
        assert!(matches!(err, Error::UnparseableResponse { .. }));
    }
}
