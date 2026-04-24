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
#![allow(clippy::missing_errors_doc)]

//! The `Rerank` trait and built-in adapters.
//!
//! A reranker scores a set of candidate documents against a query string and
//! returns per-document relevance scores. Two kinds of rerankers are supported:
//!
//! - **Native** (future): dedicated cross-encoder or reranker-API providers
//!   such as Cohere Rerank, Voyage Rerank, Jina Rerank, or local BGE. These
//!   implement [`Rerank`] directly.
//! - **LLM-backed**: any model in the chat-completion store can be used as a
//!   reranker via [`LlmRerank`], which prompts the model listwise or
//!   pointwise and parses JSON scores from the response.
//!
//! The `rerank()` UDTF looks up a model name first in the reranker store and
//! then falls back to wrapping a chat model in [`LlmRerank`] — so users can
//! use any already-registered chat model as a reranker without extra config.

use async_trait::async_trait;
use snafu::Snafu;
use std::fmt::Debug;
use std::sync::Arc;

// Alias so the `llms` crate's internal `chat` module is reachable from this
// module without fighting the `pub mod` ordering in `lib.rs`.
use crate::chat as llms_chat_module;
use llms_chat_module::Chat;

pub mod cohere;
pub mod http;
pub mod jina;
pub mod voyage;

pub use cohere::CohereReranker;
pub use http::HttpReranker;
pub use jina::JinaReranker;
pub use voyage::VoyageReranker;

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

/// Strategy for prompting an LLM to rerank documents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LlmStrategy {
    /// Single call: show the model all candidates at once, ask for a ranked
    /// JSON array of `{id, score}`. Cheapest and recommended default.
    #[default]
    Listwise,
    /// N calls: score each document independently with a 0-1 relevance prompt.
    /// More expensive; use when you need per-document calibrated scores.
    Pointwise,
}

impl LlmStrategy {
    pub fn parse(s: &str) -> std::result::Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "listwise" | "list" => Ok(Self::Listwise),
            "pointwise" | "point" => Ok(Self::Pointwise),
            other => Err(format!(
                "Unknown reranker strategy '{other}'. Use 'listwise' or 'pointwise'."
            )),
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Listwise => "listwise",
            Self::Pointwise => "pointwise",
        }
    }
}

/// Default listwise prompt template. `{query}` and `{documents}` are replaced
/// at call time. The model is asked for strict JSON so we can parse it.
pub const DEFAULT_LISTWISE_PROMPT: &str = "You are a relevance reranker. Given a query and a numbered list of documents, rank the documents by how well they answer the query. For each document, return a JSON object with `id` (the 1-based index) and `score` (a relevance score from 0.0 to 1.0 where 1.0 is maximally relevant). Return ONLY a JSON array of these objects, with no surrounding prose. Include an entry for every document in the input.

Query: {query}

Documents:
{documents}

Response (JSON array only):";

/// Default pointwise prompt template. `{query}` and `{document}` are replaced
/// at call time for each document.
pub const DEFAULT_POINTWISE_PROMPT: &str = r#"Score how relevant the following document is to the query, on a scale of 0.0 (irrelevant) to 1.0 (maximally relevant). Return ONLY a JSON object of the form {"score": <number>}, with no surrounding prose.

Query: {query}

Document: {document}

Response (JSON only):"#;

/// Reranker adapter that uses a chat-completion model.
///
/// Constructed by the `rerank()` UDTF when the requested model name resolves
/// to a chat model rather than a native reranker. Holds an `Arc<dyn Chat>` and
/// formats prompts per `strategy`.
pub struct LlmRerank {
    chat: Arc<dyn Chat>,
    name: String,
    strategy: LlmStrategy,
    prompt_template: Option<String>,
}

impl Debug for LlmRerank {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LlmRerank")
            .field("name", &self.name)
            .field("strategy", &self.strategy)
            .field("has_custom_prompt", &self.prompt_template.is_some())
            .finish_non_exhaustive()
    }
}

impl LlmRerank {
    #[must_use]
    pub fn new(name: impl Into<String>, chat: Arc<dyn Chat>) -> Self {
        Self {
            chat,
            name: name.into(),
            strategy: LlmStrategy::default(),
            prompt_template: None,
        }
    }

    #[must_use]
    pub fn with_strategy(mut self, strategy: LlmStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    #[must_use]
    pub fn with_prompt_template(mut self, template: Option<String>) -> Self {
        self.prompt_template = template;
        self
    }

    fn format_listwise(&self, query: &str, documents: &[String]) -> String {
        let template = self
            .prompt_template
            .as_deref()
            .unwrap_or(DEFAULT_LISTWISE_PROMPT);
        let docs_block = documents
            .iter()
            .enumerate()
            .map(|(i, d)| format!("{}. {}", i + 1, d))
            .collect::<Vec<_>>()
            .join("\n");
        render_template(
            template,
            &[("{query}", query), ("{documents}", &docs_block)],
        )
    }

    fn format_pointwise(&self, query: &str, document: &str) -> String {
        let template = self
            .prompt_template
            .as_deref()
            .unwrap_or(DEFAULT_POINTWISE_PROMPT);
        render_template(template, &[("{query}", query), ("{document}", document)])
    }

    async fn call_chat(&self, prompt: String) -> Result<String> {
        self.chat
            .run(prompt)
            .await
            .map_err(|e| Error::ModelCallFailed {
                model: self.name.clone(),
                source: Box::new(e),
            })?
            .ok_or_else(|| Error::UnparseableResponse {
                model: self.name.clone(),
                response: "<empty response>".to_string(),
            })
    }

    async fn rerank_listwise(&self, query: &str, documents: &[String]) -> Result<Vec<f32>> {
        if documents.is_empty() {
            return Ok(Vec::new());
        }
        let prompt = self.format_listwise(query, documents);
        let response = self.call_chat(prompt).await?;
        let scores = parse_listwise_response(&response, documents.len(), &self.name)?;
        Ok(scores)
    }

    async fn rerank_pointwise(&self, query: &str, documents: &[String]) -> Result<Vec<f32>> {
        // Run sequentially for v1. Parallelization can be added alongside
        // rate-controller integration when native rerankers land — doing it
        // ad-hoc here would bypass the per-model concurrency controls that
        // already exist for chat models.
        let mut out = Vec::with_capacity(documents.len());
        for doc in documents {
            let prompt = self.format_pointwise(query, doc);
            let response = self.call_chat(prompt).await?;
            let score = parse_pointwise_response(&response, &self.name)?;
            out.push(score);
        }
        Ok(out)
    }
}

#[async_trait]
impl Rerank for LlmRerank {
    async fn rerank(&self, query: &str, documents: &[String]) -> Result<Vec<f32>> {
        let scores = match self.strategy {
            LlmStrategy::Listwise => self.rerank_listwise(query, documents).await?,
            LlmStrategy::Pointwise => self.rerank_pointwise(query, documents).await?,
        };
        if scores.len() != documents.len() {
            return Err(Error::MismatchedScoreCount {
                model: self.name.clone(),
                expected: documents.len(),
                actual: scores.len(),
            });
        }
        Ok(scores)
    }

    fn model_name(&self) -> Option<&str> {
        Some(&self.name)
    }

    fn is_remote(&self) -> bool {
        // LLMs are nearly always remote; local mistral.rs/candle models are an
        // exception but still expensive-per-call so treating them as "remote"
        // for concurrency purposes is the right default.
        true
    }
}

/// Single-pass placeholder substitution. Unlike chained `String::replace`,
/// this walks the template once and only substitutes `{query}` / `{documents}`
/// at template positions — if a user-supplied query or document happens to
/// contain the literal string `{documents}`, it is preserved verbatim rather
/// than being re-processed as a placeholder on a later pass.
fn render_template(template: &str, placeholders: &[(&str, &str)]) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    'outer: while !rest.is_empty() {
        // Find the earliest placeholder occurrence among the registered ones.
        let mut best: Option<(usize, usize, &str)> = None;
        for (marker, value) in placeholders {
            if let Some(pos) = rest.find(marker) {
                let len = marker.len();
                let value_ref: &str = value;
                if best.is_none_or(|(p, _, _)| pos < p) {
                    best = Some((pos, len, value_ref));
                }
            }
        }
        if let Some((pos, len, value)) = best {
            out.push_str(&rest[..pos]);
            out.push_str(value);
            rest = &rest[pos + len..];
            continue 'outer;
        }
        out.push_str(rest);
        break;
    }
    out
}

/// Strip common LLM response wrappers (fenced JSON code blocks, leading/trailing
/// prose) so our JSON parser sees the bare object/array.
fn strip_llm_wrappers(s: &str) -> &str {
    let trimmed = s.trim();
    // Strip ```json ... ``` or ``` ... ``` fences.
    if let Some(rest) = trimmed.strip_prefix("```") {
        let rest = rest.strip_prefix("json").unwrap_or(rest);
        let rest = rest.trim_start_matches(|c: char| c == '\n' || c.is_whitespace());
        if let Some(end) = rest.rfind("```") {
            return rest[..end].trim();
        }
    }
    trimmed
}

/// Extract the first JSON array substring from `s`. Used as a fallback when
/// models wrap the array in prose we couldn't strip with simple fence removal.
fn first_json_array(s: &str) -> Option<&str> {
    let start = s.find('[')?;
    let end = s.rfind(']')?;
    if end > start {
        Some(&s[start..=end])
    } else {
        None
    }
}

/// Extract the first JSON object substring from `s`.
fn first_json_object(s: &str) -> Option<&str> {
    let start = s.find('{')?;
    let end = s.rfind('}')?;
    if end > start {
        Some(&s[start..=end])
    } else {
        None
    }
}

#[derive(serde::Deserialize)]
struct ListwiseEntry {
    id: usize,
    score: f32,
}

#[derive(serde::Deserialize)]
struct PointwiseEntry {
    score: f32,
}

fn parse_listwise_response(raw: &str, expected: usize, model: &str) -> Result<Vec<f32>> {
    let stripped = strip_llm_wrappers(raw);
    let candidate = first_json_array(stripped).unwrap_or(stripped);
    let entries: Vec<ListwiseEntry> =
        serde_json::from_str(candidate).map_err(|_| Error::UnparseableResponse {
            model: model.to_string(),
            response: raw.to_string(),
        })?;

    // Map 1-based ids back to positional scores. Missing ids get 0.0 (treated
    // as least relevant); out-of-range ids are ignored.
    let mut out = vec![0.0_f32; expected];
    for entry in entries {
        if entry.id >= 1 && entry.id <= expected {
            out[entry.id - 1] = entry.score;
        }
    }
    Ok(out)
}

fn parse_pointwise_response(raw: &str, model: &str) -> Result<f32> {
    let stripped = strip_llm_wrappers(raw);
    let candidate = first_json_object(stripped).unwrap_or(stripped);
    let entry: PointwiseEntry =
        serde_json::from_str(candidate).map_err(|_| Error::UnparseableResponse {
            model: model.to_string(),
            response: raw.to_string(),
        })?;
    Ok(entry.score)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_listwise_plain_array() {
        let resp = r#"[{"id":1,"score":0.9},{"id":2,"score":0.3}]"#;
        let scores = parse_listwise_response(resp, 2, "m").expect("parse");
        assert!((scores[0] - 0.9).abs() < 1e-6);
        assert!((scores[1] - 0.3).abs() < 1e-6);
    }

    #[test]
    fn parse_listwise_reorders_by_id() {
        // Model returns entries out of order; we must map back to positional.
        let resp = r#"[{"id":3,"score":0.8},{"id":1,"score":0.1},{"id":2,"score":0.5}]"#;
        let scores = parse_listwise_response(resp, 3, "m").expect("parse");
        assert!((scores[0] - 0.1).abs() < 1e-6);
        assert!((scores[1] - 0.5).abs() < 1e-6);
        assert!((scores[2] - 0.8).abs() < 1e-6);
    }

    #[test]
    fn parse_listwise_strips_code_fences() {
        let resp = "```json\n[{\"id\":1,\"score\":0.42}]\n```";
        let scores = parse_listwise_response(resp, 1, "m").expect("parse");
        assert!((scores[0] - 0.42).abs() < 1e-6);
    }

    #[test]
    fn parse_listwise_handles_surrounding_prose() {
        let resp =
            "Sure! Here is the ranking:\n[{\"id\":1,\"score\":0.7}]\nLet me know if you need more.";
        let scores = parse_listwise_response(resp, 1, "m").expect("parse");
        assert!((scores[0] - 0.7).abs() < 1e-6);
    }

    #[test]
    fn parse_listwise_missing_ids_default_to_zero() {
        // Model only scored 2 of 3 docs; the third defaults to 0.0.
        let resp = r#"[{"id":1,"score":0.6},{"id":3,"score":0.9}]"#;
        let scores = parse_listwise_response(resp, 3, "m").expect("parse");
        assert!((scores[0] - 0.6).abs() < 1e-6);
        assert!((scores[1] - 0.0).abs() < 1e-6);
        assert!((scores[2] - 0.9).abs() < 1e-6);
    }

    #[test]
    fn parse_listwise_ignores_out_of_range_ids() {
        // id=7 is out of range for expected=2; must not panic.
        let resp = r#"[{"id":1,"score":0.5},{"id":7,"score":0.9}]"#;
        let scores = parse_listwise_response(resp, 2, "m").expect("parse");
        assert!((scores[0] - 0.5).abs() < 1e-6);
        assert!((scores[1] - 0.0).abs() < 1e-6);
    }

    #[test]
    fn parse_listwise_malformed_errors() {
        let resp = "not json at all";
        let err =
            parse_listwise_response(resp, 1, "m").expect_err("should fail for unparseable output");
        assert!(matches!(err, Error::UnparseableResponse { .. }));
    }

    #[test]
    fn parse_pointwise_plain_object() {
        let resp = r#"{"score":0.33}"#;
        let score = parse_pointwise_response(resp, "m").expect("parse");
        assert!((score - 0.33).abs() < 1e-6);
    }

    #[test]
    fn parse_pointwise_strips_fences() {
        let resp = "```json\n{\"score\": 0.88}\n```";
        let score = parse_pointwise_response(resp, "m").expect("parse");
        assert!((score - 0.88).abs() < 1e-6);
    }

    #[test]
    fn strategy_parses_case_insensitively() {
        assert_eq!(
            LlmStrategy::parse("Listwise").expect("mixed case listwise"),
            LlmStrategy::Listwise
        );
        assert_eq!(
            LlmStrategy::parse("POINT").expect("upper-case point alias"),
            LlmStrategy::Pointwise
        );
        LlmStrategy::parse("nonsense").expect_err("unknown strategy must error");
    }

    #[test]
    fn listwise_prompt_substitutes_placeholders() {
        // Build an LlmRerank without a real Chat impl by poking at format_listwise
        // through a no-op trait object. Since we only test the string formatting,
        // we construct a Chat stub.
        use async_openai::error::OpenAIError;
        use async_openai::types::chat::{
            ChatCompletionResponseStream, CreateChatCompletionRequest, CreateChatCompletionResponse,
        };

        #[derive(Debug)]
        struct NoopChat;
        #[async_trait::async_trait]
        impl llms_chat_module::Chat for NoopChat {
            fn as_sql(&self) -> Option<&dyn llms_chat_module::nsql::SqlGeneration> {
                None
            }
            async fn chat_stream(
                &self,
                _req: CreateChatCompletionRequest,
            ) -> std::result::Result<ChatCompletionResponseStream, OpenAIError> {
                unimplemented!()
            }
            async fn chat_request(
                &self,
                _req: CreateChatCompletionRequest,
            ) -> std::result::Result<CreateChatCompletionResponse, OpenAIError> {
                unimplemented!()
            }
        }

        let rr = LlmRerank::new("m", Arc::new(NoopChat) as Arc<dyn Chat>);
        let formatted = rr.format_listwise("what is X", &["doc A".into(), "doc B".into()]);
        assert!(formatted.contains("what is X"));
        assert!(formatted.contains("1. doc A"));
        assert!(formatted.contains("2. doc B"));
        // The raw placeholder should no longer appear.
        assert!(!formatted.contains("{query}"));
        assert!(!formatted.contains("{documents}"));
    }

    #[test]
    fn render_template_substitutes_once() {
        // Basic substitution works.
        let out = render_template(
            "Q: {query} / D: {document}",
            &[("{query}", "hi"), ("{document}", "doc")],
        );
        assert_eq!(out, "Q: hi / D: doc");
    }

    #[test]
    fn render_template_preserves_placeholder_tokens_in_user_values() {
        // The classic chained-.replace() bug: if the query contains
        // `{document}`, a later .replace("{document}", ...) would mutate the
        // inserted query. render_template does a single pass so user values
        // are preserved verbatim.
        let out = render_template(
            "Query: {query}\nDoc: {document}",
            &[
                ("{query}", "what is {document}?"),
                ("{document}", "the actual doc"),
            ],
        );
        assert_eq!(out, "Query: what is {document}?\nDoc: the actual doc");
    }

    #[test]
    fn render_template_handles_missing_placeholder() {
        // Template that doesn't use a placeholder leaves user value unused.
        let out = render_template("just text", &[("{query}", "hi")]);
        assert_eq!(out, "just text");
    }

    #[test]
    fn render_template_multiple_occurrences() {
        let out = render_template("{query} and again {query}", &[("{query}", "X")]);
        assert_eq!(out, "X and again X");
    }
}
