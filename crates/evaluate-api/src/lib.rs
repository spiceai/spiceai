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

//! The System One evaluation contract.
//!
//! An evaluation model takes unstructured `state` plus a map of typed
//! `questions` and returns structured `answers` (noul / choice / score) with
//! calibrated probabilities. Implemented by provider crates; called by the
//! runtime's `POST /v1/evaluate` endpoint — which never names a provider.
//!
//! Deliberately separate from chat completions: System One models such as
//! TypeSafe Jev do not generate strings and must not be faked as OpenAI chat.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::Snafu;

/// Name → evaluation model map. Holds System One providers (e.g. TypeSafe Jev).
pub type EvaluateModelStore = std::collections::HashMap<String, Arc<dyn Evaluate>>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Evaluation model '{model}' failed: {source}"))]
    ModelCallFailed {
        model: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Evaluation model '{model}' returned an unparseable response: {response}"
    ))]
    UnparseableResponse { model: String, response: String },

    #[snafu(display(
        "Failed to build HTTP client for evaluation model '{model}' — standard timeout/TLS defaults are unavailable."
    ))]
    HttpClientCreationFailed { model: String },

    #[snafu(display("Evaluation health check failed: {source}"))]
    HealthCheckFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Invalid evaluation request for model '{model}': {message}"
    ))]
    InvalidRequest { model: String, message: String },

    #[snafu(display(
        "Authentication failed for evaluation model '{model}': {message}"
    ))]
    AuthenticationFailed { model: String, message: String },

    #[snafu(display(
        "Rate limited by evaluation provider for model '{model}': {message}"
    ))]
    RateLimited { model: String, message: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A typed question sent to a System One evaluation model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Question {
    /// Yes/no probability question. Answer is `noul` in \[0, 1\] (P(yes)).
    Noul {
        instructions: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        criteria: Option<NoulCriteria>,
    },
    /// Closed-set selection. Answer is the highest-probability option plus the full distribution.
    Choice {
        instructions: String,
        criteria: BTreeMap<String, Option<String>>,
    },
    /// Ordered rubric score. Answer is a probability-weighted value across levels.
    Score {
        instructions: String,
        criteria: Vec<String>,
    },
}

/// Optional yes/no rubric for a noul question.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct NoulCriteria {
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "true")]
    pub true_meaning: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "false")]
    pub false_meaning: Option<String>,
}

/// Request body for `POST /v1/evaluate` and provider System One calls.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EvaluateRequest {
    /// Spicepod model name (runtime) or provider model id (provider forward).
    pub model: String,
    /// State for the model to evaluate: string, object, or array.
    pub state: Value,
    /// Questions keyed by caller-selected identifiers.
    pub questions: BTreeMap<String, Question>,
}

/// Token usage reported by the provider.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

/// A typed answer returned for one question.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Answer {
    Noul {
        noul: f64,
    },
    Choice {
        choice: String,
        probabilities: BTreeMap<String, f64>,
        confidence: f64,
    },
    Score {
        score: f64,
        legend: BTreeMap<String, String>,
        probabilities: BTreeMap<String, f64>,
        confidence: f64,
    },
}

/// Response body for evaluation: typed answers plus provider metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct EvaluateResponse {
    /// Versioned model id that answered (e.g. `jev-1.13.0`), when the provider reports it.
    pub model: String,
    pub answers: BTreeMap<String, Answer>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
}

/// A System One evaluation model: state + typed questions → structured answers.
#[async_trait]
pub trait Evaluate: Send + Sync + Debug {
    /// Run an evaluation against `state` and `questions`.
    ///
    /// The runtime sets `request.model` to the Spicepod model *name*; providers
    /// should substitute their upstream model id before calling the remote API.
    async fn evaluate(&self, request: EvaluateRequest) -> Result<EvaluateResponse>;

    /// Spicepod / runtime name of this model.
    fn model_name(&self) -> &str;

    /// Upstream provider model id (e.g. `jev-latest`).
    fn provider_model_id(&self) -> &str;

    /// Optional health check (e.g. list models). Default is a no-op.
    async fn health(&self) -> Result<()> {
        Ok(())
    }
}
