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

//! [`Rerank`] for `TypeSafe`: a document's score is the calibrated P(relevant) that Jev
//! returns for a System One `noul` question about it. [`LlmStrategy`] picks the shape:
//!
//! - Pointwise: one request per document, `state = {query, document}`.
//! - Listwise: one request, `state = {query, documents: {"0": d0, ...}}`, with one
//!   question per document id.
//!
//! Kept apart from `mod.rs` so `Evaluate` and `Rerank` are never both in scope there:
//! each defines `health()`, and a bare `client.health()` would be ambiguous.

use std::collections::BTreeMap;

use async_trait::async_trait;
use evaluate_api::{Answer, Evaluate, EvaluateRequest, EvaluateResponse, EvaluateState, Question};
use futures::future::try_join_all;
use rerank_api::{Error, Rerank, Result};
use serde_json::{Map, Value};

use super::TypeSafe;
use crate::rerank::LlmStrategy;

const POINTWISE_QUESTION_ID: &str = "relevant";
const POINTWISE_INSTRUCTIONS: &str =
    "Is `document` relevant to `query`, i.e. does it help answer it?";

fn listwise_instructions(id: &str) -> String {
    format!("Is `documents[\"{id}\"]` relevant to `query`, i.e. does it help answer it?")
}

fn noul_question(instructions: String) -> Question {
    Question::Noul {
        instructions: instructions.into(),
        criteria: None,
    }
}

impl TypeSafe {
    async fn ask(
        &self,
        state: Map<String, Value>,
        questions: BTreeMap<String, Question>,
    ) -> Result<EvaluateResponse> {
        let request = EvaluateRequest {
            model: self.name.clone(),
            state: EvaluateState::Object(state),
            questions,
        };
        Evaluate::evaluate(self, request)
            .await
            .map_err(|e| Error::ModelCallFailed {
                model: self.name.clone(),
                source: Box::new(e),
            })
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "a probability in [0, 1] loses only precision as f32"
    )]
    fn noul_score(&self, response: &EvaluateResponse, id: &str) -> Result<f32> {
        // `evaluate` already checked each answer exists, is a noul, and is in [0, 1].
        match response.answers.get(id) {
            Some(Answer::Noul { noul }) => Ok(*noul as f32),
            other => Err(Error::UnparseableResponse {
                model: self.name.clone(),
                response: format!("expected a noul answer for '{id}', got {other:?}"),
            }),
        }
    }

    async fn rerank_pointwise_one(&self, query: &str, document: &str) -> Result<f32> {
        let state = Map::from_iter([
            ("query".to_string(), Value::from(query)),
            ("document".to_string(), Value::from(document)),
        ]);
        let questions = BTreeMap::from([(
            POINTWISE_QUESTION_ID.to_string(),
            noul_question(POINTWISE_INSTRUCTIONS.to_string()),
        )]);
        let response = self.ask(state, questions).await?;
        self.noul_score(&response, POINTWISE_QUESTION_ID)
    }

    /// Calls run concurrently; each takes a permit from the model's rate controller,
    /// which bounds the fan-out.
    async fn rerank_pointwise(&self, query: &str, documents: &[String]) -> Result<Vec<f32>> {
        try_join_all(
            documents
                .iter()
                .map(|doc| self.rerank_pointwise_one(query, doc)),
        )
        .await
    }

    async fn rerank_listwise(&self, query: &str, documents: &[String]) -> Result<Vec<f32>> {
        // System One rejects a request with no questions.
        if documents.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<String> = (0..documents.len()).map(ToString::to_string).collect();
        let state = Map::from_iter([
            ("query".to_string(), Value::from(query)),
            (
                "documents".to_string(),
                Value::Object(
                    ids.iter()
                        .cloned()
                        .zip(documents.iter().map(|d| Value::from(d.as_str())))
                        .collect(),
                ),
            ),
        ]);
        let questions = ids
            .iter()
            .map(|id| (id.clone(), noul_question(listwise_instructions(id))))
            .collect();
        let response = self.ask(state, questions).await?;
        ids.iter()
            .map(|id| self.noul_score(&response, id))
            .collect()
    }
}

#[async_trait]
impl Rerank for TypeSafe {
    async fn rerank(&self, query: &str, documents: &[String]) -> Result<Vec<f32>> {
        match self.rerank_strategy {
            LlmStrategy::Pointwise => self.rerank_pointwise(query, documents).await,
            LlmStrategy::Listwise => self.rerank_listwise(query, documents).await,
        }
    }

    fn model_name(&self) -> Option<&str> {
        Some(&self.name)
    }

    /// Reuse the model-listing check rather than the default, which spends an
    /// evaluation on a dummy document.
    async fn health(&self) -> Result<()> {
        Evaluate::health(self)
            .await
            .map_err(|e| Error::HealthCheckFailed {
                source: Box::new(e),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{body_partial_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn noul_response(noul: f64) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(json!({
            "model": "jev-latest",
            "answers": { "relevant": { "type": "noul", "noul": noul } }
        }))
    }

    async fn mock_document(server: &MockServer, document: &str, noul: f64) {
        Mock::given(method("POST"))
            .and(path("/v1/systemone"))
            .and(body_partial_json(
                json!({ "state": { "document": document } }),
            ))
            .respond_with(noul_response(noul))
            .expect(1)
            .mount(server)
            .await;
    }

    /// Scores come back in input order, one System One call per document.
    #[tokio::test]
    async fn rerank_scores_each_document_in_input_order() {
        let server = MockServer::start().await;
        mock_document(&server, "a", 0.1).await;
        mock_document(&server, "b", 0.9).await;
        mock_document(&server, "c", 0.5).await;

        let client = TypeSafe::try_new("jev", Some("jev"), "k")
            .expect("client")
            .with_base_url(server.uri());
        let docs = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let scores = Rerank::rerank(&client, "q", &docs)
            .await
            .expect("rerank succeeds");

        assert_eq!(scores, vec![0.1, 0.9, 0.5]);

        let received = server.received_requests().await.expect("requests");
        let body: Value = serde_json::from_slice(&received[0].body).expect("json");
        assert_eq!(body["model"], "jev-latest");
        assert_eq!(body["state"]["query"], "q");
        assert_eq!(body["questions"][POINTWISE_QUESTION_ID]["type"], "noul");
    }

    /// System One rejects an empty question map, so no strategy may send one.
    #[tokio::test]
    async fn rerank_of_no_documents_makes_no_call() {
        for strategy in [LlmStrategy::Pointwise, LlmStrategy::Listwise] {
            let client = TypeSafe::try_new("jev", Some("jev"), "k")
                .expect("client")
                .with_base_url("http://127.0.0.1:1")
                .with_rerank_strategy(strategy);
            let scores = Rerank::rerank(&client, "q", &[]).await.expect("empty");
            assert!(scores.is_empty(), "{strategy:?}");
        }
    }

    /// Listwise sends one request and maps each answer back by document id. Eleven
    /// documents make the ids sort differently as strings ("10" < "2") than as indexes.
    #[tokio::test]
    async fn listwise_scores_all_documents_in_one_call_in_input_order() {
        let docs: Vec<String> = (0..11).map(|i| format!("doc {i}")).collect();
        let answers: Map<String, Value> = (0..11)
            .map(|i| {
                (
                    i.to_string(),
                    json!({ "type": "noul", "noul": f64::from(i) / 10.0 }),
                )
            })
            .collect();

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/systemone"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({ "model": "jev-latest", "answers": answers })),
            )
            .expect(1)
            .mount(&server)
            .await;

        let client = TypeSafe::try_new("jev", Some("jev"), "k")
            .expect("client")
            .with_base_url(server.uri())
            .with_rerank_strategy(LlmStrategy::Listwise);
        let scores = Rerank::rerank(&client, "q", &docs)
            .await
            .expect("rerank succeeds");

        #[expect(
            clippy::cast_possible_truncation,
            reason = "test values are exact tenths"
        )]
        let expected: Vec<f32> = (0..11).map(|i| (f64::from(i) / 10.0) as f32).collect();
        assert_eq!(scores, expected);

        let received = server.received_requests().await.expect("requests");
        let body: Value = serde_json::from_slice(&received[0].body).expect("json");
        assert_eq!(body["state"]["query"], "q");
        assert_eq!(body["state"]["documents"]["10"], "doc 10");
        assert_eq!(body["questions"].as_object().map(Map::len), Some(11));
        assert_eq!(body["questions"]["10"]["type"], "noul");
    }

    /// A listwise response that skips a document is a wrong result, not a 0 score.
    #[tokio::test]
    async fn listwise_fails_when_a_document_is_unanswered() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/systemone"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "model": "jev-latest",
                "answers": { "0": { "type": "noul", "noul": 0.4 } }
            })))
            .mount(&server)
            .await;

        let client = TypeSafe::try_new("jev", Some("jev"), "k")
            .expect("client")
            .with_base_url(server.uri())
            .with_rerank_strategy(LlmStrategy::Listwise);
        let err = Rerank::rerank(&client, "q", &["a".to_string(), "b".to_string()])
            .await
            .expect_err("a missing answer fails the rerank");
        assert!(matches!(err, Error::ModelCallFailed { .. }), "{err:?}");
    }

    /// One failed document fails the whole rerank, rather than ranking it last.
    #[tokio::test]
    async fn rerank_fails_when_any_document_fails() {
        let server = MockServer::start().await;
        mock_document(&server, "a", 0.1).await;
        Mock::given(method("POST"))
            .and(path("/v1/systemone"))
            .and(body_partial_json(json!({ "state": { "document": "b" } })))
            .respond_with(ResponseTemplate::new(503).set_body_string("overloaded"))
            .mount(&server)
            .await;

        let client = TypeSafe::try_new("jev", Some("jev"), "k")
            .expect("client")
            .with_base_url(server.uri());
        let err = Rerank::rerank(&client, "q", &["a".to_string(), "b".to_string()])
            .await
            .expect_err("a 503 on one document fails the rerank");
        assert!(matches!(err, Error::ModelCallFailed { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn rerank_health_uses_the_model_listing() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/models"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"models": [{"name": "jev-latest"}]})),
            )
            .mount(&server)
            .await;

        let good = TypeSafe::try_new("jev", Some("jev"), "k")
            .expect("client")
            .with_base_url(server.uri());
        Rerank::health(&good)
            .await
            .expect("listed model is healthy");

        let bad = TypeSafe::try_new("jev", Some("jev-nope"), "k")
            .expect("client")
            .with_base_url(server.uri());
        let err = Rerank::health(&bad).await.expect_err("unlisted model");
        assert!(matches!(err, Error::HealthCheckFailed { .. }), "{err:?}");
    }
}
