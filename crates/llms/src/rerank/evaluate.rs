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

//! Reranker adapter over an [`Evaluate`] model (e.g. `TypeSafe` Jev).
//!
//! A document's score is the `noul` (P(yes), in \[0, 1\]) the model returns for "is this
//! document relevant to the query?". [`LlmStrategy`] picks the request shape:
//!
//! - Pointwise: one request per document, `state = {query, document}`.
//! - Listwise: one request, `state = {query, documents: {"0": d0, ...}}`, with one
//!   question per document id.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use evaluate_api::{Answer, Evaluate, EvaluateRequest, EvaluateResponse, EvaluateState, Question};
use futures::future::try_join_all;
use serde_json::{Map, Value};

use super::{Error, LlmStrategy, Rerank, Result};

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

/// Reranker adapter that uses an evaluation model.
///
/// Constructed by the `rerank()` UDTF when the requested model name resolves to an
/// evaluation model, so `strategy => ...` applies per query as it does for
/// [`super::LlmRerank`].
pub struct EvaluateRerank {
    evaluator: Arc<dyn Evaluate>,
    name: String,
    strategy: LlmStrategy,
}

impl Debug for EvaluateRerank {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EvaluateRerank")
            .field("name", &self.name)
            .field("strategy", &self.strategy)
            .finish_non_exhaustive()
    }
}

impl EvaluateRerank {
    #[must_use]
    pub fn new(name: impl Into<String>, evaluator: Arc<dyn Evaluate>) -> Self {
        Self {
            evaluator,
            name: name.into(),
            // Not `LlmStrategy::default()` (listwise): pointwise keeps each document's
            // score independent of the other candidates in the request.
            strategy: LlmStrategy::Pointwise,
        }
    }

    #[must_use]
    pub fn with_strategy(mut self, strategy: LlmStrategy) -> Self {
        self.strategy = strategy;
        self
    }

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
        self.evaluator
            .evaluate(request)
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
        match response.answers.get(id) {
            Some(Answer::Noul { noul }) if (0.0..=1.0).contains(noul) => Ok(*noul as f32),
            other => Err(Error::UnparseableResponse {
                model: self.name.clone(),
                response: format!("expected a noul in [0, 1] for '{id}', got {other:?}"),
            }),
        }
    }

    async fn score_one(&self, query: &str, document: &str) -> Result<f32> {
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

    /// Requests run concurrently. Rate limits are the evaluation model's own: `TypeSafe`
    /// takes a rate-controller permit per call.
    async fn rerank_pointwise(&self, query: &str, documents: &[String]) -> Result<Vec<f32>> {
        try_join_all(documents.iter().map(|doc| self.score_one(query, doc))).await
    }

    async fn rerank_listwise(&self, query: &str, documents: &[String]) -> Result<Vec<f32>> {
        // An evaluation needs at least one question.
        if documents.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<String> = (0..documents.len()).map(|i| i.to_string()).collect();
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
impl Rerank for EvaluateRerank {
    async fn rerank(&self, query: &str, documents: &[String]) -> Result<Vec<f32>> {
        match self.strategy {
            LlmStrategy::Pointwise => self.rerank_pointwise(query, documents).await,
            LlmStrategy::Listwise => self.rerank_listwise(query, documents).await,
        }
    }

    fn model_name(&self) -> Option<&str> {
        Some(&self.name)
    }

    /// The evaluation model's own check, rather than the default, which spends an
    /// evaluation on a dummy document.
    async fn health(&self) -> Result<()> {
        self.evaluator
            .health()
            .await
            .map_err(|e| Error::HealthCheckFailed {
                source: Box::new(e),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Answers each question with the noul `score(state, question_id)` and records
    /// every request it receives.
    struct MockEvaluate {
        score: fn(&EvaluateState, &str) -> Option<f64>,
        requests: Mutex<Vec<EvaluateRequest>>,
    }

    impl Debug for MockEvaluate {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("MockEvaluate")
        }
    }

    impl MockEvaluate {
        fn new(score: fn(&EvaluateState, &str) -> Option<f64>) -> Arc<Self> {
            Arc::new(Self {
                score,
                requests: Mutex::new(Vec::new()),
            })
        }

        fn requests(&self) -> Vec<EvaluateRequest> {
            self.requests.lock().expect("lock").clone()
        }
    }

    #[async_trait]
    impl Evaluate for MockEvaluate {
        async fn evaluate(
            &self,
            request: EvaluateRequest,
        ) -> evaluate_api::Result<EvaluateResponse> {
            let answers = request
                .questions
                .keys()
                .filter_map(|id| {
                    (self.score)(&request.state, id).map(|noul| (id.clone(), Answer::Noul { noul }))
                })
                .collect();
            self.requests.lock().expect("lock").push(request);
            Ok(EvaluateResponse {
                model: "mock".into(),
                answers,
                usage: None,
            })
        }

        async fn health(&self) -> evaluate_api::Result<()> {
            Ok(())
        }
    }

    fn state_field<'a>(state: &'a EvaluateState, key: &str) -> Option<&'a Value> {
        match state {
            EvaluateState::Object(map) => map.get(key),
            _ => None,
        }
    }

    /// Pointwise: the document text is the digit to score, e.g. "3" → 0.3.
    fn pointwise_score(state: &EvaluateState, _id: &str) -> Option<f64> {
        let doc = state_field(state, "document")?.as_str()?;
        doc.parse::<u8>().ok().map(|n| f64::from(n) / 10.0)
    }

    /// Listwise: the score of document id `n` is `n / 10`.
    fn listwise_score(_state: &EvaluateState, id: &str) -> Option<f64> {
        id.parse::<u8>().ok().map(|n| f64::from(n) / 10.0)
    }

    fn docs(texts: &[&str]) -> Vec<String> {
        texts.iter().map(ToString::to_string).collect()
    }

    #[tokio::test]
    async fn pointwise_is_the_default_and_sends_one_request_per_document() {
        let mock = MockEvaluate::new(pointwise_score);
        let reranker = EvaluateRerank::new("jev", Arc::clone(&mock) as Arc<dyn Evaluate>);

        let scores = reranker
            .rerank("q", &docs(&["1", "9", "5"]))
            .await
            .expect("rerank");

        assert_eq!(scores, vec![0.1, 0.9, 0.5]);
        let requests = mock.requests();
        assert_eq!(requests.len(), 3);
        assert!(requests.iter().all(|r| r.model == "jev"
            && r.questions.len() == 1
            && state_field(&r.state, "query") == Some(&Value::from("q"))));
    }

    /// Eleven documents make the ids sort differently as strings ("10" < "2") than as
    /// indexes, so this catches mapping answers back by position.
    #[tokio::test]
    async fn listwise_sends_one_request_and_maps_answers_by_id() {
        let mock = MockEvaluate::new(listwise_score);
        let reranker = EvaluateRerank::new("jev", Arc::clone(&mock) as Arc<dyn Evaluate>)
            .with_strategy(LlmStrategy::Listwise);
        let texts: Vec<String> = (0..11).map(|i| format!("doc {i}")).collect();

        let scores = reranker.rerank("q", &texts).await.expect("rerank");

        #[expect(
            clippy::cast_possible_truncation,
            reason = "test values are exact tenths"
        )]
        let expected: Vec<f32> = (0..11u8).map(|i| (f64::from(i) / 10.0) as f32).collect();
        assert_eq!(scores, expected);
        let requests = mock.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].questions.len(), 11);
        let documents = state_field(&requests[0].state, "documents").expect("documents");
        assert_eq!(documents["10"], "doc 10");
    }

    #[tokio::test]
    async fn no_documents_makes_no_request() {
        for strategy in [LlmStrategy::Pointwise, LlmStrategy::Listwise] {
            let mock = MockEvaluate::new(listwise_score);
            let reranker = EvaluateRerank::new("jev", Arc::clone(&mock) as Arc<dyn Evaluate>)
                .with_strategy(strategy);
            let scores = reranker.rerank("q", &[]).await.expect("rerank");
            assert!(scores.is_empty(), "{strategy:?}");
            assert!(mock.requests().is_empty(), "{strategy:?}");
        }
    }

    /// An unanswered document is a wrong result, not a 0 score.
    #[tokio::test]
    async fn listwise_fails_when_a_document_is_unanswered() {
        fn skip_last(_state: &EvaluateState, id: &str) -> Option<f64> {
            (id != "1").then_some(0.4)
        }
        let reranker = EvaluateRerank::new("jev", MockEvaluate::new(skip_last))
            .with_strategy(LlmStrategy::Listwise);

        let err = reranker
            .rerank("q", &docs(&["a", "b"]))
            .await
            .expect_err("a missing answer fails the rerank");
        assert!(matches!(err, Error::UnparseableResponse { .. }), "{err:?}");
    }

    /// The adapter does not trust every evaluation model to range-check its nouls.
    #[tokio::test]
    async fn a_noul_outside_zero_to_one_fails() {
        // `MockEvaluate` takes a fn returning `Option`, so the wrap is required.
        #[expect(clippy::unnecessary_wraps)]
        fn out_of_range(_state: &EvaluateState, _id: &str) -> Option<f64> {
            Some(1.7)
        }
        let reranker = EvaluateRerank::new("jev", MockEvaluate::new(out_of_range));

        let err = reranker
            .rerank("q", &docs(&["a"]))
            .await
            .expect_err("an out-of-range noul fails the rerank");
        assert!(matches!(err, Error::UnparseableResponse { .. }), "{err:?}");
    }
}
