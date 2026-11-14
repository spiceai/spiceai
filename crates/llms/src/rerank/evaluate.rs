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
//! [`LlmStrategy`] picks the request shape and what a score means:
//!
//! - Pointwise: one request per document, `state = {query, document}`, with one `noul`
//!   question. The score is P(yes), in \[0, 1\], for "is this document relevant to the
//!   query?", independent of the other candidates.
//! - Listwise: one request, `state = {query, documents: {"0": d0, ...}}`, with one
//!   `choice` question whose options are the document ids. The score of a document is
//!   its probability in the answer's distribution, so the scores compare the candidates
//!   and sum to about 1. A distribution can put most of its mass on the best one or two
//!   documents, which leaves the others tied near 0.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use evaluate_api::{
    Answer, EntryType, Evaluate, EvaluateRequest, EvaluateResponse, EvaluateState, Question,
};
use futures::future::try_join_all;
use serde_json::{Map, Value};

use super::{Error, LlmStrategy, Rerank, Result};

const POINTWISE_QUESTION_ID: &str = "relevant";
const POINTWISE_INSTRUCTIONS: &str =
    "Is `document` relevant to `query`, i.e. does it help answer it?";

const LISTWISE_QUESTION_ID: &str = "best";
const LISTWISE_INSTRUCTIONS: &str = "Which document in `documents` best answers `query`? Probabilities will be used as listwise reranking score. Each option is the key of one document in `documents`.";

/// Most documents one listwise request can rank: one choice option per document, and
/// both `TypeSafe` and `OpenAI` decision models accept at most 255 options.
const MAX_LISTWISE_DOCUMENTS: usize = evaluate_api::openai::MAX_CHOICES;

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
            safety_identifier: None,
            reasoning_effort: None,
            // Only a choice with a boolean option needs typed values. The listwise
            // options are string ids.
            typed_choices: BTreeMap::new(),
        };
        self.evaluator
            .evaluate(request)
            .await
            .map_err(|e| Error::ModelCallFailed {
                model: self.name.clone(),
                source: Box::new(e),
            })
    }

    fn unparseable(&self, response: String) -> Error {
        Error::UnparseableResponse {
            model: self.name.clone(),
            response,
        }
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "a probability in [0, 1] loses only precision as f32"
    )]
    fn noul_score(&self, response: &EvaluateResponse, id: &str) -> Result<f32> {
        match response.answers.get(id) {
            Some(Answer::Noul { noul }) if (0.0..=1.0).contains(noul) => Ok(*noul as f32),
            other => Err(self.unparseable(format!(
                "expected a noul in [0, 1] for '{id}', got {other:?}"
            ))),
        }
    }

    /// The probability of each option in `ids`, in the order of `ids`.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "a probability in [0, 1] loses only precision as f32"
    )]
    fn choice_scores(&self, answer: Option<&Answer>, ids: &[String]) -> Result<Vec<f32>> {
        let Some(Answer::Choice { probabilities, .. }) = answer else {
            return Err(self.unparseable(format!(
                "expected a choice for '{LISTWISE_QUESTION_ID}', got {answer:?}"
            )));
        };
        ids.iter()
            .map(|id| match probabilities.get(id) {
                Some(p) if (0.0..=1.0).contains(p) => Ok(*p as f32),
                other => Err(self.unparseable(format!(
                    "expected a probability in [0, 1] for document '{id}', got {other:?}"
                ))),
            })
            .collect()
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

    /// One choice question over all the documents, so the model compares them in one
    /// judgment.
    ///
    /// More than [`MAX_LISTWISE_DOCUMENTS`] fails instead of being split into windows:
    /// a probability is relative to the documents in its own request, so scores from
    /// different windows are not comparable.
    async fn rerank_listwise(&self, query: &str, documents: &[String]) -> Result<Vec<f32>> {
        // An evaluation needs at least one question, and a choice at least one option.
        if documents.is_empty() {
            return Ok(Vec::new());
        }
        if documents.len() > MAX_LISTWISE_DOCUMENTS {
            return Err(Error::TooManyDocuments {
                model: self.name.clone(),
                count: documents.len(),
                max: MAX_LISTWISE_DOCUMENTS,
            });
        }
        let ids: Vec<String> = (0..documents.len()).map(|i| i.to_string()).collect();
        let question = Question::Choice {
            instructions: LISTWISE_INSTRUCTIONS.into(),
            // The text stays in `state.documents`; each option is only its id.
            criteria: ids.iter().map(|id| (id.clone(), EntryType::Null)).collect(),
        };
        // A single document is the only option, so it needs no request.
        if let Some(answer) = question.foregone_answer() {
            return self.choice_scores(Some(&answer), &ids);
        }
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
        let questions = BTreeMap::from([(LISTWISE_QUESTION_ID.to_string(), question)]);
        let response = self.ask(state, questions).await?;
        self.choice_scores(response.answers.get(LISTWISE_QUESTION_ID), &ids)
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

    /// Answers each question with `answer(state, question)` and records every request
    /// it receives.
    struct MockEvaluate {
        answer: fn(&EvaluateState, &Question) -> Option<Answer>,
        requests: Mutex<Vec<EvaluateRequest>>,
    }

    impl Debug for MockEvaluate {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("MockEvaluate")
        }
    }

    impl MockEvaluate {
        fn new(answer: fn(&EvaluateState, &Question) -> Option<Answer>) -> Arc<Self> {
            Arc::new(Self {
                answer,
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
                .iter()
                .filter_map(|(id, question)| {
                    (self.answer)(&request.state, question).map(|answer| (id.clone(), answer))
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

    fn option_ids(question: &Question) -> Vec<&String> {
        match question {
            Question::Choice { criteria, .. } => criteria.keys().collect(),
            _ => Vec::new(),
        }
    }

    /// A choice answer over `probabilities`, choosing a highest-probability option.
    fn choice(probabilities: BTreeMap<String, f64>) -> Answer {
        let choice = probabilities
            .iter()
            .max_by(|a, b| a.1.total_cmp(b.1))
            .map(|(id, _)| id.clone())
            .unwrap_or_default();
        Answer::Choice {
            choice,
            probabilities,
            confidence: 0.5,
        }
    }

    /// Pointwise: the document text is the digit to score, e.g. "3" → 0.3.
    fn pointwise_score(state: &EvaluateState, _question: &Question) -> Option<Answer> {
        let doc = state_field(state, "document")?.as_str()?;
        let n = doc.parse::<u8>().ok()?;
        Some(Answer::Noul {
            noul: f64::from(n) / 10.0,
        })
    }

    /// Listwise: the probability of document id `n` is `n / 55`, a distribution over
    /// the eleven ids 0..=10.
    #[expect(clippy::unnecessary_wraps, reason = "matches the mock's fn signature")]
    fn listwise_score(_state: &EvaluateState, question: &Question) -> Option<Answer> {
        let probabilities = option_ids(question)
            .into_iter()
            .map(|id| (id.clone(), id.parse::<u8>().map_or(0.0, f64::from) / 55.0))
            .collect();
        Some(choice(probabilities))
    }

    fn docs(texts: &[&str]) -> Vec<String> {
        texts.iter().map(ToString::to_string).collect()
    }

    fn listwise(answer: fn(&EvaluateState, &Question) -> Option<Answer>) -> EvaluateRerank {
        EvaluateRerank::new("jev", MockEvaluate::new(answer)).with_strategy(LlmStrategy::Listwise)
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
            && matches!(r.questions.values().next(), Some(Question::Noul { .. }))
            && state_field(&r.state, "query") == Some(&Value::from("q"))));
    }

    /// Eleven documents make the ids sort differently as strings ("10" < "2") than as
    /// indexes, so this catches mapping probabilities back by position.
    #[tokio::test]
    async fn listwise_asks_one_choice_and_scores_from_its_distribution_by_id() {
        let mock = MockEvaluate::new(listwise_score);
        let reranker = EvaluateRerank::new("jev", Arc::clone(&mock) as Arc<dyn Evaluate>)
            .with_strategy(LlmStrategy::Listwise);
        let texts: Vec<String> = (0..11).map(|i| format!("doc {i}")).collect();

        let scores = reranker.rerank("q", &texts).await.expect("rerank");

        #[expect(
            clippy::cast_possible_truncation,
            reason = "the test compares the same f64 → f32 cast the adapter makes"
        )]
        let expected: Vec<f32> = (0..11u8).map(|i| (f64::from(i) / 55.0) as f32).collect();
        assert_eq!(scores, expected);
        let requests = mock.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].questions.len(), 1);
        let question = &requests[0].questions[LISTWISE_QUESTION_ID];
        let mut expected_ids: Vec<String> = (0..11).map(|i| i.to_string()).collect();
        expected_ids.sort();
        assert_eq!(
            option_ids(question),
            expected_ids.iter().collect::<Vec<_>>()
        );
        // The option is only an id; its text is in `state.documents`.
        let Question::Choice { criteria, .. } = question else {
            panic!("listwise asks a choice, got {question:?}");
        };
        assert!(criteria.values().all(|entry| *entry == EntryType::Null));
        let documents = state_field(&requests[0].state, "documents").expect("documents");
        assert_eq!(documents["10"], "doc 10");
    }

    /// A single document is a choice with one option, which needs no model.
    #[tokio::test]
    async fn listwise_scores_a_single_document_without_a_request() {
        let mock = MockEvaluate::new(listwise_score);
        let reranker = EvaluateRerank::new("jev", Arc::clone(&mock) as Arc<dyn Evaluate>)
            .with_strategy(LlmStrategy::Listwise);

        let scores = reranker.rerank("q", &docs(&["a"])).await.expect("rerank");

        assert_eq!(scores, vec![1.0]);
        assert!(mock.requests().is_empty());
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

    /// More documents than a choice can have options fails before any request, rather
    /// than ranking only some of them.
    #[tokio::test]
    async fn listwise_fails_over_the_option_limit_without_a_request() {
        let mock = MockEvaluate::new(listwise_score);
        let reranker = EvaluateRerank::new("jev", Arc::clone(&mock) as Arc<dyn Evaluate>)
            .with_strategy(LlmStrategy::Listwise);
        let texts: Vec<String> = (0..=MAX_LISTWISE_DOCUMENTS)
            .map(|i| format!("doc {i}"))
            .collect();

        let err = reranker
            .rerank("q", &texts)
            .await
            .expect_err("too many documents for one choice");
        assert!(
            matches!(err, Error::TooManyDocuments { count, max, .. }
                if count == MAX_LISTWISE_DOCUMENTS + 1 && max == MAX_LISTWISE_DOCUMENTS),
            "{err:?}"
        );
        assert!(mock.requests().is_empty());
    }

    /// A document missing from the distribution is a wrong result, not a 0 score.
    #[tokio::test]
    async fn listwise_fails_when_a_document_has_no_probability() {
        #[expect(clippy::unnecessary_wraps, reason = "matches the mock's fn signature")]
        fn skip_one(_state: &EvaluateState, _question: &Question) -> Option<Answer> {
            Some(choice(BTreeMap::from([("0".to_string(), 1.0)])))
        }

        let err = listwise(skip_one)
            .rerank("q", &docs(&["a", "b"]))
            .await
            .expect_err("a missing probability fails the rerank");
        assert!(matches!(err, Error::UnparseableResponse { .. }), "{err:?}");
    }

    /// The adapter does not trust every evaluation model to range-check its probabilities.
    #[tokio::test]
    async fn listwise_fails_on_a_probability_outside_zero_to_one() {
        #[expect(clippy::unnecessary_wraps, reason = "matches the mock's fn signature")]
        fn out_of_range(_state: &EvaluateState, _question: &Question) -> Option<Answer> {
            Some(choice(BTreeMap::from([
                ("0".to_string(), 1.7),
                ("1".to_string(), -0.7),
            ])))
        }

        let err = listwise(out_of_range)
            .rerank("q", &docs(&["a", "b"]))
            .await
            .expect_err("an out-of-range probability fails the rerank");
        assert!(matches!(err, Error::UnparseableResponse { .. }), "{err:?}");
    }

    /// A listwise answer of another kind is a wrong result.
    #[tokio::test]
    async fn listwise_fails_when_the_answer_is_not_a_choice() {
        #[expect(clippy::unnecessary_wraps, reason = "matches the mock's fn signature")]
        fn noul(_state: &EvaluateState, _question: &Question) -> Option<Answer> {
            Some(Answer::Noul { noul: 0.4 })
        }

        let err = listwise(noul)
            .rerank("q", &docs(&["a", "b"]))
            .await
            .expect_err("a noul answer to a choice fails the rerank");
        assert!(matches!(err, Error::UnparseableResponse { .. }), "{err:?}");
    }

    /// The adapter does not trust every evaluation model to range-check its nouls.
    #[tokio::test]
    async fn a_noul_outside_zero_to_one_fails() {
        #[expect(clippy::unnecessary_wraps, reason = "matches the mock's fn signature")]
        fn out_of_range(_state: &EvaluateState, _question: &Question) -> Option<Answer> {
            Some(Answer::Noul { noul: 1.7 })
        }
        let reranker = EvaluateRerank::new("jev", MockEvaluate::new(out_of_range));

        let err = reranker
            .rerank("q", &docs(&["a"]))
            .await
            .expect_err("an out-of-range noul fails the rerank");
        assert!(matches!(err, Error::UnparseableResponse { .. }), "{err:?}");
    }
}
