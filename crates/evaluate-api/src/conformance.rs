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

//! A conformance suite for [`Evaluate`] implementations.
//!
//! Every provider crate runs the same [`cases`] against its model, so each is held to the
//! same contract: the response answers every question with the matching kind of answer
//! ([`check_answers`]), and the answer to an unambiguous question is the obvious one.
//!
//! The cases use evidence a model cannot reasonably misread, and thresholds that only
//! separate the two sides (a probability above or below one half), so a live model passes
//! without the test pinning its exact numbers.

use std::collections::BTreeMap;
use std::fmt::{self, Display};

use serde_json::json;

use crate::{
    Answer, Evaluate, EvaluateRequest, EvaluateResponse, EvaluateState, NullableEntry, Question,
    check_answers,
};

/// What a case requires of the answer to one question.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Expectation {
    /// A noul answer strictly above this probability.
    NoulAbove(f64),
    /// A noul answer strictly below this probability.
    NoulBelow(f64),
    /// A choice answer that selects this option.
    Chooses(&'static str),
    /// A score answer of at least this value.
    ScoreAtLeast(f64),
    /// A score answer of at most this value.
    ScoreAtMost(f64),
}

impl Expectation {
    /// `None` when `answer` meets the expectation, otherwise why it does not.
    fn violation(self, answer: &Answer) -> Option<String> {
        match (self, answer) {
            (Self::NoulAbove(floor), Answer::Noul { noul }) if *noul > floor => None,
            (Self::NoulAbove(floor), Answer::Noul { noul }) => {
                Some(format!("noul {noul} is not above {floor}"))
            }
            (Self::NoulBelow(ceiling), Answer::Noul { noul }) if *noul < ceiling => None,
            (Self::NoulBelow(ceiling), Answer::Noul { noul }) => {
                Some(format!("noul {noul} is not below {ceiling}"))
            }
            (Self::Chooses(option), Answer::Choice { choice, .. }) if choice == option => None,
            (Self::Chooses(option), Answer::Choice { choice, .. }) => {
                Some(format!("chose '{choice}', not '{option}'"))
            }
            (Self::ScoreAtLeast(floor), Answer::Score { score, .. }) if *score >= floor => None,
            (Self::ScoreAtLeast(floor), Answer::Score { score, .. }) => {
                Some(format!("score {score} is below {floor}"))
            }
            (Self::ScoreAtMost(ceiling), Answer::Score { score, .. }) if *score <= ceiling => None,
            (Self::ScoreAtMost(ceiling), Answer::Score { score, .. }) => {
                Some(format!("score {score} is above {ceiling}"))
            }
            (_, Answer::Refusal {}) => Some("the model refused to answer".to_string()),
            (expected, got) => Some(format!("expected {expected:?}, got {got:?}")),
        }
    }
}

/// One request and what its answers must be.
#[derive(Debug, Clone)]
pub struct Case {
    pub name: &'static str,
    pub state: EvaluateState,
    pub questions: BTreeMap<String, Question>,
    /// Question id → what its answer must be. A question without an entry is checked
    /// only for shape.
    pub expectations: BTreeMap<String, Expectation>,
}

impl Case {
    /// The request that asks this case of the model `model`.
    #[must_use]
    pub fn request(&self, model: &str) -> EvaluateRequest {
        EvaluateRequest {
            model: model.to_string(),
            state: self.state.clone(),
            questions: self.questions.clone(),
            safety_identifier: None,
            reasoning_effort: None,
            typed_choices: BTreeMap::new(),
        }
    }
}

/// A conformance check a model failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    /// The case that failed, or `health` for the health check.
    pub case: &'static str,
    pub detail: String,
}

impl Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.case, self.detail)
    }
}

/// Every failure on its own line, for an assertion message.
#[must_use]
pub fn report(failures: &[Failure]) -> String {
    failures
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n")
}

fn noul(instructions: &str) -> Question {
    Question::Noul {
        instructions: NullableEntry::from(instructions),
        criteria: None,
    }
}

fn choice(instructions: &str, options: &[(&str, &str)]) -> Question {
    Question::Choice {
        instructions: NullableEntry::from(instructions),
        criteria: options
            .iter()
            .map(|(id, description)| ((*id).to_string(), (*description).into()))
            .collect(),
    }
}

fn score(instructions: &str, levels: &[&str]) -> Question {
    Question::Score {
        instructions: NullableEntry::from(instructions),
        criteria: levels.iter().map(|level| (*level).into()).collect(),
    }
}

fn case(
    name: &'static str,
    state: impl Into<EvaluateState>,
    questions: Vec<(&str, Question, Option<Expectation>)>,
) -> Case {
    let mut asked = BTreeMap::new();
    let mut expectations = BTreeMap::new();
    for (id, question, expectation) in questions {
        asked.insert(id.to_string(), question);
        if let Some(expectation) = expectation {
            expectations.insert(id.to_string(), expectation);
        }
    }
    Case {
        name,
        state: state.into(),
        questions: asked,
        expectations,
    }
}

const REFUND_QUESTION: &str = "Is the customer asking for a refund?";
const TEAM_QUESTION: &str = "Which team should handle this message?";
const MOOD_QUESTION: &str = "How upset is the customer?";
const TEAMS: [(&str, &str); 3] = [
    ("billing", "Charges, invoices and refunds"),
    ("technical", "Bugs and outages"),
    ("account", "Logins and profile changes"),
];
const MOODS: [&str; 3] = ["calm", "annoyed", "furious"];

/// The cases every [`Evaluate`] implementation must pass.
#[must_use]
pub fn cases() -> Vec<Case> {
    let wants_refund = "I was charged twice for my order and I want my money back today.";
    vec![
        case(
            "noul_true",
            wants_refund,
            vec![(
                "refund",
                noul(REFUND_QUESTION),
                Some(Expectation::NoulAbove(0.5)),
            )],
        ),
        case(
            "noul_false",
            "Thank you! The order arrived early and works perfectly.",
            vec![(
                "refund",
                noul(REFUND_QUESTION),
                Some(Expectation::NoulBelow(0.5)),
            )],
        ),
        case(
            "choice",
            "My invoice shows the same charge twice.",
            vec![(
                "team",
                choice(TEAM_QUESTION, &TEAMS),
                Some(Expectation::Chooses("billing")),
            )],
        ),
        case(
            "score",
            "THIS IS UNACCEPTABLE. Third outage this week and nobody will answer me. \
             I am cancelling everything.",
            vec![(
                "mood",
                score(MOOD_QUESTION, &MOODS),
                Some(Expectation::ScoreAtLeast(1.0)),
            )],
        ),
        case(
            "several_questions",
            wants_refund,
            vec![
                (
                    "refund",
                    noul(REFUND_QUESTION),
                    Some(Expectation::NoulAbove(0.5)),
                ),
                (
                    "spam",
                    noul("Is this message spam?"),
                    Some(Expectation::NoulBelow(0.5)),
                ),
                (
                    "team",
                    choice(TEAM_QUESTION, &TEAMS),
                    Some(Expectation::Chooses("billing")),
                ),
                ("mood", score(MOOD_QUESTION, &MOODS), None),
            ],
        ),
        case(
            "single_option_choice",
            "Any message at all.",
            vec![(
                "only",
                choice("Which option applies?", &[("sole", "The only option")]),
                Some(Expectation::Chooses("sole")),
            )],
        ),
        Case {
            state: EvaluateState::Object(
                json!({"subject": "Double charge", "body": wants_refund})
                    .as_object()
                    .cloned()
                    .unwrap_or_default(),
            ),
            ..case(
                "object_state",
                "",
                vec![(
                    "refund",
                    noul(REFUND_QUESTION),
                    Some(Expectation::NoulAbove(0.5)),
                )],
            )
        },
    ]
}

/// Asks `model` the case `case` as the model named `name` and returns the first way the
/// response breaks the contract.
///
/// # Errors
///
/// Returns the detail of the failure: the model's own error, an answer that does not fit
/// its question, a response naming another model, or an answer the case does not accept.
pub async fn run_case(model: &dyn Evaluate, name: &str, case: &Case) -> Result<(), String> {
    let request = case.request(name);
    let response = model
        .evaluate(request)
        .await
        .map_err(|e| format!("the model failed: {e}"))?;
    judge(name, case, &response)
}

fn judge(name: &str, case: &Case, response: &EvaluateResponse) -> Result<(), String> {
    check_answers(name, &case.questions, response).map_err(|e| e.to_string())?;
    if response.model != name {
        return Err(format!(
            "the response names model '{}', not the requested '{name}'",
            response.model
        ));
    }
    for (id, expectation) in &case.expectations {
        let Some(answer) = response.answers.get(id) else {
            return Err(format!("no answer for '{id}'"));
        };
        if let Some(violation) = expectation.violation(answer) {
            return Err(format!("question '{id}': {violation}"));
        }
    }
    Ok(())
}

/// Runs the health check and every case against `model`, named `name`, and returns every
/// failure; an empty list means the model conforms. One failing case does not stop the
/// others, so a single run shows everything a provider gets wrong.
pub async fn run_all(model: &dyn Evaluate, name: &str) -> Vec<Failure> {
    let mut failures = Vec::new();
    if let Err(e) = model.health().await {
        failures.push(Failure {
            case: "health",
            detail: e.to_string(),
        });
    }
    for case in cases() {
        if let Err(detail) = run_case(model, name, &case).await {
            failures.push(Failure {
                case: case.name,
                detail,
            });
        }
    }
    failures
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;
    use crate::{Error, Result};

    /// Answers every question as `answer_for` says, naming `reported_model`.
    #[derive(Debug)]
    struct Scripted {
        reported_model: &'static str,
        healthy: bool,
        refuse: bool,
        flip: bool,
    }

    impl Scripted {
        fn correct() -> Self {
            Self {
                reported_model: "scripted",
                healthy: true,
                refuse: false,
                flip: false,
            }
        }
    }

    /// The answer a perfect model gives: it reads the obvious side of every question
    /// from `evidence`, which `flip` reverses.
    fn answer_for(evidence: &str, id: &str, question: &Question, flip: bool) -> Answer {
        match question {
            Question::Noul { .. } => {
                let yes = id == "refund" && evidence.contains("money back");
                Answer::Noul {
                    noul: if yes != flip { 0.9 } else { 0.1 },
                }
            }
            Question::Choice { criteria, .. } => {
                let pick = if flip {
                    criteria.keys().next_back()
                } else {
                    criteria
                        .keys()
                        .find(|key| *key == "billing")
                        .or_else(|| criteria.keys().next())
                };
                let choice = pick.cloned().unwrap_or_default();
                let probabilities = criteria
                    .keys()
                    .map(|key| (key.clone(), if *key == choice { 1.0 } else { 0.0 }))
                    .collect();
                Answer::Choice {
                    choice,
                    probabilities,
                    confidence: 1.0,
                }
            }
            Question::Score { criteria, .. } => {
                let top = criteria.len().saturating_sub(1);
                let furious = evidence.contains("UNACCEPTABLE");
                let level = if furious != flip { top } else { 0 };
                let probabilities = (0..=top)
                    .map(|i| (i.to_string(), if i == level { 1.0 } else { 0.0 }))
                    .collect();
                #[expect(clippy::cast_precision_loss, reason = "at most ten levels")]
                let score = level as f64;
                Answer::Score {
                    score,
                    legend: BTreeMap::new(),
                    probabilities,
                    confidence: 1.0,
                }
            }
        }
    }

    #[async_trait]
    impl Evaluate for Scripted {
        async fn evaluate(&self, request: EvaluateRequest) -> Result<EvaluateResponse> {
            let evidence = serde_json::to_string(&request.state).unwrap_or_default();
            let answers = request
                .questions
                .iter()
                .map(|(id, question)| {
                    let answer = if self.refuse {
                        Answer::Refusal {}
                    } else {
                        answer_for(&evidence, id, question, self.flip)
                    };
                    (id.clone(), answer)
                })
                .collect();
            Ok(EvaluateResponse {
                model: self.reported_model.to_string(),
                answers,
                usage: None,
            })
        }

        async fn health(&self) -> Result<()> {
            if self.healthy {
                Ok(())
            } else {
                Err(Error::HealthCheckFailed {
                    source: "down".into(),
                })
            }
        }
    }

    #[tokio::test]
    async fn a_model_that_reads_the_evidence_conforms() {
        let failures = run_all(&Scripted::correct(), "scripted").await;
        assert!(failures.is_empty(), "{}", report(&failures));
    }

    #[tokio::test]
    async fn a_model_that_reads_the_evidence_backwards_fails_every_expectation() {
        let model = Scripted {
            flip: true,
            ..Scripted::correct()
        };
        let failed: Vec<&str> = run_all(&model, "scripted")
            .await
            .iter()
            .map(|failure| failure.case)
            .collect();
        // The single-option choice has only one answer to give, so a flip cannot move it.
        assert_eq!(
            failed,
            [
                "noul_true",
                "noul_false",
                "choice",
                "score",
                "several_questions",
                "object_state"
            ]
        );
    }

    #[tokio::test]
    async fn a_refusal_fails_the_case() {
        let model = Scripted {
            refuse: true,
            ..Scripted::correct()
        };
        let failures = run_all(&model, "scripted").await;
        assert_eq!(failures.len(), cases().len());
        assert!(
            failures
                .iter()
                .all(|failure| failure.detail.contains("refused")),
            "{}",
            report(&failures)
        );
    }

    #[tokio::test]
    async fn a_response_naming_another_model_fails() {
        let model = Scripted {
            reported_model: "upstream-id",
            ..Scripted::correct()
        };
        let failures = run_all(&model, "scripted").await;
        assert_eq!(failures.len(), cases().len());
        assert!(
            failures
                .iter()
                .all(|failure| failure.detail.contains("upstream-id")),
            "{}",
            report(&failures)
        );
    }

    #[tokio::test]
    async fn an_unhealthy_model_fails_the_health_check_and_still_runs_the_cases() {
        let model = Scripted {
            healthy: false,
            ..Scripted::correct()
        };
        let failures = run_all(&model, "scripted").await;
        assert_eq!(
            failures,
            [Failure {
                case: "health",
                detail: "Evaluation health check failed: down".to_string(),
            }]
        );
    }

    #[test]
    fn every_case_asks_at_least_one_question_and_expects_only_what_it_asks() {
        for case in cases() {
            assert!(!case.questions.is_empty(), "{} asks nothing", case.name);
            for id in case.expectations.keys() {
                assert!(
                    case.questions.contains_key(id),
                    "{} expects an answer to '{id}', which it does not ask",
                    case.name
                );
            }
        }
    }
}
