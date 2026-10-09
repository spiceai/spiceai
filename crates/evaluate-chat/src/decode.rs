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

//! Reads a model's reply into typed answers, or explains everything wrong with it.
//!
//! The reply's JSON object must match the schema from [`crate::schema`] exactly: every
//! question answered, nothing else present, and every value inside its question's
//! domain. The object may sit inside a Markdown fence or between sentences, as long as
//! it is the only one. Each problem is described in terms the model can act on,
//! because the description is what the corrective turn sends back.

use std::collections::{BTreeMap, HashSet};
use std::fmt;

use evaluate_api::{
    Answer, EntryType, NonNullEntry, Question, is_probability, probability_sum_tolerance,
};
use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};

use crate::AnswerMode;

/// The longest excerpt of an offending value quoted back in a problem description.
const MAX_QUOTED_VALUE_CHARS: usize = 120;

/// How far from 1 a sum may be and still count as exactly 1: floating-point rounding
/// of values that sum to 1, not a distribution that needs rescaling.
const NORMALIZED_SUM_SLACK: f64 = 1e-9;

/// The typed answers in `reply`, or a description of every way it fails to match the
/// questions.
pub(crate) fn parse_reply(
    reply: &str,
    questions: &BTreeMap<String, Question>,
    mode: AnswerMode,
) -> Result<BTreeMap<String, Answer>, String> {
    let json = json_object(reply)?;
    let Value::Object(mut reply) =
        serde_json::from_str(json).map_err(|e| format!("the reply is not a JSON object ({e})"))?
    else {
        return Err("the reply must be a JSON object with an 'answers' property".to_string());
    };

    let mut problems: Vec<String> = reply
        .keys()
        .filter(|key| key.as_str() != "answers")
        .map(|key| format!("unexpected top-level property '{key}'"))
        .collect();
    let answers = match reply.remove("answers") {
        Some(Value::Object(answers)) => answers,
        Some(other) => {
            problems.push(format!(
                "'answers' must be an object, got {}",
                quote(&other)
            ));
            return Err(problems.join("; "));
        }
        None => {
            problems.push("the reply has no 'answers' property".to_string());
            return Err(problems.join("; "));
        }
    };

    problems.extend(
        answers
            .keys()
            .filter(|id| !questions.contains_key(*id))
            .map(|id| format!("'answers' has a property '{id}', which is not a question")),
    );

    let mut decoded = BTreeMap::new();
    for (id, question) in questions {
        let Some(value) = answers.get(id) else {
            problems.push(format!("'answers' is missing question '{id}'"));
            continue;
        };
        match answer(question, value, mode) {
            Ok(answer) => {
                decoded.insert(id.clone(), answer);
            }
            Err(problem) => problems.push(format!("question '{id}': {problem}")),
        }
    }

    if problems.is_empty() {
        Ok(decoded)
    } else {
        Err(problems.join("; "))
    }
}

fn answer(question: &Question, value: &Value, mode: AnswerMode) -> Result<Answer, String> {
    match question {
        Question::Noul { .. } => noul(value, mode),
        Question::Choice { criteria, .. } => {
            let options: Vec<&str> = criteria.keys().map(String::as_str).collect();
            let probabilities = distribution(value, &options, mode)?;
            Ok(choice_answer(&options, &probabilities))
        }
        Question::Score { criteria, .. } => {
            let labels: Vec<String> = (0..criteria.len()).map(|level| level.to_string()).collect();
            let labels: Vec<&str> = labels.iter().map(String::as_str).collect();
            let probabilities = distribution(value, &labels, mode)?;
            Ok(score_answer(criteria, &labels, &probabilities))
        }
    }
}

fn noul(value: &Value, mode: AnswerMode) -> Result<Answer, String> {
    match mode {
        AnswerMode::Probabilities => match value.as_f64() {
            Some(p) if is_probability(p) => Ok(Answer::Noul { noul: p }),
            _ => Err(format!(
                "expected a probability between 0 and 1, got {}",
                quote(value)
            )),
        },
        AnswerMode::Discrete => match value.as_bool() {
            Some(yes) => Ok(Answer::Noul {
                noul: if yes { 1.0 } else { 0.0 },
            }),
            None => Err(format!("expected true or false, got {}", quote(value))),
        },
    }
}

/// One probability per label, in `labels` order, summing to 1.
///
/// A distribution the model reports is accepted when its sum is within rounding
/// tolerance of 1 ([`probability_sum_tolerance`], the bound applied to every
/// evaluation answer) and, unless it already sums to 1, is rescaled to, so a score
/// computed from it is the weighted average of a true distribution. A sum further from
/// 1 is a malformed answer, not something to renormalize silently.
fn distribution(value: &Value, labels: &[&str], mode: AnswerMode) -> Result<Vec<f64>, String> {
    match mode {
        AnswerMode::Probabilities => {
            let Value::Object(reported) = value else {
                return Err(format!(
                    "expected an object mapping each of {} to its probability, got {}",
                    list(labels),
                    quote(value)
                ));
            };
            let probabilities = reported_distribution(reported, labels)?;
            let sum: f64 = probabilities.iter().sum();
            if (sum - 1.0).abs() > probability_sum_tolerance(labels.len()) {
                return Err(format!(
                    "the probabilities sum to {sum:.3}, but they must sum to 1"
                ));
            }
            // A sum that is 1 but for floating-point rounding is left alone: rescaling it
            // would only perturb the values the model reported.
            if (sum - 1.0).abs() <= NORMALIZED_SUM_SLACK {
                return Ok(probabilities);
            }
            Ok(probabilities.iter().map(|p| p / sum).collect())
        }
        AnswerMode::Discrete => {
            let selected = match value {
                Value::String(label) => labels.iter().position(|l| l == label),
                Value::Number(level) => whole_number(level)
                    .map(|level| level.to_string())
                    .and_then(|level| labels.iter().position(|l| *l == level)),
                _ => None,
            };
            let Some(selected) = selected else {
                return Err(format!(
                    "expected exactly one of {}, got {}",
                    list(labels),
                    quote(value)
                ));
            };
            Ok((0..labels.len())
                .map(|i| if i == selected { 1.0 } else { 0.0 })
                .collect())
        }
    }
}

fn reported_distribution(
    reported: &Map<String, Value>,
    labels: &[&str],
) -> Result<Vec<f64>, String> {
    let mut problems: Vec<String> = reported
        .keys()
        .filter(|key| !labels.contains(&key.as_str()))
        .map(|key| format!("'{key}' is not an allowed label"))
        .collect();
    let mut probabilities = Vec::with_capacity(labels.len());
    for label in labels {
        match reported.get(*label) {
            None => problems.push(format!("the probability for '{label}' is missing")),
            Some(value) => match value.as_f64() {
                Some(p) if is_probability(p) => probabilities.push(p),
                _ => problems.push(format!(
                    "the probability for '{label}' must be a number between 0 and 1, got {}",
                    quote(value)
                )),
            },
        }
    }
    if problems.is_empty() {
        Ok(probabilities)
    } else {
        Err(problems.join(", "))
    }
}

/// The most probable option, with the full distribution.
///
/// On a tie the first tied option in label order wins.
fn choice_answer(options: &[&str], probabilities: &[f64]) -> Answer {
    let chosen = argmax(probabilities);
    Answer::Choice {
        choice: options
            .get(chosen)
            .map(ToString::to_string)
            .unwrap_or_default(),
        probabilities: options
            .iter()
            .zip(probabilities)
            .map(|(option, p)| ((*option).to_string(), *p))
            .collect(),
        confidence: choice_confidence(probabilities),
    }
}

/// The probability-weighted average level, with the full distribution and the rubric
/// as its legend.
fn score_answer(levels: &[NonNullEntry], labels: &[&str], probabilities: &[f64]) -> Answer {
    let score: f64 = probabilities
        .iter()
        .enumerate()
        .map(|(level, p)| as_f64(level) * p)
        .sum();
    Answer::Score {
        // The exact average of a distribution lies inside the rubric; rounding in the sum
        // can land it a hair outside, on a level no reply chose.
        score: score.clamp(0.0, as_f64(levels.len().saturating_sub(1))),
        legend: labels
            .iter()
            .zip(levels)
            .map(|(label, criterion)| ((*label).to_string(), EntryType::from(criterion)))
            .collect(),
        probabilities: labels
            .iter()
            .zip(probabilities)
            .map(|(label, p)| ((*label).to_string(), *p))
            .collect(),
        confidence: score_confidence(probabilities),
    }
}

/// Where the peak probability sits between uniform (0) and certainty (1).
///
/// `probabilities` sum to 1, so no rescaling is needed; the clamp absorbs the rounding
/// in a sum that is 1 only to within [`NORMALIZED_SUM_SLACK`].
pub(crate) fn choice_confidence(probabilities: &[f64]) -> f64 {
    if probabilities.len() <= 1 {
        return 1.0;
    }
    let uniform = 1.0 / as_f64(probabilities.len());
    let peak = probabilities.iter().copied().fold(0.0, f64::max);
    ((peak - uniform) / (1.0 - uniform)).clamp(0.0, 1.0)
}

/// How tightly the distribution concentrates around its most probable level: 1 when
/// all mass is on it, 0 when it is as spread as a uniform distribution or wider.
pub(crate) fn score_confidence(probabilities: &[f64]) -> f64 {
    if probabilities.len() <= 1 {
        return 1.0;
    }
    let mode = as_f64(argmax(probabilities));
    let distance_from_mode: f64 = probabilities
        .iter()
        .enumerate()
        .map(|(level, p)| p * (as_f64(level) - mode).abs())
        .sum();
    let levels = as_f64(probabilities.len());
    let center = (levels - 1.0) / 2.0;
    let uniform_spread = (0..probabilities.len())
        .map(|level| (as_f64(level) - center).abs())
        .sum::<f64>()
        / levels;
    (1.0 - distance_from_mode / uniform_spread).clamp(0.0, 1.0)
}

/// A count of options or rubric levels, or an index into one, as a float.
#[expect(
    clippy::cast_precision_loss,
    reason = "option and level counts are far below where `f64` stops representing integers"
)]
fn as_f64(n: usize) -> f64 {
    n as f64
}

/// The index of the first largest value.
fn argmax(values: &[f64]) -> usize {
    let mut best = 0;
    for (i, value) in values.iter().enumerate() {
        if values.get(best).is_some_and(|b| value > b) {
            best = i;
        }
    }
    best
}

/// The one JSON object in `reply`, which may be wrapped in a Markdown fence or sit
/// between sentences.
///
/// A reply holding a second JSON object is ambiguous — a draft and its revision, say —
/// so it is rejected rather than read as either. A `{` that does not begin an object,
/// such as `{braces}` in leading or trailing prose, is skipped so a later object can
/// still be read. An unclosed `{` is not skipped: finishing it would be a guess. An
/// object that repeats a key is also rejected: parsing it into a `Value` would keep
/// only the last of two answers to one question.
fn json_object(reply: &str) -> Result<&str, String> {
    let mut remaining = reply;
    let mut last_syntax_error: Option<String> = None;
    while let Some(start) = remaining.find('{') {
        let from_object = remaining.get(start..).unwrap_or_default();
        let mut values =
            serde_json::Deserializer::from_str(from_object).into_iter::<NoRepeatedKeys>();
        match values.next() {
            Some(Ok(NoRepeatedKeys)) => {
                let (object, rest) = from_object.split_at(values.byte_offset());
                if contains_json_object(rest) {
                    return Err(
                        "the reply contains more than one JSON object; return exactly one"
                            .to_string(),
                    );
                }
                return Ok(object);
            }
            // A repeated key is well-formed JSON with a meaning problem, which the
            // error describes itself.
            Some(Err(e)) if e.is_data() => return Err(e.to_string()),
            // An object that starts and never closes is that object, unfinished —
            // not a reason to look for another `{`.
            Some(Err(e)) if e.is_eof() => {
                return Err(format!("the reply is not a JSON object ({e})"));
            }
            Some(Err(e)) => {
                last_syntax_error = Some(format!("the reply is not a JSON object ({e})"));
                remaining = from_object.get(1..).unwrap_or_default();
            }
            None => {
                remaining = from_object.get(1..).unwrap_or_default();
            }
        }
    }
    Err(last_syntax_error.unwrap_or_else(|| "the reply contains no JSON object".to_string()))
}

/// True when `text` holds a `{` that begins a JSON object.
fn contains_json_object(text: &str) -> bool {
    let mut remaining = text;
    while let Some(start) = remaining.find('{') {
        let from_brace = remaining.get(start..).unwrap_or_default();
        let mut values = serde_json::Deserializer::from_str(from_brace).into_iter::<Value>();
        if matches!(values.next(), Some(Ok(Value::Object(_)))) {
            return true;
        }
        remaining = from_brace.get(1..).unwrap_or_default();
    }
    false
}

/// A JSON number that is a non-negative whole number, however it is written: `2`,
/// `2.0` and `2e0` are all level 2.
fn whole_number(number: &Number) -> Option<u64> {
    if let Some(whole) = number.as_u64() {
        return Some(whole);
    }
    let n = number
        .as_f64()
        .filter(|n| n.fract() == 0.0 && (0.0..=f64::from(u32::MAX)).contains(n))?;
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "a whole number in [0, u32::MAX] converts exactly"
    )]
    let whole = n as u64;
    Some(whole)
}

/// A JSON value that fails to deserialize when any object in it repeats a key.
struct NoRepeatedKeys;

impl<'de> Deserialize<'de> for NoRepeatedKeys {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(NoRepeatedKeysVisitor)
    }
}

struct NoRepeatedKeysVisitor;

impl<'de> Visitor<'de> for NoRepeatedKeysVisitor {
    type Value = NoRepeatedKeys;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON value")
    }

    fn visit_bool<E: de::Error>(self, _: bool) -> Result<Self::Value, E> {
        Ok(NoRepeatedKeys)
    }

    fn visit_i64<E: de::Error>(self, _: i64) -> Result<Self::Value, E> {
        Ok(NoRepeatedKeys)
    }

    fn visit_u64<E: de::Error>(self, _: u64) -> Result<Self::Value, E> {
        Ok(NoRepeatedKeys)
    }

    fn visit_f64<E: de::Error>(self, _: f64) -> Result<Self::Value, E> {
        Ok(NoRepeatedKeys)
    }

    fn visit_str<E: de::Error>(self, _: &str) -> Result<Self::Value, E> {
        Ok(NoRepeatedKeys)
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(NoRepeatedKeys)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut items: A) -> Result<Self::Value, A::Error> {
        while items.next_element::<NoRepeatedKeys>()?.is_some() {}
        Ok(NoRepeatedKeys)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut entries: A) -> Result<Self::Value, A::Error> {
        let mut seen = HashSet::new();
        while let Some(key) = entries.next_key::<String>()? {
            if seen.contains(&key) {
                return Err(de::Error::custom(format!(
                    "the reply gives '{key}' more than once"
                )));
            }
            entries.next_value::<NoRepeatedKeys>()?;
            seen.insert(key);
        }
        Ok(NoRepeatedKeys)
    }
}

fn list(labels: &[&str]) -> String {
    let quoted: Vec<String> = labels.iter().map(|label| format!("'{label}'")).collect();
    format!("[{}]", quoted.join(", "))
}

/// `value` as compact JSON, cut short when long.
fn quote(value: &Value) -> String {
    crate::truncated(&value.to_string(), MAX_QUOTED_VALUE_CHARS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::tests::sample_questions;
    use serde_json::json;

    fn close(actual: f64, expected: f64) -> bool {
        (actual - expected).abs() < 1e-12
    }

    /// Reference values computed with `system_one_adapter._utils.confidence_metrics`
    /// (`choice_confidence`, `score_confidence`) and the adapter's expected-value score.
    #[test]
    fn confidence_and_score_match_the_python_adapter() {
        for (probabilities, expected) in [
            (vec![0.7, 0.2, 0.1], 0.55),
            (vec![0.5, 0.5], 0.0),
            (vec![1.0, 0.0], 1.0),
            (vec![0.25, 0.25, 0.25, 0.25], 0.0),
            (vec![0.0, 0.0], 0.0),
        ] {
            let actual = choice_confidence(&probabilities);
            assert!(
                close(actual, expected),
                "choice {probabilities:?}: {actual}"
            );
        }

        let levels: Vec<NonNullEntry> = ["a", "b", "c", "d", "e"]
            .into_iter()
            .map(NonNullEntry::from)
            .collect();
        for (probabilities, confidence, score) in [
            (vec![0.1, 0.2, 0.4, 0.2, 0.1], 1.0 / 3.0, 2.0),
            (vec![0.0, 0.0, 0.0, 0.0, 1.0], 1.0, 4.0),
            (vec![0.5, 0.0, 0.0, 0.0, 0.5], 0.0, 2.0),
            (vec![0.2, 0.2, 0.2, 0.2, 0.2], 0.0, 2.0),
            (vec![0.05, 0.05, 0.1, 0.3, 0.5], 7.0 / 24.0, 3.15),
        ] {
            let Answer::Score {
                score: actual_score,
                confidence: actual_confidence,
                ..
            } = score_answer(&levels, &["0", "1", "2", "3", "4"], &probabilities)
            else {
                panic!("expected a score");
            };
            assert!(
                close(actual_confidence, confidence),
                "score confidence {probabilities:?}: {actual_confidence}"
            );
            assert!(
                close(actual_score, score),
                "score {probabilities:?}: {actual_score}"
            );
        }
        assert!(close(score_confidence(&[0.6, 0.4]), 0.2));
    }

    #[test]
    fn decodes_a_probability_reply() {
        let reply = json!({"answers": {
            "is_urgent": 0.8,
            "team": {"billing": 0.7, "sales": 0.1, "technical": 0.2},
            "tone": {"0": 0.1, "1": 0.3, "2": 0.6}
        }})
        .to_string();
        let answers = parse_reply(&reply, &sample_questions(), AnswerMode::Probabilities)
            .expect("valid reply");

        assert_eq!(answers.get("is_urgent"), Some(&Answer::Noul { noul: 0.8 }));
        let Some(Answer::Choice {
            choice,
            probabilities,
            confidence,
        }) = answers.get("team")
        else {
            panic!("team: {answers:?}");
        };
        assert_eq!(choice, "billing");
        assert_eq!(probabilities.len(), 3);
        assert!(close(*confidence, 0.55), "{confidence}");
        let Some(Answer::Score { score, legend, .. }) = answers.get("tone") else {
            panic!("tone: {answers:?}");
        };
        assert!(close(*score, 1.5), "{score}");
        assert_eq!(legend.get("2"), Some(&EntryType::String("furious".into())));
    }

    #[test]
    fn decodes_a_discrete_reply_into_one_hot_answers() {
        let reply =
            json!({"answers": {"is_urgent": false, "team": "technical", "tone": 2}}).to_string();
        let answers =
            parse_reply(&reply, &sample_questions(), AnswerMode::Discrete).expect("valid reply");
        assert_eq!(answers.get("is_urgent"), Some(&Answer::Noul { noul: 0.0 }));
        let Some(Answer::Choice {
            choice, confidence, ..
        }) = answers.get("team")
        else {
            panic!("team: {answers:?}");
        };
        assert_eq!((choice.as_str(), *confidence), ("technical", 1.0));
        let Some(Answer::Score {
            score, confidence, ..
        }) = answers.get("tone")
        else {
            panic!("tone: {answers:?}");
        };
        assert_eq!((*score, *confidence), (2.0, 1.0));
    }

    #[test]
    fn a_distribution_within_rounding_of_one_is_rescaled_to_one() {
        let reply = json!({"answers": {
            "is_urgent": 1,
            "team": {"billing": 0.33, "sales": 0.33, "technical": 0.33},
            "tone": {"0": 0.0, "1": 0.0, "2": 1.0}
        }})
        .to_string();
        let answers = parse_reply(&reply, &sample_questions(), AnswerMode::Probabilities)
            .expect("0.99 is within rounding of 1 for three options");
        let Some(Answer::Choice {
            choice,
            probabilities,
            ..
        }) = answers.get("team")
        else {
            panic!("team: {answers:?}");
        };
        let sum: f64 = probabilities.values().sum();
        assert!(close(sum, 1.0), "{sum}");
        // An exact tie resolves to the first option in label order.
        assert_eq!(choice, "billing");
        assert_eq!(answers.get("is_urgent"), Some(&Answer::Noul { noul: 1.0 }));
    }

    #[test]
    fn reports_every_problem_in_one_pass() {
        let reply = json!({
            "answers": {
                "is_urgent": 1.5,
                "team": {"billing": 0.9, "technical": 0.3, "refunds": 0.1},
                "extra": 1
            },
            "reasoning": "because"
        })
        .to_string();
        let problem = parse_reply(&reply, &sample_questions(), AnswerMode::Probabilities)
            .expect_err("invalid reply");
        for expected in [
            "unexpected top-level property 'reasoning'",
            "'answers' has a property 'extra', which is not a question",
            "question 'is_urgent': expected a probability between 0 and 1, got 1.5",
            "'refunds' is not an allowed label",
            "the probability for 'sales' is missing",
            "'answers' is missing question 'tone'",
        ] {
            assert!(
                problem.contains(expected),
                "missing {expected:?} in: {problem}"
            );
        }
    }

    #[test]
    fn a_distribution_that_does_not_sum_to_one_is_rejected() {
        let reply = json!({"answers": {
            "is_urgent": 0.5,
            "team": {"billing": 0.9, "sales": 0.2, "technical": 0.1},
            "tone": {"0": 0.2, "1": 0.2, "2": 0.2}
        }})
        .to_string();
        let problem = parse_reply(&reply, &sample_questions(), AnswerMode::Probabilities)
            .expect_err("sums of 1.2 and 0.6 are not distributions");
        assert!(
            problem.contains("question 'team': the probabilities sum to 1.200,"),
            "{problem}"
        );
        assert!(
            problem.contains("question 'tone': the probabilities sum to 0.600,"),
            "{problem}"
        );
    }

    #[test]
    fn discrete_values_outside_the_domain_are_rejected() {
        let reply =
            json!({"answers": {"is_urgent": "yes", "team": "refunds", "tone": 3}}).to_string();
        let problem = parse_reply(&reply, &sample_questions(), AnswerMode::Discrete)
            .expect_err("invalid reply");
        for expected in [
            "question 'is_urgent': expected true or false, got \"yes\"",
            "question 'team': expected exactly one of ['billing', 'sales', 'technical'], got \"refunds\"",
            "question 'tone': expected exactly one of ['0', '1', '2'], got 3",
        ] {
            assert!(
                problem.contains(expected),
                "missing {expected:?} in: {problem}"
            );
        }
    }

    #[test]
    fn a_reply_that_is_not_an_answers_object_is_rejected() {
        let questions = sample_questions();
        let not_json = parse_reply("Sure! Here you go.", &questions, AnswerMode::Probabilities)
            .expect_err("prose");
        assert_eq!(not_json, "the reply contains no JSON object");
        let array =
            parse_reply("[1, 2]", &questions, AnswerMode::Probabilities).expect_err("array");
        assert_eq!(array, "the reply contains no JSON object");
        let bare = parse_reply(
            "{\"is_urgent\": 0.5}",
            &questions,
            AnswerMode::Probabilities,
        )
        .expect_err("no answers wrapper");
        assert!(
            bare.contains("the reply has no 'answers' property"),
            "{bare}"
        );
    }

    #[test]
    fn long_values_are_quoted_in_part() {
        let long = Value::String("x".repeat(500));
        let quoted = quote(&long);
        assert_eq!(quoted.chars().count(), MAX_QUOTED_VALUE_CHARS + 1);
        assert!(quoted.ends_with('…'));
    }

    /// A small model's reply from an end-to-end run: a correct answer in a fence,
    /// followed by an explanation.
    #[test]
    fn an_answer_followed_by_an_explanation_is_read() {
        let reply = "```json\n{\n  \"answers\": {\n    \"is_urgent\": 0.2,\n    \"team\": {\n      \"billing\": 0.1,\n      \"sales\": 0.6,\n      \"technical\": 0.3\n    },\n    \"tone\": {\n      \"0\": 0.7,\n      \"1\": 0.2,\n      \"2\": 0.1\n    }\n  }\n}\n```\n\nThis JSON object adheres to the provided schema, with each property reflecting the probability of the corresponding answer.";
        let answers = parse_reply(reply, &sample_questions(), AnswerMode::Probabilities)
            .expect("the object is the answer");
        let Some(Answer::Choice { choice, .. }) = answers.get("team") else {
            panic!("team: {answers:?}");
        };
        assert_eq!(choice, "sales");

        let introduced = format!(
            "Here are the answers: {} Let me know if you need more.",
            json!({"answers": {
                "is_urgent": 0.5,
                "team": {"billing": 1.0, "sales": 0.0, "technical": 0.0},
                "tone": {"0": 1.0, "1": 0.0, "2": 0.0}
            }})
        );
        parse_reply(&introduced, &sample_questions(), AnswerMode::Probabilities)
            .expect("prose around a single object");
    }

    /// Probabilities that already sum to 1 are reported exactly as the model gave them;
    /// rescaling by a sum of `0.9999999999999999` would turn `0.7` into
    /// `0.7000000000000001`.
    #[test]
    fn a_distribution_that_sums_to_one_is_reported_unchanged() {
        let reply = json!({"answers": {
            "is_urgent": 0.2,
            "team": {"billing": 0.1, "sales": 0.6, "technical": 0.3},
            "tone": {"0": 0.7, "1": 0.2, "2": 0.1}
        }})
        .to_string();
        let answers = parse_reply(&reply, &sample_questions(), AnswerMode::Probabilities)
            .expect("valid reply");
        let Some(Answer::Score { probabilities, .. }) = answers.get("tone") else {
            panic!("tone: {answers:?}");
        };
        assert_eq!(
            probabilities,
            &BTreeMap::from([
                ("0".to_string(), 0.7),
                ("1".to_string(), 0.2),
                ("2".to_string(), 0.1)
            ])
        );
    }

    /// Two objects are a draft and a revision, or two different answers: neither is
    /// taken. A `{` that does not begin an object does not hide a later one.
    #[test]
    fn a_reply_with_two_objects_is_rejected() {
        let answer = json!({"answers": {
            "is_urgent": 0.5,
            "team": {"billing": 1.0, "sales": 0.0, "technical": 0.0},
            "tone": {"0": 1.0, "1": 0.0, "2": 0.0}
        }});
        let reply = format!("Draft: {answer}\nFinal: {answer}");
        let problem = parse_reply(&reply, &sample_questions(), AnswerMode::Probabilities)
            .expect_err("two objects");
        assert_eq!(
            problem,
            "the reply contains more than one JSON object; return exactly one"
        );

        let with_prose_braces = format!("Draft: {answer} use {{braces}} Final: {answer}");
        let problem = parse_reply(
            &with_prose_braces,
            &sample_questions(),
            AnswerMode::Probabilities,
        )
        .expect_err("two objects with braces in the prose between them");
        assert_eq!(
            problem,
            "the reply contains more than one JSON object; return exactly one"
        );
    }

    /// A `{` in prose is not an object unless it begins one, before or after the
    /// answer.
    #[test]
    fn a_brace_in_trailing_prose_is_not_a_second_object() {
        let questions: BTreeMap<String, Question> = serde_json::from_value(json!({
            "q": {"type": "noul", "instructions": "yes or no"}
        }))
        .expect("question");
        let trailing = r#"{"answers":{"q":0.5}} Explanation: use {braces} literally."#;
        let answers = parse_reply(trailing, &questions, AnswerMode::Probabilities)
            .expect("a brace in the explanation is not a second object");
        assert_eq!(answers.get("q"), Some(&Answer::Noul { noul: 0.5 }));

        let leading = r#"Note: use {braces} literally. Final: {"answers":{"q":0.5}}"#;
        let answers = parse_reply(leading, &questions, AnswerMode::Probabilities)
            .expect("a brace in the leading prose is not the answer object");
        assert_eq!(answers.get("q"), Some(&Answer::Noul { noul: 0.5 }));
    }

    /// A small model's reply from an end-to-end run: the outer object is never closed.
    /// Closing it would be a guess at what the model meant.
    #[test]
    fn an_unclosed_object_is_rejected() {
        let reply = r#"{"answers":{"is_urgent":1.0,"team":{"sales":1.0,"billing":0.0,"technical":0.0},"tone":{}}"#;
        let problem = parse_reply(reply, &sample_questions(), AnswerMode::Probabilities)
            .expect_err("unclosed object");
        assert!(
            problem.starts_with("the reply is not a JSON object (EOF while parsing an object"),
            "{problem}"
        );
    }

    /// Parses `reply` and holds the answers to the invariants every evaluation must meet.
    fn parse_and_check(
        reply: &str,
        questions: &BTreeMap<String, Question>,
        mode: AnswerMode,
    ) -> Result<BTreeMap<String, Answer>, String> {
        let answers = parse_reply(reply, questions, mode)?;
        let response = evaluate_api::EvaluateResponse {
            model: "judge".to_string(),
            answers,
            usage: None,
        };
        evaluate_api::check_answers("judge", questions, &response).map_err(|e| e.to_string())?;
        Ok(response.answers)
    }

    /// Regression: twenty equal masses of 0.05 came out with a confidence a hair below
    /// zero, which `check_answers` rejects, turning a valid reply into a failed
    /// evaluation.
    #[test]
    fn a_uniform_choice_has_zero_confidence() {
        let options: Vec<String> = (0..20).map(|i| format!("option_{i:02}")).collect();
        let questions: BTreeMap<String, Question> = serde_json::from_value(json!({
            "q": {"type": "choice", "criteria": options.iter().map(|o| (o.clone(), Value::Null)).collect::<Map<_, _>>()}
        }))
        .expect("question");
        let reply = json!({"answers": {"q": options.iter().map(|o| (o.clone(), json!(0.05))).collect::<Map<_, _>>()}});

        let answers = parse_and_check(&reply.to_string(), &questions, AnswerMode::Probabilities)
            .expect("a uniform distribution is a valid answer");

        let Some(Answer::Choice { confidence, .. }) = answers.get("q") else {
            panic!("q: {answers:?}");
        };
        assert!(close(*confidence, 0.0), "{confidence}");
    }

    /// Regression: a distribution that rescales by a hair put the weighted average a
    /// hair above the top level (`6.000000000000001` on a 0–6 rubric), which
    /// `check_answers` rejects.
    #[test]
    fn a_score_at_the_top_level_stays_inside_the_rubric() {
        let questions: BTreeMap<String, Question> = serde_json::from_value(json!({
            "q": {"type": "score", "criteria": ["0", "1", "2", "3", "4", "5", "6"]}
        }))
        .expect("question");
        let reply =
            r#"{"answers": {"q": {"0": 0, "1": 0, "2": 0, "3": 0, "4": 0, "5": 1e-16, "6": 1}}}"#;

        let answers = parse_and_check(reply, &questions, AnswerMode::Probabilities)
            .expect("all mass on the top level is a valid answer");

        let Some(Answer::Score { score, .. }) = answers.get("q") else {
            panic!("q: {answers:?}");
        };
        assert!(*score <= 6.0 && close(*score, 6.0), "{score}");
    }

    /// A reply that answers a question twice, or gives an option two probabilities, is
    /// contradictory; keeping whichever value came last would publish a guess.
    #[test]
    fn a_reply_with_duplicate_keys_is_rejected() {
        let questions = sample_questions();
        let answered_twice = r#"{"answers": {"is_urgent": 0.0, "is_urgent": 1.0, "team": {"billing": 1, "sales": 0, "technical": 0}, "tone": {"0": 1, "1": 0, "2": 0}}}"#;
        let problem = parse_reply(answered_twice, &questions, AnswerMode::Probabilities)
            .expect_err("a question answered twice");
        assert!(problem.contains("'is_urgent' more than once"), "{problem}");

        let option_twice = r#"{"answers": {"is_urgent": 0.5, "team": {"billing": 0.2, "billing": 1, "sales": 0, "technical": 0}, "tone": {"0": 1, "1": 0, "2": 0}}}"#;
        let problem = parse_reply(option_twice, &questions, AnswerMode::Probabilities)
            .expect_err("an option given two probabilities");
        assert!(problem.contains("'billing' more than once"), "{problem}");
    }

    /// A level written as a whole-valued float (`2.0`, `2e0`) is that level.
    #[test]
    fn a_discrete_level_written_as_a_whole_float_is_that_level() {
        for tone in ["2.0", "2e0"] {
            let reply =
                format!(r#"{{"answers": {{"is_urgent": true, "team": "sales", "tone": {tone}}}}}"#);
            let answers = parse_reply(&reply, &sample_questions(), AnswerMode::Discrete)
                .unwrap_or_else(|problem| panic!("{tone}: {problem}"));
            let Some(Answer::Score { score, .. }) = answers.get("tone") else {
                panic!("tone: {answers:?}");
            };
            assert!(close(*score, 2.0), "{tone}: {score}");
        }
        let problem = parse_reply(
            r#"{"answers": {"is_urgent": true, "team": "sales", "tone": 1.5}}"#,
            &sample_questions(),
            AnswerMode::Discrete,
        )
        .expect_err("1.5 is not a level");
        assert!(problem.contains("got 1.5"), "{problem}");
    }
}
