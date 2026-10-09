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

//! The invariants every evaluation answer must hold, whichever model produced it.

use std::collections::{BTreeMap, BTreeSet};

use crate::{Answer, EvaluateResponse, Question};

/// Half the step of the two-decimal rounding that responses commonly carry.
const ROUNDING_HALF_STEP: f64 = 0.005;

/// Allowance for the floating-point representation of a rounded total, so an exact
/// shortfall such as `1.0 - 0.99` is not rejected in its last bit.
const FLOAT_SLACK: f64 = 0.001;

/// The most a distribution may sum from 1, however many values it holds.
///
/// Rounding alone can move a wide distribution further than this: 200 options that
/// each sit just under half a step all round to zero. A distribution that has lost
/// that much mass to rounding is not a calibrated answer to publish, so it fails
/// with an error instead. No score rubric reaches the cap, since ten levels allow at
/// most `0.051`, so it bounds only wide choice domains.
const MAX_PROBABILITY_SUM_TOLERANCE: f64 = 0.1;

/// Checks that `response` answers every question in `asked` and nothing else, each
/// with an answer of the matching kind and values inside the domain the question
/// defined.
///
/// A response that drops, re-types, or answers outside its own options is a wrong
/// result, not a success, so it is reported as an unparseable response from `model`
/// rather than published.
///
/// # Errors
///
/// Returns [`crate::Error::UnparseableResponse`] describing the first answer that does
/// not match its question.
pub fn check_answers(
    model: &str,
    asked: &BTreeMap<String, Question>,
    response: &EvaluateResponse,
) -> crate::Result<()> {
    first_mismatch(asked, response).map_err(|detail| crate::Error::UnparseableResponse {
        model: model.to_string(),
        response: detail,
    })
}

fn first_mismatch(
    asked: &BTreeMap<String, Question>,
    response: &EvaluateResponse,
) -> Result<(), String> {
    let missing: Vec<&str> = asked
        .keys()
        .filter(|id| !response.answers.contains_key(*id))
        .map(String::as_str)
        .collect();
    if !missing.is_empty() {
        return Err(format!("no answer for question(s): {}", missing.join(", ")));
    }

    for (id, answer) in &response.answers {
        let Some(question) = asked.get(id) else {
            return Err(format!("answer for question '{id}', which was not asked"));
        };
        let (expected, got) = (question_kind(question), answer_kind(answer));
        if expected != got {
            return Err(format!(
                "question '{id}' is a {expected} question but the answer is a {got}"
            ));
        }

        match (question, answer) {
            (_, Answer::Noul { noul }) => {
                if !is_probability(*noul) {
                    return Err(format!("question '{id}': noul {noul} is outside [0, 1]"));
                }
            }
            (
                Question::Choice { criteria, .. },
                Answer::Choice {
                    choice,
                    probabilities,
                    confidence,
                },
            ) => {
                if !criteria.contains_key(choice) {
                    return Err(format!(
                        "question '{id}': answer '{choice}' is not one of its options"
                    ));
                }
                check_distribution(
                    id,
                    probabilities,
                    *confidence,
                    criteria.keys().map(String::as_str),
                )?;
                // A choice answer is the selected option. Another option with a
                // strictly higher probability contradicts that selection; an exact
                // tie among the max remains valid.
                let Some(&chosen_p) = probabilities.get(choice) else {
                    return Err(format!(
                        "question '{id}': answer '{choice}' is missing from the distribution"
                    ));
                };
                if probabilities
                    .iter()
                    .any(|(option, p)| option != choice && *p > chosen_p)
                {
                    return Err(format!(
                        "question '{id}': answer '{choice}' is not a highest-probability option"
                    ));
                }
            }
            (
                Question::Score { criteria, .. },
                Answer::Score {
                    score,
                    legend,
                    probabilities,
                    confidence,
                },
            ) => {
                let Some(top_idx) = criteria.len().checked_sub(1) else {
                    return Err(format!(
                        "question '{id}': score criteria must contain at least one level"
                    ));
                };
                #[expect(
                    clippy::cast_precision_loss,
                    reason = "score criteria are bounded at ten levels"
                )]
                let top = top_idx as f64;
                if !score.is_finite() || *score < 0.0 || *score > top {
                    return Err(format!(
                        "question '{id}': score {score} is outside [0, {top}]"
                    ));
                }
                for key in legend.keys() {
                    if key.parse::<usize>().ok().is_none_or(|idx| idx > top_idx) {
                        return Err(format!(
                            "question '{id}': legend key '{key}' is not in the score rubric [0, {top_idx}]"
                        ));
                    }
                }
                // A sparse legend is valid: every supplied key must be in range,
                // but not every rubric level needs a description. The probability
                // distribution still covers the full rubric.
                let domain: Vec<String> = (0..=top_idx).map(|i| i.to_string()).collect();
                check_distribution(
                    id,
                    probabilities,
                    *confidence,
                    domain.iter().map(String::as_str),
                )?;
                // `score` is the probability-weighted average of the rubric indices.
                // The reported probabilities pin that average to an interval, and a
                // score outside it contradicts them: a wrong result, not a
                // successful evaluation.
                let Some((lowest, highest)) = weighted_score_interval(probabilities) else {
                    return Err(format!(
                        "question '{id}': its probabilities are not a rounding of any distribution over the rubric"
                    ));
                };
                // The score may itself be rounded.
                let slack = ROUNDING_HALF_STEP + FLOAT_SLACK;
                if !(lowest - slack..=highest + slack).contains(score) {
                    return Err(format!(
                        "question '{id}': score {score} is not the probability-weighted average of its distribution, which allows [{lowest}, {highest}]"
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Whether `value` is a probability: finite and in `[0, 1]`.
#[must_use]
pub fn is_probability(value: f64) -> bool {
    value.is_finite() && (0.0..=1.0).contains(&value)
}

/// How far a reported probability sum may sit from 1, for a distribution of `n`
/// masses.
///
/// Probabilities are documented as summing to approximately 1. Each mass may be
/// independently rounded to two decimal places, so the aggregate error scales with
/// `n` (half a step per value) plus a small floating-point fudge. A constant sized for
/// two values (`0.011`) rejects a valid seven-way rounding such as `[0.15 × 6, 0.12]`
/// from `[0.146 × 6, 0.124]`. The allowance stops at a fixed cap, so a wide domain
/// cannot grow it into acceptance of any sum.
#[must_use]
pub fn probability_sum_tolerance(n: usize) -> f64 {
    #[expect(
        clippy::cast_precision_loss,
        reason = "distribution size is the question domain, not a byte count"
    )]
    let n = n as f64;
    ROUNDING_HALF_STEP
        .mul_add(n, FLOAT_SLACK)
        .min(MAX_PROBABILITY_SUM_TOLERANCE)
}

/// The range a probability-weighted score can take, given the reported
/// probabilities and the rounding each may carry, or `None` when no distribution
/// that sums to 1 could have been rounded to them.
///
/// Each reported value stands for a true value within half a step of it, and the true
/// values sum to 1. Starting every value at its lower bound and spreading the mass
/// that leaves over the lowest indices first gives the smallest average; over the
/// highest first, the largest. The range is exact for the values reported, so a
/// one-hot `{9: 1.0}` allows only `[8.955, 9.0]`, not the slack a ten-level rubric
/// could need in the worst case.
fn weighted_score_interval(probabilities: &BTreeMap<String, f64>) -> Option<(f64, f64)> {
    let mut levels = probabilities
        .iter()
        .map(|(key, &p)| {
            let index = f64::from(key.parse::<u32>().ok()?);
            let low = (p - ROUNDING_HALF_STEP).max(0.0);
            let high = (p + ROUNDING_HALF_STEP).min(1.0);
            Some((index, low, high))
        })
        .collect::<Option<Vec<_>>>()?;
    levels.sort_by(|a, b| a.0.total_cmp(&b.0));

    let spare = 1.0 - levels.iter().map(|&(_, low, _)| low).sum::<f64>();
    let room: f64 = levels.iter().map(|&(_, low, high)| high - low).sum();
    if spare < -FLOAT_SLACK || spare > room + FLOAT_SLACK {
        return None;
    }
    let base: f64 = levels.iter().map(|&(index, low, _)| index * low).sum();
    Some((
        base + spread_mass(levels.iter(), spare),
        base + spread_mass(levels.iter().rev(), spare),
    ))
}

/// Adds `spare` probability mass to `levels` in the order given, each up to its upper
/// bound, and returns how far that moves the weighted average.
fn spread_mass<'a>(levels: impl Iterator<Item = &'a (f64, f64, f64)>, spare: f64) -> f64 {
    let mut left = spare.max(0.0);
    let mut moved = 0.0;
    for &(index, low, high) in levels {
        let added = left.min(high - low);
        moved += index * added;
        left -= added;
    }
    moved
}

/// Confidence and every probability in the distribution must be a probability.
/// The distribution must cover exactly the question's domain — no missing keys,
/// no extras — and sum to 1 within [`probability_sum_tolerance`].
fn check_distribution<'a>(
    id: &str,
    probabilities: &BTreeMap<String, f64>,
    confidence: f64,
    domain: impl IntoIterator<Item = &'a str>,
) -> Result<(), String> {
    if !is_probability(confidence) {
        return Err(format!(
            "question '{id}': confidence {confidence} is outside [0, 1]"
        ));
    }
    let domain: BTreeSet<&str> = domain.into_iter().collect();
    for key in &domain {
        if !probabilities.contains_key(*key) {
            return Err(format!(
                "question '{id}': probability key '{key}' is missing from the distribution"
            ));
        }
    }
    for (key, p) in probabilities {
        if !domain.contains(key.as_str()) {
            return Err(format!(
                "question '{id}': probability key '{key}' is not in the question's domain"
            ));
        }
        if !is_probability(*p) {
            return Err(format!(
                "question '{id}': probability for '{key}' is {p}, outside [0, 1]"
            ));
        }
    }
    let sum: f64 = probabilities.values().sum();
    if (sum - 1.0).abs() > probability_sum_tolerance(probabilities.len()) {
        return Err(format!(
            "question '{id}': probabilities sum to {sum}, which is not a distribution over [0, 1]"
        ));
    }
    Ok(())
}

/// The primitive a question asks for, used to check the answer that comes back.
fn question_kind(question: &Question) -> &'static str {
    match question {
        Question::Noul { .. } => "noul",
        Question::Choice { .. } => "choice",
        Question::Score { .. } => "score",
    }
}

/// The primitive an answer carries.
fn answer_kind(answer: &Answer) -> &'static str {
    match answer {
        Answer::Noul { .. } => "noul",
        Answer::Choice { .. } => "choice",
        Answer::Score { .. } => "score",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Six masses of 0.146 and one of 0.124 sum to 1 before rounding. Independently
    /// rounded to two decimals they become `[0.15 × 6, 0.12]` (wire sum 1.02),
    /// which a two-value tolerance of 0.011 rejects.
    #[test]
    fn check_distribution_accepts_independently_rounded_seven_way_masses() {
        let probabilities = BTreeMap::from([
            ("a".to_string(), 0.15),
            ("b".to_string(), 0.15),
            ("c".to_string(), 0.15),
            ("d".to_string(), 0.15),
            ("e".to_string(), 0.15),
            ("f".to_string(), 0.15),
            ("g".to_string(), 0.12),
        ]);
        check_distribution(
            "q",
            &probabilities,
            0.9,
            ["a", "b", "c", "d", "e", "f", "g"],
        )
        .expect("independent two-decimal rounding of a unit distribution");
    }
}
