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
//! Questions and answers, as upstream's `schema.py` defines them, and the confidence each
//! answer reports.

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ensure;

use crate::{InvalidQuestionSnafu, Result, as_f64};

/// The most options a choice question may list.
pub const MAX_CHOICE_OPTIONS: usize = 255;
/// The fewest levels a score question may list.
pub const MIN_SCORE_LEVELS: usize = 2;
/// The most levels a score question may list.
pub const MAX_SCORE_LEVELS: usize = 10;

/// The three question primitives. Each has its own calibrated temperature.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    /// Yes or no: the answer is P(true).
    Noul,
    /// One of N named options.
    Choice,
    /// A level on an ordered scale: the answer is the expected level.
    Score,
}

impl Kind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Noul => "noul",
            Self::Choice => "choice",
            Self::Score => "score",
        }
    }
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A state, instruction or option description: text, or structured data that renders as
/// indented JSON.
#[derive(Debug, Clone, PartialEq)]
pub enum Content {
    Text(String),
    /// A JSON object or array.
    Structured(Value),
}

impl Content {
    /// The content a JSON value carries: `None` for null; text for a string; structured for
    /// an object or an array. Upstream accepts no other value.
    pub fn from_json(value: Value) -> Result<Option<Self>> {
        match value {
            Value::Null => Ok(None),
            Value::String(text) => Ok(Some(Self::Text(text))),
            value @ (Value::Object(_) | Value::Array(_)) => Ok(Some(Self::Structured(value))),
            Value::Bool(_) | Value::Number(_) => InvalidQuestionSnafu {
                message: "content must be a string, an object, an array or null",
            }
            .fail(),
        }
    }
}

impl From<&str> for Content {
    fn from(text: &str) -> Self {
        Self::Text(text.to_string())
    }
}

impl From<String> for Content {
    fn from(text: String) -> Self {
        Self::Text(text)
    }
}

/// What one side of a noul question means.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum Meaning {
    /// Upstream's default text for that side.
    #[default]
    Default,
    /// Given as null: the option is listed with no description.
    Blank,
    Given(Content),
}

/// Optional descriptions of the two sides of a noul question.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct NoulCriteria {
    pub true_meaning: Meaning,
    pub false_meaning: Meaning,
}

/// A choice question's options, in the order the prompt lists them: 2 to
/// [`MAX_CHOICE_OPTIONS`] names, none repeated, each with an optional description.
#[derive(Debug, Clone, PartialEq)]
pub struct ChoiceOptions(Vec<(String, Option<Content>)>);

impl ChoiceOptions {
    pub fn new(options: Vec<(String, Option<Content>)>) -> Result<Self> {
        ensure!(
            options.len() >= 2,
            InvalidQuestionSnafu {
                message: "choice requires at least 2 options",
            }
        );
        ensure!(
            options.len() <= MAX_CHOICE_OPTIONS,
            InvalidQuestionSnafu {
                message: format!("choice allows at most {MAX_CHOICE_OPTIONS} options"),
            }
        );
        for (i, (name, _)) in options.iter().enumerate() {
            ensure!(
                !options[..i].iter().any(|(other, _)| other == name),
                InvalidQuestionSnafu {
                    message: format!("choice lists the option '{name}' twice"),
                }
            );
        }
        Ok(Self(options))
    }

    #[must_use]
    pub fn as_slice(&self) -> &[(String, Option<Content>)] {
        &self.0
    }
}

/// A score question's levels, lowest first: [`MIN_SCORE_LEVELS`] to [`MAX_SCORE_LEVELS`]
/// rubric texts.
#[derive(Debug, Clone, PartialEq)]
pub struct ScoreLevels(Vec<String>);

impl ScoreLevels {
    pub fn new(levels: Vec<String>) -> Result<Self> {
        ensure!(
            (MIN_SCORE_LEVELS..=MAX_SCORE_LEVELS).contains(&levels.len()),
            InvalidQuestionSnafu {
                message: format!(
                    "score requires between {MIN_SCORE_LEVELS} and {MAX_SCORE_LEVELS} levels"
                ),
            }
        );
        Ok(Self(levels))
    }

    #[must_use]
    pub fn as_slice(&self) -> &[String] {
        &self.0
    }
}

/// A typed question about a state.
#[derive(Debug, Clone, PartialEq)]
pub enum Question {
    Noul {
        instructions: Content,
        criteria: NoulCriteria,
    },
    Choice {
        instructions: Content,
        options: ChoiceOptions,
    },
    Score {
        instructions: Content,
        levels: ScoreLevels,
    },
}

impl Question {
    #[must_use]
    pub fn kind(&self) -> Kind {
        match self {
            Self::Noul { .. } => Kind::Noul,
            Self::Choice { .. } => Kind::Choice,
            Self::Score { .. } => Kind::Score,
        }
    }
}

/// An answer, its probabilities rounded to 4 places as upstream rounds them.
#[derive(Debug, Clone, PartialEq)]
pub enum Answer {
    /// P(true).
    Noul { noul: f64 },
    Choice {
        /// The most probable option; the first listed on a tie.
        choice: String,
        /// Every option's probability, in the order the question listed them.
        probabilities: Vec<(String, f64)>,
        confidence: f64,
    },
    Score {
        /// The expected level index.
        score: f64,
        /// Each level index, lowest first, with its rubric text.
        legend: Vec<(String, String)>,
        /// Each level index, lowest first, with its probability.
        probabilities: Vec<(String, f64)>,
        confidence: f64,
    },
}

/// Normalised max-probability: `(N * p_max - 1) / (N - 1)`, clamped to [0, 1]. Uniform is 0,
/// one-hot is 1, whatever N is; one option is 1 by definition.
#[must_use]
pub fn derive_confidence(probabilities: &[f64]) -> f64 {
    let n = probabilities.len();
    if n <= 1 {
        return 1.0;
    }
    let n = as_f64(n);
    let p_max = probabilities
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);
    ((n * p_max - 1.0) / (n - 1.0)).clamp(0.0, 1.0)
}

/// Confidence for an ordinal answer: how tightly the mass clusters on the scale, from its
/// standard deviation against the largest possible, `(L - 1) / 2`. `ordinal_smoothing` is
/// the mass training put on neighbouring levels; its square root is the spread of a perfect
/// fit, which counts as full confidence.
#[must_use]
pub fn derive_score_confidence(probabilities: &[f64], ordinal_smoothing: f64) -> f64 {
    let n = probabilities.len();
    if n <= 1 {
        return 1.0;
    }
    let total: f64 = probabilities.iter().sum();
    if total <= 0.0 {
        return 0.0;
    }
    let p: Vec<f64> = probabilities.iter().map(|x| x / total).collect();
    let mean: f64 = p.iter().enumerate().map(|(i, pi)| as_f64(i) * pi).sum();
    let var: f64 = p
        .iter()
        .enumerate()
        .map(|(i, pi)| pi * (as_f64(i) - mean).powi(2))
        .sum();
    let sigma = var.sqrt();
    let sigma_max = as_f64(n - 1) / 2.0;
    let sigma_floor = if ordinal_smoothing > 0.0 {
        ordinal_smoothing.sqrt()
    } else {
        0.0
    };
    // A floor at or past the ceiling (few levels, much smoothing) leaves no range to scale.
    if sigma_max <= sigma_floor {
        return if sigma <= sigma_floor { 1.0 } else { 0.0 };
    }
    ((sigma_max - sigma) / (sigma_max - sigma_floor)).clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-12
    }

    #[test]
    fn confidence_spans_uniform_to_one_hot() {
        assert!(close(derive_confidence(&[1.0 / 3.0; 3]), 0.0));
        assert!(close(derive_confidence(&[0.0, 1.0, 0.0]), 1.0));
        assert!(close(derive_confidence(&[1.0]), 1.0));
        // (3 * 0.5 - 1) / 2
        assert!(close(derive_confidence(&[0.5, 0.3, 0.2]), 0.25));
    }

    #[test]
    fn score_confidence_corrects_for_smoothing() {
        // One-hot on the middle of three levels: sigma 0, full confidence.
        assert!(close(derive_score_confidence(&[0.0, 1.0, 0.0], 0.1), 1.0));
        // Mass split at the extremes: sigma = sigma_max, none.
        assert!(close(derive_score_confidence(&[0.5, 0.0, 0.5], 0.0), 0.0));
        // [0.25, 0.5, 0.25]: sigma = sqrt(0.5); (1 - sqrt(0.5)) / (1 - sqrt(0.1)).
        let expected = (1.0 - 0.5f64.sqrt()) / (1.0 - 0.1f64.sqrt());
        assert!(close(
            derive_score_confidence(&[0.25, 0.5, 0.25], 0.1),
            expected
        ));
        // Unnormalised input is normalised first.
        assert!(close(
            derive_score_confidence(&[1.0, 2.0, 1.0], 0.1),
            expected
        ));
        assert!(close(derive_score_confidence(&[0.0, 0.0], 0.0), 0.0));
    }

    #[test]
    fn invalid_questions_are_refused() {
        assert!(matches!(
            ChoiceOptions::new(vec![("a".into(), None)]),
            Err(crate::Error::InvalidQuestion { .. })
        ));
        assert!(matches!(
            ChoiceOptions::new(vec![("a".into(), None), ("a".into(), None)]),
            Err(crate::Error::InvalidQuestion { .. })
        ));
        assert!(matches!(
            ScoreLevels::new(vec!["low".into()]),
            Err(crate::Error::InvalidQuestion { .. })
        ));
        assert!(matches!(
            ScoreLevels::new(vec![String::new(); 11]),
            Err(crate::Error::InvalidQuestion { .. })
        ));
        assert!(matches!(
            Content::from_json(Value::Bool(true)),
            Err(crate::Error::InvalidQuestion { .. })
        ));
    }
}
