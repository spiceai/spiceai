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
//! Turning a state and its questions into the prompt text the model was trained on, and the
//! option probabilities back into answers: a port of upstream's `prompting.py`.
//!
//! The head has no fixed meaning per option: it scores whatever option the prompt lists, so
//! the rendering must match upstream's byte for byte.

use crate::pyjson::{collapse_space, dumps_indented, strip};
use crate::schema::{
    Answer, Content, Kind, Meaning, Question, derive_confidence, derive_score_confidence,
};
use crate::{OptionTruncatedSnafu, Result};

/// The labels of a noul's two options, in the order they are listed.
const NOUL_SLOT_LABELS: [&str; 2] = ["false", "true"];
const NOUL_DEFAULT_FALSE: &str = "the statement does not hold for this state";
const NOUL_DEFAULT_TRUE: &str = "the statement holds for this state";

/// A state or instruction as prompt text: text stripped of surrounding white space, and
/// structured data as indented JSON, so the same content always tokenizes the same.
#[must_use]
pub fn render_content(content: Option<&Content>) -> String {
    match content {
        None => String::new(),
        Some(Content::Text(text)) => strip(text).to_string(),
        Some(Content::Structured(value)) => dumps_indented(value),
    }
}

/// The shared prefix of every question's prompt.
#[must_use]
pub fn render_state(state: &Content) -> String {
    format!("<state>\n{}\n</state>\n", render_content(Some(state)))
}

/// A question rendered as prompt text, with what reading its answer back needs.
#[derive(Debug, Clone, PartialEq)]
pub struct RenderedQuestion {
    pub text: String,
    pub kind: Kind,
    /// The name of the option listed `k`-th, which the head's `k`-th logit scores.
    pub slot_labels: Vec<String>,
    /// That option's description, stripped; a score answer's legend reports it.
    pub slot_descriptions: Vec<String>,
    /// The byte range of each option's line within `text`. The head reads an option from the
    /// hidden state of the last token inside its line.
    pub option_spans: Vec<(usize, usize)>,
}

impl RenderedQuestion {
    #[must_use]
    pub fn n_slots(&self) -> usize {
        self.slot_labels.len()
    }
}

/// Numbers the options from 1, one per line (a description is collapsed to one line), and
/// returns each line's byte range within the block.
fn option_block(pairs: &[(String, String)]) -> (String, Vec<(usize, usize)>) {
    let mut lines = Vec::with_capacity(pairs.len());
    let mut spans = Vec::with_capacity(pairs.len());
    let mut cursor = 0;
    for (i, (name, desc)) in pairs.iter().enumerate() {
        let desc = collapse_space(desc);
        let mut line = format!("{}. {name}", i + 1);
        if !desc.is_empty() {
            line.push_str(" \u{2014} ");
            line.push_str(&desc);
        }
        spans.push((cursor, cursor + line.len()));
        cursor += line.len() + 1;
        lines.push(line);
    }
    (lines.join("\n"), spans)
}

/// Renders one question, its options in the order the question lists them.
#[must_use]
pub fn render_question(question: &Question) -> RenderedQuestion {
    let (instructions, pairs, header) = match question {
        Question::Noul {
            instructions,
            criteria,
        } => {
            let meaning = |meaning: &Meaning, default: &str| match meaning {
                Meaning::Default => default.to_string(),
                Meaning::Blank => String::new(),
                Meaning::Given(content) => render_content(Some(content)),
            };
            let pairs = vec![
                (
                    NOUL_SLOT_LABELS[0].to_string(),
                    meaning(&criteria.false_meaning, NOUL_DEFAULT_FALSE),
                ),
                (
                    NOUL_SLOT_LABELS[1].to_string(),
                    meaning(&criteria.true_meaning, NOUL_DEFAULT_TRUE),
                ),
            ];
            (
                instructions,
                pairs,
                "Decide whether the statement is true of the state.",
            )
        }
        Question::Choice {
            instructions,
            options,
        } => {
            let pairs = options
                .as_slice()
                .iter()
                .map(|(name, desc)| (name.clone(), render_content(desc.as_ref())))
                .collect();
            (instructions, pairs, "Select exactly one option.")
        }
        Question::Score {
            instructions,
            levels,
        } => {
            // The level index is the label; the rubric text, unrendered, the description.
            let pairs = levels
                .as_slice()
                .iter()
                .enumerate()
                .map(|(i, desc)| (i.to_string(), desc.clone()))
                .collect();
            (
                instructions,
                pairs,
                "Rate the state against the ordered levels below (lowest first).",
            )
        }
    };
    let kind = question.kind();
    let (block, block_spans) = option_block(&pairs);
    let prefix = format!(
        "<question type=\"{kind}\">\n{header}\n{}\n<options>\n",
        render_content(Some(instructions))
    );
    let base = prefix.len();
    let text = format!("{prefix}{block}\n</options>\n</question>\n<answer>");
    RenderedQuestion {
        text,
        kind,
        slot_labels: pairs.iter().map(|(name, _)| name.clone()).collect(),
        slot_descriptions: pairs
            .iter()
            .map(|(_, desc)| strip(desc).to_string())
            .collect(),
        option_spans: block_spans
            .into_iter()
            .map(|(start, end)| (base + start, base + end))
            .collect(),
    }
}

/// The index of the last token inside each option's line, from the question's token
/// offsets (byte ranges in its text). Refuses an option no token falls inside: truncation
/// removed it, and reading it from a neighbour would be silently wrong.
pub fn option_token_index(
    offsets: &[(usize, usize)],
    spans: &[(usize, usize)],
) -> Result<Vec<usize>> {
    spans
        .iter()
        .map(|&(start, end)| {
            offsets
                .iter()
                .enumerate()
                .filter(|&(_, &(lo, hi))| hi > lo && lo >= start && hi <= end)
                .map(|(j, _)| j)
                .next_back()
                .ok_or_else(|| OptionTruncatedSnafu { start, end }.build())
        })
        .collect()
}

/// `x` rounded to 4 decimal places, ties to even, as Python's `round(x, 4)`.
#[must_use]
pub fn round4(x: f64) -> f64 {
    // Formatting rounds the float's exact decimal value, as Python's round does.
    format!("{x:.4}").parse().unwrap_or(x)
}

/// The answer the option probabilities give, in the order the question listed its options.
#[must_use]
pub fn to_answer(rendered: &RenderedQuestion, probs: &[f64], ordinal_smoothing: f64) -> Answer {
    match rendered.kind {
        Kind::Noul => {
            let index = rendered
                .slot_labels
                .iter()
                .position(|label| label == NOUL_SLOT_LABELS[1])
                .unwrap_or(1);
            Answer::Noul {
                noul: round4(probs.get(index).copied().unwrap_or(0.0)),
            }
        }
        Kind::Choice => {
            // The first of the most probable options, as Python's max() picks it.
            let mut best = 0;
            for (i, &p) in probs.iter().enumerate() {
                if p > probs[best] {
                    best = i;
                }
            }
            Answer::Choice {
                choice: rendered.slot_labels.get(best).cloned().unwrap_or_default(),
                probabilities: rendered
                    .slot_labels
                    .iter()
                    .zip(probs)
                    .map(|(name, &p)| (name.clone(), round4(p)))
                    .collect(),
                confidence: round4(derive_confidence(probs)),
            }
        }
        Kind::Score => {
            // Labels are the level indices, so the expected level is a weighted sum of them.
            let score: f64 = rendered
                .slot_labels
                .iter()
                .zip(probs)
                .map(|(level, &p)| level.parse::<f64>().unwrap_or(0.0) * p)
                .sum();
            Answer::Score {
                score: round4(score),
                legend: rendered
                    .slot_labels
                    .iter()
                    .cloned()
                    .zip(rendered.slot_descriptions.iter().cloned())
                    .collect(),
                probabilities: rendered
                    .slot_labels
                    .iter()
                    .zip(probs)
                    .map(|(level, &p)| (level.clone(), round4(p)))
                    .collect(),
                confidence: round4(derive_score_confidence(probs, ordinal_smoothing)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ChoiceOptions, NoulCriteria, ScoreLevels};

    fn choice() -> Question {
        Question::Choice {
            instructions: "Which team should handle this?".into(),
            options: ChoiceOptions::new(vec![
                ("billing".into(), Some("".into())),
                ("sales".into(), Some("".into())),
                ("retail".into(), Some("".into())),
            ])
            .unwrap_or_else(|e| panic!("{e}")),
        }
    }

    /// The texts below are what upstream's `build_prompt` returns for the model card's
    /// example (`strands-decider ask` builds these questions from its flags).
    #[test]
    fn renders_the_model_card_example_as_upstream() {
        let state = render_state(&"Help! My payouts have been failing for 3 days! ".into());
        assert_eq!(
            state,
            "<state>\nHelp! My payouts have been failing for 3 days!\n</state>\n"
        );

        let noul = render_question(&Question::Noul {
            instructions: "Does this convey urgency?".into(),
            criteria: NoulCriteria::default(),
        });
        assert_eq!(
            noul.text,
            "<question type=\"noul\">\nDecide whether the statement is true of the state.\nDoes this convey urgency?\n<options>\n1. false \u{2014} the statement does not hold for this state\n2. true \u{2014} the statement holds for this state\n</options>\n</question>\n<answer>"
        );
        assert_eq!(noul.slot_labels, ["false", "true"]);

        let choice = render_question(&choice());
        assert_eq!(
            choice.text,
            "<question type=\"choice\">\nSelect exactly one option.\nWhich team should handle this?\n<options>\n1. billing\n2. sales\n3. retail\n</options>\n</question>\n<answer>"
        );
        let spans: Vec<&str> = choice
            .option_spans
            .iter()
            .map(|&(a, b)| &choice.text[a..b])
            .collect();
        assert_eq!(spans, ["1. billing", "2. sales", "3. retail"]);

        let score = render_question(&Question::Score {
            instructions: "How frustrated is the writer?".into(),
            levels: ScoreLevels::new(vec!["calm".into(), "frustrated".into(), "depressed".into()])
                .unwrap_or_else(|e| panic!("{e}")),
        });
        assert_eq!(
            score.text,
            "<question type=\"score\">\nRate the state against the ordered levels below (lowest first).\nHow frustrated is the writer?\n<options>\n1. 0 \u{2014} calm\n2. 1 \u{2014} frustrated\n3. 2 \u{2014} depressed\n</options>\n</question>\n<answer>"
        );
        // Spans are byte ranges: the em dash is three bytes.
        let spans: Vec<&str> = score
            .option_spans
            .iter()
            .map(|&(a, b)| &score.text[a..b])
            .collect();
        assert_eq!(
            spans,
            [
                "1. 0 \u{2014} calm",
                "2. 1 \u{2014} frustrated",
                "3. 2 \u{2014} depressed"
            ]
        );
    }

    #[test]
    fn noul_criteria_replace_or_blank_the_defaults() {
        let noul = render_question(&Question::Noul {
            instructions: "Is it  late?".into(),
            criteria: NoulCriteria {
                true_meaning: Meaning::Given("  past\n midnight ".into()),
                false_meaning: Meaning::Blank,
            },
        });
        assert!(
            noul.text
                .contains("Is it  late?\n<options>\n1. false\n2. true \u{2014} past midnight\n")
        );
        // The legend keeps the description's inner white space; only its line collapses it.
        assert_eq!(noul.slot_descriptions, ["", "past\n midnight"]);
    }

    #[test]
    fn option_index_is_the_last_token_inside_the_line() {
        // "1. a\n2. b": tokens "1", ".", " a", "\n", "2", ".", " b"
        let offsets = [(0, 1), (1, 2), (2, 4), (4, 5), (5, 6), (6, 7), (7, 9)];
        let spans = [(0, 4), (5, 9)];
        assert_eq!(option_token_index(&offsets, &spans).ok(), Some(vec![2, 6]));
        // A token across the line's end is not inside it; nothing inside is refused.
        assert!(matches!(
            option_token_index(&[(0, 9)], &spans),
            Err(crate::Error::OptionTruncated { start: 0, end: 4 })
        ));
    }

    #[test]
    fn rounding_matches_python() {
        // round(x, 4) in CPython 3.12.
        assert!((round4(0.77312) - 0.7731).abs() < 1e-15);
        assert!((round4(0.03125) - 0.0312).abs() < 1e-15);
        assert!((round4(0.858_449_9) - 0.8584).abs() < 1e-15);
    }

    #[test]
    fn answers_read_back_as_upstream() {
        let rendered = render_question(&choice());
        let Answer::Choice {
            choice,
            probabilities,
            confidence,
        } = to_answer(&rendered, &[0.5, 0.25, 0.25], 0.0)
        else {
            panic!("not a choice answer");
        };
        assert_eq!(choice, "billing");
        assert_eq!(probabilities[2], ("retail".to_string(), 0.25));
        assert!((confidence - 0.25).abs() < 1e-15);
    }
}
