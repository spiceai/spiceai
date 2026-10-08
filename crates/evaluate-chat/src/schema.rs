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

//! The JSON schema a reply must match, generated per request from the questions.
//!
//! The reply is `{"answers": {<question id>: <answer>}}`, where each answer is pinned
//! to its own question's values: a probability or boolean for a noul, and for a choice
//! or score either one probability per option or level, or a single option or level.
//! The question's instructions and criteria travel as schema descriptions, so the
//! schema alone tells the model what each answer means.
//!
//! Every object is closed (`additionalProperties: false`) and lists every property as
//! required, which is what strict structured-output modes demand. Nested objects are
//! written inline rather than behind `$ref`: some providers drop the keywords that sit
//! beside a reference, and small models copy the reference itself as their answer.
//!
//! Object keys are written in sorted order, so the schema — and the prompt that
//! carries it — serializes identically whether or not `serde_json` preserves insertion
//! order, a feature any crate in the build can switch on.

use std::collections::BTreeMap;

use evaluate_api::{EntryType, NoulCriteria, NullableEntry, Question};
use serde_json::{Map, Value, json};

use crate::AnswerMode;

const ANSWERS_DESCRIPTION: &str = "Exactly one answer per property below. Use these property names verbatim and do not add, rename, or nest them under any other key.";

/// What the model is told for an absent or `null` instruction or criterion.
const NO_INSTRUCTIONS: &str = "No additional instructions.";

/// The schema of the whole reply.
pub(crate) fn reply_schema(questions: &BTreeMap<String, Question>, mode: AnswerMode) -> Value {
    let properties: Map<String, Value> = questions
        .iter()
        .map(|(id, question)| (id.clone(), answer_schema(question, mode)))
        .collect();
    json!({
        "additionalProperties": false,
        "properties": {
            "answers": {
                "additionalProperties": false,
                "description": ANSWERS_DESCRIPTION,
                "properties": properties,
                "required": questions.keys().collect::<Vec<_>>(),
                "type": "object",
            }
        },
        "required": ["answers"],
        "type": "object",
    })
}

fn answer_schema(question: &Question, mode: AnswerMode) -> Value {
    let (instructions, labels, probability_intro, discrete_header) = match question {
        Question::Noul {
            instructions,
            criteria,
        } => {
            let kind = match mode {
                AnswerMode::Probabilities => "number",
                AnswerMode::Discrete => "boolean",
            };
            return json!({
                "description": noul_description(instructions, criteria.as_ref(), mode),
                "type": kind,
            });
        }
        Question::Choice {
            instructions,
            criteria,
        } => (
            instructions,
            criteria
                .iter()
                .map(|(option, criterion)| (option.clone(), entry_text(criterion)))
                .collect::<Vec<_>>(),
            "Each property maps an option to the probability that it is the best answer.",
            "Choice labels, answer with one label",
        ),
        Question::Score {
            instructions,
            criteria,
        } => (
            instructions,
            criteria
                .iter()
                .enumerate()
                .map(|(level, criterion)| {
                    (level.to_string(), entry_text(&EntryType::from(criterion)))
                })
                .collect(),
            "Each property maps a rubric level to the probability that the document matches it.",
            "Score levels, answer with the integer",
        ),
    };
    let asked = instruction_text(instructions);
    match mode {
        AnswerMode::Probabilities => {
            probability_map(&format!("{probability_intro}\nQuestion: {asked}"), labels)
        }
        AnswerMode::Discrete => {
            let listing = labels
                .iter()
                .map(|(label, text)| format!("{label} = {text}"))
                .collect::<Vec<_>>()
                .join("\n");
            let description = format!("{asked}\n{discrete_header}:\n{listing}");
            // A choice is answered with its label, a score with its level's index.
            if matches!(question, Question::Choice { .. }) {
                let options: Vec<String> = labels.into_iter().map(|(label, _)| label).collect();
                json!({"description": description, "enum": options, "type": "string"})
            } else {
                json!({"description": description, "type": "integer"})
            }
        }
    }
}

/// An object holding one probability per option or rubric level, each described by
/// its criterion.
fn probability_map(description: &str, labels: Vec<(String, String)>) -> Value {
    let mut properties = Map::new();
    let mut required = Vec::new();
    for (label, criterion) in labels {
        properties.insert(
            label.clone(),
            json!({"description": criterion, "type": "number"}),
        );
        required.push(Value::String(label));
    }
    json!({
        "additionalProperties": false,
        "description": description,
        "properties": properties,
        "required": required,
        "type": "object",
    })
}

fn noul_description(
    instructions: &NullableEntry,
    criteria: Option<&NoulCriteria>,
    mode: AnswerMode,
) -> String {
    let question = instruction_text(instructions);
    let description = match mode {
        AnswerMode::Probabilities => format!(
            "Probability that the answer is yes or the assertion is true. 0 means no or false, 0.5 means uncertain, and 1 means yes or true.\nQuestion: {question}"
        ),
        AnswerMode::Discrete => question,
    };
    match criteria {
        Some(criteria) => format!(
            "{description}\nTrue criteria: {}\nFalse criteria: {}",
            instruction_text(&criteria.true_meaning),
            instruction_text(&criteria.false_meaning)
        ),
        None => description,
    }
}

/// An instruction or noul criterion as prompt text: a string as written, anything else
/// as compact JSON.
fn instruction_text(entry: &NullableEntry) -> String {
    match entry {
        NullableEntry::Absent | NullableEntry::Null => NO_INSTRUCTIONS.to_string(),
        NullableEntry::Value(value) => entry_text(value),
    }
}

fn entry_text(entry: &EntryType) -> String {
    match entry {
        EntryType::Null => NO_INSTRUCTIONS.to_string(),
        EntryType::String(text) => text.clone(),
        EntryType::Array(items) => Value::Array(items.clone()).to_string(),
        EntryType::Object(fields) => Value::Object(fields.clone()).to_string(),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn sample_questions() -> BTreeMap<String, Question> {
        serde_json::from_value(json!({
            "is_urgent": {
                "type": "noul",
                "instructions": "Does this convey urgency?",
                "criteria": {"true": "Money or access is blocked now", "false": null}
            },
            "team": {
                "type": "choice",
                "instructions": ["Route the ticket", "Prefer technical when ambiguous"],
                "criteria": {"billing": "Payments and invoices", "technical": {"area": "bugs"}, "sales": null}
            },
            "tone": {
                "type": "score",
                "criteria": ["calm", "annoyed", "furious"]
            }
        }))
        .expect("sample questions")
    }

    #[test]
    fn probabilities_schema() {
        insta::assert_snapshot!(
            "probabilities_schema",
            serde_json::to_string_pretty(&reply_schema(
                &sample_questions(),
                AnswerMode::Probabilities
            ))
            .expect("schema serializes")
        );
    }

    #[test]
    fn discrete_schema() {
        insta::assert_snapshot!(
            "discrete_schema",
            serde_json::to_string_pretty(&reply_schema(&sample_questions(), AnswerMode::Discrete))
                .expect("schema serializes")
        );
    }

    /// Every object is closed and requires every property it lists, so a strict
    /// structured-output mode accepts the schema as generated.
    #[test]
    fn every_object_is_closed_and_requires_all_its_properties() {
        fn visit(schema: &Value, path: &str) {
            let Some(object) = schema.as_object() else {
                return;
            };
            if let Some(properties) = object.get("properties").and_then(Value::as_object) {
                assert_eq!(
                    object.get("additionalProperties"),
                    Some(&Value::Bool(false)),
                    "{path} must be closed"
                );
                let required: Vec<&str> = object
                    .get("required")
                    .and_then(Value::as_array)
                    .expect("required list")
                    .iter()
                    .filter_map(Value::as_str)
                    .collect();
                let listed: Vec<&str> = properties.keys().map(String::as_str).collect();
                assert_eq!(required, listed, "{path} must require every property");
                for (name, property) in properties {
                    visit(property, &format!("{path}.{name}"));
                }
            }
        }
        for mode in [AnswerMode::Probabilities, AnswerMode::Discrete] {
            visit(&reply_schema(&sample_questions(), mode), "$");
        }
    }

    /// Keys are written in sorted order, so a build where `serde_json` preserves
    /// insertion order serializes the same schema, and prompt, as one where it does not.
    #[test]
    fn object_keys_are_in_sorted_order() {
        fn visit(value: &Value, path: &str) {
            match value {
                Value::Object(object) => {
                    let keys: Vec<&String> = object.keys().collect();
                    let mut sorted = keys.clone();
                    sorted.sort();
                    assert_eq!(keys, sorted, "{path} keys must be written in sorted order");
                    for (key, child) in object {
                        visit(child, &format!("{path}.{key}"));
                    }
                }
                Value::Array(items) => {
                    for item in items {
                        visit(item, path);
                    }
                }
                _ => {}
            }
        }
        for mode in [AnswerMode::Probabilities, AnswerMode::Discrete] {
            visit(&reply_schema(&sample_questions(), mode), "$");
        }
    }
}
