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

//! The SQL values decision functions return.
//!
//! `ai_decide` returns a struct with one field per question id:
//!
//! - `noul` → `{probability}`
//! - `choice` → `{choice, probabilities: [{value, probability}], confidence}`
//! - `score` → `{score, probabilities: [{value, label, probability}], confidence}`
//!
//! The typed functions return the one value they ask for.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::common::{DataFusionError, Result, exec_err, internal_err};
use evaluate_api::{Answer, EntryType, NonNullEntry, Question};
use serde_json::{Map, Value, json};

use crate::args::TYPED_QUESTION_ID;
use crate::exec::RowAnswers;
use crate::functions::Kind;

/// The struct field names, shared by the planner's field accesses.
pub(crate) const PROBABILITY: &str = "probability";
pub(crate) const CHOICE: &str = "choice";
pub(crate) const SCORE: &str = "score";
const PROBABILITIES: &str = "probabilities";
const CONFIDENCE: &str = "confidence";
const VALUE: &str = "value";
const LABEL: &str = "label";

/// The threshold above which `ai_if` is true: yes is more likely than no.
pub(crate) const IF_THRESHOLD: f64 = 0.5;

fn float(name: &str) -> Field {
    Field::new(name, DataType::Float64, true)
}

fn list_of(fields: Vec<Field>) -> DataType {
    DataType::List(Arc::new(Field::new_list_field(
        DataType::Struct(Fields::from(fields)),
        true,
    )))
}

/// The struct one question's answer has.
fn answer_type(question: &Question) -> DataType {
    let fields = match question {
        Question::Noul { .. } => vec![float(PROBABILITY)],
        Question::Choice { .. } => vec![
            Field::new(CHOICE, DataType::Utf8, true),
            Field::new(
                PROBABILITIES,
                list_of(vec![
                    Field::new(VALUE, DataType::Utf8, true),
                    float(PROBABILITY),
                ]),
                true,
            ),
            float(CONFIDENCE),
        ],
        Question::Score { .. } => vec![
            float(SCORE),
            Field::new(
                PROBABILITIES,
                list_of(vec![
                    Field::new(VALUE, DataType::Int64, true),
                    Field::new(LABEL, DataType::Utf8, true),
                    float(PROBABILITY),
                ]),
                true,
            ),
            float(CONFIDENCE),
        ],
    };
    DataType::Struct(Fields::from(fields))
}

/// The struct `ai_decide` returns for `questions`.
pub(crate) fn decision_type(questions: &BTreeMap<String, Question>) -> DataType {
    DataType::Struct(Fields::from(
        questions
            .iter()
            .map(|(id, question)| Field::new(id, answer_type(question), true))
            .collect::<Vec<_>>(),
    ))
}

/// The text of a rubric level or option description.
fn entry_text(entry: &EntryType) -> Option<String> {
    match entry {
        EntryType::String(text) => Some(text.clone()),
        EntryType::Null => None,
        EntryType::Array(_) | EntryType::Object(_) => serde_json::to_string(entry).ok(),
    }
}

/// One answer as the JSON of its struct.
fn answer_json(id: &str, question: &Question, answer: &Answer) -> Result<Value> {
    match (question, answer) {
        (Question::Noul { .. }, Answer::Noul { noul }) => Ok(json!({ PROBABILITY: noul })),
        (
            Question::Choice { .. },
            Answer::Choice {
                choice,
                probabilities,
                confidence,
            },
        ) => Ok(json!({
            CHOICE: choice,
            PROBABILITIES: probabilities
                .iter()
                .map(|(value, p)| json!({ VALUE: value, PROBABILITY: p }))
                .collect::<Vec<_>>(),
            CONFIDENCE: confidence,
        })),
        (
            Question::Score { criteria, .. },
            Answer::Score {
                score,
                probabilities,
                confidence,
                ..
            },
        ) => {
            let mut levels = Vec::with_capacity(criteria.len());
            for (level, entry) in criteria.iter().enumerate() {
                let Some(p) = probabilities.get(&level.to_string()) else {
                    return exec_err!(
                        "the model gave no probability for level {level} of question '{id}'"
                    );
                };
                levels.push(json!({
                    VALUE: level,
                    LABEL: entry_text(&EntryType::from(entry as &NonNullEntry)),
                    PROBABILITY: p,
                }));
            }
            Ok(json!({ SCORE: score, PROBABILITIES: levels, CONFIDENCE: confidence }))
        }
        _ => exec_err!("the model answered question '{id}' with the wrong kind of answer"),
    }
}

/// Builds the column a call returns from each row's answers.
pub(crate) fn build(
    kind: Kind,
    questions: &BTreeMap<String, Question>,
    rows: &[RowAnswers],
) -> Result<ArrayRef> {
    match kind {
        Kind::Decide => decision_array(questions, rows),
        Kind::If => {
            let values = typed_values(rows, |answer| match answer {
                Answer::Noul { noul } => Some(*noul > IF_THRESHOLD),
                _ => None,
            })?;
            Ok(Arc::new(BooleanArray::from(values)))
        }
        Kind::Probability => {
            let values = typed_values(rows, |answer| match answer {
                Answer::Noul { noul } => Some(*noul),
                _ => None,
            })?;
            Ok(Arc::new(Float64Array::from(values)))
        }
        Kind::Score => {
            let values = typed_values(rows, |answer| match answer {
                Answer::Score { score, .. } => Some(*score),
                _ => None,
            })?;
            Ok(Arc::new(Float64Array::from(values)))
        }
        Kind::Classify => {
            let values = typed_values(rows, |answer| match answer {
                Answer::Choice { choice, .. } => Some(choice.clone()),
                _ => None,
            })?;
            Ok(Arc::new(StringArray::from(values)))
        }
    }
}

/// The one value a typed function returns per row, from the answer to its question.
fn typed_values<T>(
    rows: &[RowAnswers],
    value: impl Fn(&Answer) -> Option<T>,
) -> Result<Vec<Option<T>>> {
    rows.iter()
        .map(|row| match row {
            None => Ok(None),
            Some(answers) => match answers.get(TYPED_QUESTION_ID) {
                Some(Answer::Refusal {}) => Ok(None),
                Some(answer) => match value(answer) {
                    Some(v) => Ok(Some(v)),
                    None => {
                        internal_err!("the model's answer does not match the question it was asked")
                    }
                },
                None => {
                    internal_err!("the model's answer does not match the question it was asked")
                }
            },
        })
        .collect()
}

fn decision_array(questions: &BTreeMap<String, Question>, rows: &[RowAnswers]) -> Result<ArrayRef> {
    let data_type = decision_type(questions);
    let mut values = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(answers) = row else {
            values.push(json!({ "v": null }));
            continue;
        };
        let mut object = Map::new();
        for (id, question) in questions {
            let value = match answers.get(id) {
                // Declined under `on_error => 'null'`: this answer is NULL, the rest stand.
                Some(Answer::Refusal {}) => Value::Null,
                Some(answer) => answer_json(id, question, answer)?,
                None => return exec_err!("the model returned no answer for question '{id}'"),
            };
            object.insert(id.clone(), value);
        }
        values.push(json!({ "v": object }));
    }

    let schema = Arc::new(Schema::new(vec![Field::new("v", data_type, true)]));
    let mut decoder = arrow_json::ReaderBuilder::new(schema)
        .with_batch_size(rows.len().max(1))
        .build_decoder()?;
    decoder.serialize(&values)?;
    match decoder.flush()? {
        Some(batch) => Ok(Arc::clone(batch.column(0))),
        None if rows.is_empty() => Ok(arrow::array::new_empty_array(&decision_type(questions))),
        None => Err(DataFusionError::Internal(
            "decoding the decision answers produced no rows".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::util::pretty::pretty_format_columns;

    fn questions() -> BTreeMap<String, Question> {
        serde_json::from_value(json!({
            "team": {"type": "choice", "criteria": {"billing": "Payments", "technical": null}},
            "tone": {"type": "score", "instructions": "How upset?", "criteria": ["calm", "furious"]},
            "urgent": {"type": "noul", "instructions": "Urgent?"}
        }))
        .expect("questions")
    }

    #[test]
    fn decide_rows_build_a_struct_with_nulls() {
        let answers: BTreeMap<String, Answer> = serde_json::from_value(json!({
            "team": {"type": "choice", "choice": "billing", "probabilities": {"billing": 0.8, "technical": 0.2}, "confidence": 0.6},
            "tone": {"type": "score", "score": 0.25, "legend": {}, "probabilities": {"0": 0.75, "1": 0.25}, "confidence": 0.5},
            "urgent": {"type": "noul", "noul": 0.9}
        }))
        .expect("answers");
        let array = build(Kind::Decide, &questions(), &[Some(answers), None]).expect("builds");
        insta::assert_snapshot!(
            "decide_struct",
            pretty_format_columns("ai_decide", &[array]).expect("formats")
        );
    }

    #[test]
    fn typed_functions_return_one_value() {
        let row = |noul: f64| {
            Some(BTreeMap::from([(
                TYPED_QUESTION_ID.to_string(),
                Answer::Noul { noul },
            )]))
        };
        let q = BTreeMap::new();
        let flags = build(Kind::If, &q, &[row(0.5), row(0.51), None]).expect("ai_if");
        assert_eq!(
            flags.as_ref(),
            &BooleanArray::from(vec![Some(false), Some(true), None]) as &dyn arrow::array::Array
        );
        let probabilities =
            build(Kind::Probability, &q, &[row(0.25), None]).expect("ai_probability");
        assert_eq!(
            probabilities.as_ref(),
            &Float64Array::from(vec![Some(0.25), None]) as &dyn arrow::array::Array
        );
    }
}
