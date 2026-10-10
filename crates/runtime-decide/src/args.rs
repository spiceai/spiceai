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

//! Reading a decision call's arguments: which parameter each one is, and the
//! questions and options its constants define.
//!
//! `DataFusion` resolves `name => value` arguments to their parameter positions and then
//! drops the optional parameters that were not given, so `ai_if(x, 'c', on_error =>
//! 'null')` arrives with three arguments, the same count as `ai_if(x, 'c', 'jev')`. A
//! named literal keeps its parameter name in `spice.parameter_name` field metadata,
//! which is what tells the two apart. The planner rule and the function both read
//! arguments through [`positions`], so neither has to guess.

use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;

use arrow::array::{Array, AsArray};
use datafusion::common::{DataFusionError, Result, ScalarValue, plan_datafusion_err, plan_err};
use datafusion::logical_expr::{ColumnarValue, Expr, ScalarFunctionArgs};
use evaluate_api::{EntryType, NonNullEntry, NullableEntry, Question};
use serde::Deserialize;
use serde::de::{Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Value};

use crate::functions::Kind;

/// The metadata key `DataFusion` (Spice fork) sets on a named literal argument.
const PARAMETER_NAME_KEY: &str = "spice.parameter_name";

/// Fewest and most labels `ai_classify` accepts.
const MIN_LABELS: usize = 2;
const MAX_LABELS: usize = 255;
/// Fewest and most levels `ai_score` accepts, the bound `Question` enforces on an
/// `ai_decide` score question.
const MIN_LEVELS: usize = 2;
const MAX_LEVELS: usize = 10;
/// Most options a choice question in `ai_decide` may offer.
const MAX_CHOICE_OPTIONS: usize = 255;

/// The parameters of a decision function, in position order.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Layout {
    pub params: &'static [&'static str],
    pub required: usize,
}

/// What happens to a row the model cannot answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum OnError {
    /// Fail the query, naming the model and the cause.
    #[default]
    Fail,
    /// Return NULL for the row.
    Null,
}

impl OnError {
    fn parse(kind: Kind, value: Option<&str>) -> Result<Self> {
        match value.map(str::trim) {
            None => Ok(Self::Fail),
            Some(v) if v.eq_ignore_ascii_case("fail") => Ok(Self::Fail),
            Some(v) if v.eq_ignore_ascii_case("null") => Ok(Self::Null),
            Some(other) => plan_err!(
                "{}: `on_error` must be 'fail' or 'null', not '{other}'. Usage: {}",
                kind.name(),
                kind.usage()
            ),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Fail => "fail",
            Self::Null => "null",
        }
    }
}

/// The parameter position of each supplied argument. `names[i]` is the parameter name
/// recorded for argument `i`, or `None` for a positional argument; positional arguments
/// come first, so they fill positions from 0.
pub(crate) fn positions(kind: Kind, names: &[Option<String>]) -> Result<Vec<usize>> {
    let layout = kind.layout();
    let mut taken = vec![false; layout.params.len()];
    let mut positions = Vec::with_capacity(names.len());
    for (index, name) in names.iter().enumerate() {
        let position = match name {
            Some(name) => layout
                .params
                .iter()
                .position(|param| param.eq_ignore_ascii_case(name))
                .ok_or_else(|| {
                    plan_datafusion_err!(
                        "{}: unknown argument `{name}`. Usage: {}",
                        kind.name(),
                        kind.usage()
                    )
                })?,
            None => index,
        };
        if position >= layout.params.len() {
            return plan_err!(
                "{}: too many arguments. Usage: {}",
                kind.name(),
                kind.usage()
            );
        }
        if taken[position] {
            return plan_err!(
                "{}: argument `{}` is given more than once. Usage: {}",
                kind.name(),
                layout.params[position],
                kind.usage()
            );
        }
        taken[position] = true;
        positions.push(position);
    }
    for (position, param) in layout.params.iter().enumerate().take(layout.required) {
        if !taken[position] {
            return plan_err!(
                "{}: missing required argument `{param}`. Usage: {}",
                kind.name(),
                kind.usage()
            );
        }
    }
    Ok(positions)
}

fn literal_parameter_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(_, Some(metadata)) => metadata.inner().get(PARAMETER_NAME_KEY).cloned(),
        _ => None,
    }
}

/// A call's arguments in every position, with constants normalized.
///
/// Returns `None` when `args` is already in that form, so the rule can report that it
/// changed nothing.
pub(crate) fn canonical_args(kind: Kind, args: &[Expr]) -> Result<Option<Vec<Expr>>> {
    let layout = kind.layout();
    let names: Vec<Option<String>> = args.iter().map(literal_parameter_name).collect();
    let positions = positions(kind, &names)?;
    let mut slots: Vec<Option<&Expr>> = vec![None; layout.params.len()];
    for (arg, position) in args.iter().zip(positions) {
        slots[position] = Some(arg);
    }

    let mut canonical = Vec::with_capacity(slots.len());
    for (position, slot) in slots.into_iter().enumerate() {
        let param = layout.params[position];
        let value = if position == 0 {
            // The input is per row; it is used as written.
            match slot {
                Some(expr) => expr.clone(),
                None => return plan_err!("{}: missing `input`", kind.name()),
            }
        } else {
            let text = match slot {
                Some(expr) => expr_constant(kind, param, expr)?,
                None => None,
            };
            string_literal(normalize_constant(kind, param, text)?)
        };
        canonical.push(value);
    }

    if canonical.as_slice() == args {
        Ok(None)
    } else {
        Ok(Some(canonical))
    }
}

fn string_literal(value: Option<String>) -> Expr {
    Expr::Literal(ScalarValue::Utf8(value), None)
}

/// The text of a constant argument: a string, or — for `labels` and `levels` — a list
/// of strings, given as its JSON. `None` for NULL.
fn expr_constant(kind: Kind, param: &str, expr: &Expr) -> Result<Option<String>> {
    match expr {
        Expr::Literal(scalar, _) => scalar_constant(kind, param, scalar),
        // `['a', 'b']` is planned as `make_array('a', 'b')` until constants are folded.
        Expr::ScalarFunction(function)
            if function.func.name() == "make_array" && is_list_param(param) =>
        {
            let mut items = Vec::with_capacity(function.args.len());
            for item in &function.args {
                match expr_constant(kind, param, item)? {
                    Some(text) => items.push(Value::String(text)),
                    None => {
                        return plan_err!(
                            "{}: `{param}` cannot contain NULL. Usage: {}",
                            kind.name(),
                            kind.usage()
                        );
                    }
                }
            }
            Ok(Some(Value::Array(items).to_string()))
        }
        _ => not_constant(kind, param),
    }
}

fn is_list_param(param: &str) -> bool {
    matches!(param, "labels" | "levels")
}

fn not_constant<T>(kind: Kind, param: &str) -> Result<T> {
    Err(not_constant_error(kind, param))
}

fn not_constant_error(kind: Kind, param: &str) -> DataFusionError {
    let example = match param {
        "labels" => " such as ['billing', 'technical']",
        "levels" => " such as ['low', 'medium', 'high']",
        "questions" => {
            r#" such as '{"urgent": {"type": "noul", "instructions": "Is this urgent?"}}'"#
        }
        "on_error" => " ('fail' or 'null')",
        _ => " string",
    };
    plan_datafusion_err!(
        "{}: `{param}` must be a constant{example}; it cannot vary by row. Usage: {}",
        kind.name(),
        kind.usage()
    )
}

/// The text of a constant scalar, as [`expr_constant`] describes.
fn scalar_constant(kind: Kind, param: &str, scalar: &ScalarValue) -> Result<Option<String>> {
    if scalar.is_null() {
        return Ok(None);
    }
    match scalar {
        ScalarValue::Utf8(Some(text))
        | ScalarValue::LargeUtf8(Some(text))
        | ScalarValue::Utf8View(Some(text)) => Ok(Some(text.clone())),
        ScalarValue::List(list) if is_list_param(param) => {
            list_constant(kind, param, &list.value(0))
        }
        ScalarValue::LargeList(list) if is_list_param(param) => {
            list_constant(kind, param, &list.value(0))
        }
        ScalarValue::FixedSizeList(list) if is_list_param(param) => {
            list_constant(kind, param, &list.value(0))
        }
        _ => not_constant(kind, param),
    }
}

fn list_constant(
    kind: Kind,
    param: &str,
    values: &arrow::array::ArrayRef,
) -> Result<Option<String>> {
    let values = arrow::compute::cast(values, &arrow::datatypes::DataType::Utf8)
        .map_err(|_| not_constant_error(kind, param))?;
    let strings = values.as_string::<i32>();
    let mut items = Vec::with_capacity(strings.len());
    for item in strings {
        match item {
            Some(text) => items.push(Value::String(text.to_string())),
            None => {
                return plan_err!(
                    "{}: `{param}` cannot contain NULL. Usage: {}",
                    kind.name(),
                    kind.usage()
                );
            }
        }
    }
    Ok(Some(Value::Array(items).to_string()))
}

/// Validates a constant and puts it in the one form the planner and the function read:
/// labels as a JSON object of label to description, levels as a JSON array, questions
/// re-serialized, `on_error` lowercase.
fn normalize_constant(kind: Kind, param: &str, text: Option<String>) -> Result<Option<String>> {
    match param {
        "condition" => required_text(kind, param, text).map(Some),
        "instructions" => {
            if kind == Kind::Score {
                required_text(kind, param, text).map(Some)
            } else {
                Ok(text.filter(|t| !t.trim().is_empty()))
            }
        }
        "labels" => {
            let labels = parse_labels(kind, &required_text(kind, param, text)?)?;
            Ok(Some(Value::Object(labels).to_string()))
        }
        "levels" => {
            let levels = parse_levels(kind, &required_text(kind, param, text)?)?;
            Ok(Some(Value::Array(levels).to_string()))
        }
        "questions" => {
            let questions = parse_questions(kind, &required_text(kind, param, text)?)?;
            serde_json::to_string(&questions)
                .map(Some)
                .map_err(|e| DataFusionError::External(Box::new(e)))
        }
        "model" => match text {
            Some(name) if name.trim().is_empty() => plan_err!(
                "{}: `model` cannot be empty. Name a model under `models` in the Spicepod, or omit `model`.",
                kind.name()
            ),
            other => Ok(other),
        },
        "on_error" => Ok(Some(
            OnError::parse(kind, text.as_deref())?.as_str().to_string(),
        )),
        _ => Ok(text),
    }
}

fn required_text(kind: Kind, param: &str, text: Option<String>) -> Result<String> {
    match text {
        Some(text) if !text.trim().is_empty() => Ok(text),
        _ => plan_err!(
            "{}: `{param}` is required and cannot be empty or NULL. Usage: {}",
            kind.name(),
            kind.usage()
        ),
    }
}

/// Labels from a JSON array of strings, or a JSON object of label to description
/// (a string, or null when the label says it all).
fn parse_labels(kind: Kind, text: &str) -> Result<Map<String, Value>> {
    let invalid = || {
        plan_datafusion_err!(
            "{}: `labels` must be a list of labels such as ['billing', 'technical'], or a JSON object of label to description such as '{{\"billing\": \"Payments and refunds\"}}'.",
            kind.name()
        )
    };
    let mut labels = Map::new();
    let mut add = |label: String, description: Value| -> Result<()> {
        if label.trim().is_empty() {
            return plan_err!("{}: a label cannot be empty", kind.name());
        }
        if labels.insert(label.clone(), description).is_some() {
            return plan_err!(
                "{}: label '{label}' is listed more than once. List each label once.",
                kind.name()
            );
        }
        Ok(())
    };
    match serde_json::from_str::<LabelsJson>(text).map_err(|_| invalid())? {
        LabelsJson::List(items) => {
            for item in items {
                let Value::String(label) = item else {
                    return Err(invalid());
                };
                add(label, Value::Null)?;
            }
        }
        LabelsJson::Object(entries) => {
            for (label, description) in entries {
                match description {
                    Value::String(_) | Value::Null => add(label, description)?,
                    _ => return Err(invalid()),
                }
            }
        }
    }
    if !(MIN_LABELS..=MAX_LABELS).contains(&labels.len()) {
        return plan_err!(
            "{}: `labels` must list between {MIN_LABELS} and {MAX_LABELS} labels; it lists {}. Include a fallback such as 'other' when no label may fit.",
            kind.name(),
            labels.len()
        );
    }
    Ok(labels)
}

/// Score levels from a JSON array of strings, lowest first.
fn parse_levels(kind: Kind, text: &str) -> Result<Vec<Value>> {
    let invalid = || {
        plan_datafusion_err!(
            "{}: `levels` must be a list of level descriptions, lowest first, such as ['calm', 'annoyed', 'furious'].",
            kind.name()
        )
    };
    let Value::Array(items) = serde_json::from_str::<Value>(text).map_err(|_| invalid())? else {
        return Err(invalid());
    };
    for item in &items {
        match item {
            Value::String(level) if !level.trim().is_empty() => {}
            _ => return Err(invalid()),
        }
    }
    if !(MIN_LEVELS..=MAX_LEVELS).contains(&items.len()) {
        return plan_err!(
            "{}: `levels` must list between {MIN_LEVELS} and {MAX_LEVELS} levels; it lists {}.",
            kind.name(),
            items.len()
        );
    }
    Ok(items)
}

/// The questions of an `ai_decide` call: a JSON object of question id to a `noul`,
/// `choice` or `score` question, the same grammar as `TypeSafe`'s API and Databricks'
/// `ai_decide`.
pub(crate) fn parse_questions(kind: Kind, text: &str) -> Result<BTreeMap<String, Question>> {
    let Entries(entries) = serde_json::from_str::<Entries<Question>>(text).map_err(|e| {
        plan_datafusion_err!(
            "{}: `questions` is not a valid questions object: {e}. Each entry needs a `type` of 'noul', 'choice' or 'score', with `instructions` and, for choice and score, `criteria`.",
            kind.name()
        )
    })?;
    if entries.is_empty() {
        return plan_err!(
            "{}: `questions` must contain at least one question.",
            kind.name()
        );
    }
    let mut questions = BTreeMap::new();
    for (id, question) in entries {
        if id.trim().is_empty() {
            return plan_err!("{}: a question id cannot be empty.", kind.name());
        }
        if let Question::Choice { criteria, .. } = &question {
            if criteria.is_empty() || criteria.len() > MAX_CHOICE_OPTIONS {
                return plan_err!(
                    "{}: choice question '{id}' must offer between 1 and {MAX_CHOICE_OPTIONS} options in `criteria`; it offers {}.",
                    kind.name(),
                    criteria.len()
                );
            }
            if criteria.keys().any(|label| label.trim().is_empty()) {
                return plan_err!(
                    "{}: choice question '{id}' has an option with an empty label in `criteria`. Give every option a label.",
                    kind.name()
                );
            }
        }
        if questions.contains_key(&id) {
            return plan_err!(
                "{}: question '{id}' is listed more than once. Give each question its own id.",
                kind.name()
            );
        }
        questions.insert(id, question);
    }
    Ok(questions)
}

/// A JSON object's entries in document order, repeats included. A map would keep only
/// the last value of a repeated key, silently dropping a question or a label.
struct Entries<V>(Vec<(String, V)>);

impl<'de, V: Deserialize<'de>> Deserialize<'de> for Entries<V> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct EntriesVisitor<V>(PhantomData<V>);

        impl<'de, V: Deserialize<'de>> Visitor<'de> for EntriesVisitor<V> {
            type Value = Entries<V>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a JSON object")
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Entries<V>, A::Error> {
                entries(map).map(Entries)
            }
        }

        deserializer.deserialize_map(EntriesVisitor(PhantomData))
    }
}

/// `labels` as written: a list, or an object's entries with any repeat kept, so that a
/// repeated label is reported rather than collapsed.
enum LabelsJson {
    List(Vec<Value>),
    Object(Vec<(String, Value)>),
}

impl<'de> Deserialize<'de> for LabelsJson {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct LabelsVisitor;

        impl<'de> Visitor<'de> for LabelsVisitor {
            type Value = LabelsJson;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a list of labels, or an object of label to description")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut items: A) -> Result<LabelsJson, A::Error> {
                let mut labels = Vec::with_capacity(items.size_hint().unwrap_or(0));
                while let Some(label) = items.next_element()? {
                    labels.push(label);
                }
                Ok(LabelsJson::List(labels))
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<LabelsJson, A::Error> {
                entries(map).map(LabelsJson::Object)
            }
        }

        deserializer.deserialize_any(LabelsVisitor)
    }
}

/// Every entry of a JSON object, in order, repeats included.
fn entries<'de, V: Deserialize<'de>, A: MapAccess<'de>>(
    mut map: A,
) -> Result<Vec<(String, V)>, A::Error> {
    let mut entries = Vec::with_capacity(map.size_hint().unwrap_or(0));
    while let Some(entry) = map.next_entry()? {
        entries.push(entry);
    }
    Ok(entries)
}

/// The questions of an `ai_decide` call from its `questions` argument, for its return
/// type.
pub(crate) fn questions_from_scalar(
    kind: Kind,
    scalar: Option<&ScalarValue>,
) -> Result<BTreeMap<String, Question>> {
    match scalar.map(|s| scalar_constant(kind, "questions", s)) {
        Some(Ok(Some(text))) => parse_questions(kind, &text),
        Some(Err(e)) => Err(e),
        None | Some(Ok(None)) => not_constant(kind, "questions"),
    }
}

fn text_entry(text: &str) -> EntryType {
    EntryType::String(text.to_string())
}

/// The question a typed function asks, from its normalized constants by position.
pub(crate) fn typed_question(kind: Kind, constants: &[Option<String>]) -> Result<Question> {
    let constant = |position: usize| constants.get(position).cloned().flatten();
    let required = |position: usize| {
        constant(position).ok_or_else(|| {
            plan_datafusion_err!(
                "{}: missing required argument `{}`. Usage: {}",
                kind.name(),
                kind.layout().params[position],
                kind.usage()
            )
        })
    };
    match kind {
        Kind::If | Kind::Probability => Ok(Question::Noul {
            instructions: NullableEntry::Value(text_entry(&required(1)?)),
            criteria: None,
        }),
        Kind::Classify => {
            let labels = parse_labels(kind, &required(1)?)?;
            let criteria = labels
                .into_iter()
                .map(|(label, description)| {
                    let entry = match description {
                        Value::String(text) => EntryType::String(text),
                        _ => EntryType::Null,
                    };
                    (label, entry)
                })
                .collect();
            let instructions = match constant(2) {
                Some(text) => NullableEntry::Value(text_entry(&text)),
                None => NullableEntry::Absent,
            };
            Ok(Question::Choice {
                instructions,
                criteria,
            })
        }
        Kind::Score => {
            let levels = parse_levels(kind, &required(2)?)?;
            let criteria = levels
                .into_iter()
                .filter_map(|level| match level {
                    Value::String(text) => Some(NonNullEntry::String(text)),
                    _ => None,
                })
                .collect();
            Ok(Question::Score {
                instructions: NullableEntry::Value(text_entry(&required(1)?)),
                criteria,
            })
        }
        Kind::Decide => plan_err!("ai_decide asks the questions it is given"),
    }
}

/// Checks a call's arguments while its query is planned, so a mistake is reported
/// before anything runs. A `labels` or `levels` list written as `['a', 'b']` is not a
/// literal yet at this point; the planner rule checks it.
pub(crate) fn check_planning_args(
    kind: Kind,
    arg_fields: &[arrow::datatypes::FieldRef],
    scalar_arguments: &[Option<&ScalarValue>],
) -> Result<()> {
    let layout = kind.layout();
    let names: Vec<Option<String>> = arg_fields
        .iter()
        .map(|field| field.metadata().get(PARAMETER_NAME_KEY).cloned())
        .collect();
    for (index, position) in positions(kind, &names)?.into_iter().enumerate() {
        if position == 0 {
            continue;
        }
        let param = layout.params[position];
        match scalar_arguments.get(index).copied().flatten() {
            Some(scalar) => {
                let text = scalar_constant(kind, param, scalar)?;
                normalize_constant(kind, param, text)?;
            }
            None if is_list_param(param) => {}
            None => return not_constant(kind, param),
        }
    }
    Ok(())
}

/// The typed functions a planner-built `ai_decide` call stands for, read from its
/// question ids (`ai_if_0`, `ai_classify_1`, ...), so errors name the functions the
/// query used. `None` for a user's own `ai_decide` questions.
pub(crate) fn lowered_from(questions: &BTreeMap<String, Question>) -> Option<String> {
    let mut names: Vec<&str> = Vec::new();
    for id in questions.keys() {
        let (prefix, index) = id.rsplit_once('_')?;
        if index.is_empty() || !index.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let kind = [Kind::If, Kind::Probability, Kind::Classify, Kind::Score]
            .into_iter()
            .find(|kind| kind.name() == prefix)?;
        if !names.contains(&kind.name()) {
            names.push(kind.name());
        }
    }
    (!names.is_empty()).then(|| names.join(", "))
}

/// The id a typed function's question has when the function runs on its own.
pub(crate) const TYPED_QUESTION_ID: &str = "q";

/// A call's questions and options, read when the function runs.
#[derive(Debug)]
pub(crate) struct CallArgs {
    pub questions: BTreeMap<String, Question>,
    pub model: Option<String>,
    pub on_error: OnError,
}

impl CallArgs {
    /// Reads the constants of an invocation. A call the planner rule built has every
    /// argument in its position; any other is placed by the parameter names recorded on
    /// its argument fields.
    pub(crate) fn from_invocation(kind: Kind, args: &ScalarFunctionArgs) -> Result<Self> {
        let layout = kind.layout();
        let positions: Vec<usize> = if args.args.len() == layout.params.len() {
            (0..layout.params.len()).collect()
        } else {
            let names: Vec<Option<String>> = args
                .arg_fields
                .iter()
                .map(|field| field.metadata().get(PARAMETER_NAME_KEY).cloned())
                .collect();
            positions(kind, &names)?
        };

        let mut constants: Vec<Option<String>> = vec![None; layout.params.len()];
        for (value, position) in args.args.iter().zip(positions) {
            if position == 0 {
                continue;
            }
            let param = layout.params[position];
            let text = match value {
                ColumnarValue::Scalar(scalar) => scalar_constant(kind, param, scalar)?,
                ColumnarValue::Array(_) => return not_constant(kind, param),
            };
            constants[position] = normalize_constant(kind, param, text)?;
        }

        let model_position = layout.params.len() - 2;
        let on_error_position = layout.params.len() - 1;
        let questions = match kind {
            Kind::Decide => parse_questions(
                kind,
                constants[1]
                    .as_deref()
                    .ok_or_else(|| not_constant_error(kind, "questions"))?,
            )?,
            _ => BTreeMap::from([(
                TYPED_QUESTION_ID.to_string(),
                typed_question(kind, &constants)?,
            )]),
        };
        Ok(Self {
            questions,
            model: constants[model_position].clone(),
            on_error: OnError::parse(kind, constants[on_error_position].as_deref())?,
        })
    }
}

/// The normalized constants of a call whose arguments are already in every position,
/// as [`canonical_args`] leaves them.
pub(crate) fn canonical_constants(kind: Kind, args: &[Expr]) -> Result<Vec<Option<String>>> {
    let layout = kind.layout();
    if args.len() != layout.params.len() {
        return plan_err!(
            "{}: expected {} arguments after analysis, found {}",
            kind.name(),
            layout.params.len(),
            args.len()
        );
    }
    args.iter()
        .enumerate()
        .map(|(position, arg)| {
            if position == 0 {
                Ok(None)
            } else {
                expr_constant(kind, layout.params[position], arg)
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};
    use datafusion::scalar::ScalarValue;
    use std::collections::BTreeMap as Map2;

    fn named(name: &str, value: &str) -> Expr {
        let metadata = datafusion::common::metadata::FieldMetadata::new(Map2::from([(
            PARAMETER_NAME_KEY.to_string(),
            name.to_string(),
        )]));
        Expr::Literal(ScalarValue::Utf8(Some(value.to_string())), Some(metadata))
    }

    fn null() -> Expr {
        Expr::Literal(ScalarValue::Utf8(None), None)
    }

    /// `DataFusion` compacts `ai_if(x, 'c', on_error => 'null')` to three arguments, the
    /// count `ai_if(x, 'c', 'jev')` also has; the recorded name keeps them apart.
    #[test]
    fn a_named_option_lands_in_its_own_position() {
        let canonical = canonical_args(Kind::If, &[col("x"), lit("c"), named("on_error", "NULL")])
            .expect("valid")
            .expect("rewritten");
        assert_eq!(canonical, vec![col("x"), lit("c"), null(), lit("null")]);

        let canonical = canonical_args(Kind::If, &[col("x"), lit("c"), lit("jev")])
            .expect("valid")
            .expect("rewritten");
        assert_eq!(canonical, vec![col("x"), lit("c"), lit("jev"), lit("fail")]);
    }

    /// Without the required `condition`, `ai_if(x, model => 'm')` would otherwise put the
    /// model name where the condition belongs.
    #[test]
    fn a_missing_required_argument_is_an_error_not_a_shift() {
        let err = canonical_args(Kind::If, &[col("x"), named("model", "m")])
            .expect_err("condition is missing");
        assert_eq!(
            err.strip_backtrace(),
            "Error during planning: ai_if: missing required argument `condition`. Usage: ai_if(input, condition[, model => 'name'][, on_error => 'fail' | 'null'])"
        );
    }

    #[test]
    fn canonical_arguments_are_left_alone() {
        let canonical = vec![col("x"), lit("c"), lit("jev"), lit("null")];
        assert_eq!(canonical_args(Kind::If, &canonical).expect("valid"), None);
    }

    #[test]
    fn labels_normalize_from_a_list_or_an_object() {
        let from_list = canonical_args(
            Kind::Classify,
            &[
                col("x"),
                lit(ScalarValue::List(ScalarValue::new_list_nullable(
                    &[ScalarValue::from("technical"), ScalarValue::from("billing")],
                    &arrow::datatypes::DataType::Utf8,
                ))),
            ],
        )
        .expect("valid")
        .expect("rewritten");
        // Labels keep the order they were written in.
        assert_eq!(from_list[1], lit(r#"{"technical":null,"billing":null}"#));

        let from_object = canonical_args(
            Kind::Classify,
            &[
                col("x"),
                lit(r#"{"billing": "Payments", "technical": null}"#),
            ],
        )
        .expect("valid")
        .expect("rewritten");
        assert_eq!(
            from_object[1],
            lit(r#"{"billing":"Payments","technical":null}"#)
        );
    }

    #[test]
    fn invalid_constants_name_the_argument() {
        let cases: Vec<(Kind, Vec<Expr>, &str)> = vec![
            (
                Kind::Classify,
                vec![col("x"), lit(r#"["only"]"#)],
                "Error during planning: ai_classify: `labels` must list between 2 and 255 labels; it lists 1. Include a fallback such as 'other' when no label may fit.",
            ),
            (
                Kind::Classify,
                vec![col("x"), lit(r#"["a", "a"]"#)],
                "Error during planning: ai_classify: label 'a' is listed more than once. List each label once.",
            ),
            (
                Kind::Score,
                vec![col("x"), lit("How bad?"), lit(r#"["one"]"#)],
                "Error during planning: ai_score: `levels` must list between 2 and 10 levels; it lists 1.",
            ),
            (
                Kind::If,
                vec![col("x"), col("question")],
                "Error during planning: ai_if: `condition` must be a constant string; it cannot vary by row. Usage: ai_if(input, condition[, model => 'name'][, on_error => 'fail' | 'null'])",
            ),
            (
                Kind::If,
                vec![col("x"), lit("c"), null(), lit("skip")],
                "Error during planning: ai_if: `on_error` must be 'fail' or 'null', not 'skip'. Usage: ai_if(input, condition[, model => 'name'][, on_error => 'fail' | 'null'])",
            ),
            (
                Kind::Decide,
                vec![col("x"), lit("{}")],
                "Error during planning: ai_decide: `questions` must contain at least one question.",
            ),
            (
                Kind::Classify,
                vec![col("x"), lit(r#"{"a": "First", "a": "Second", "b": null}"#)],
                "Error during planning: ai_classify: label 'a' is listed more than once. List each label once.",
            ),
            (
                Kind::Decide,
                vec![
                    col("x"),
                    lit(
                        r#"{"u": {"type": "noul", "instructions": "A?"}, "u": {"type": "noul", "instructions": "B?"}}"#,
                    ),
                ],
                "Error during planning: ai_decide: question 'u' is listed more than once. Give each question its own id.",
            ),
            (
                Kind::Decide,
                vec![
                    col("x"),
                    lit(
                        r#"{"c": {"type": "choice", "instructions": "Which?", "criteria": {"a": null, "a": "Again"}}}"#,
                    ),
                ],
                "Error during planning: ai_decide: `questions` is not a valid questions object: choice option 'a' is listed more than once at line 1 column 90. Each entry needs a `type` of 'noul', 'choice' or 'score', with `instructions` and, for choice and score, `criteria`.",
            ),
            (
                Kind::Decide,
                vec![
                    col("x"),
                    lit(
                        r#"{"c": {"type": "choice", "instructions": "Which?", "criteria": {" ": null, "b": null}}}"#,
                    ),
                ],
                "Error during planning: ai_decide: choice question 'c' has an option with an empty label in `criteria`. Give every option a label.",
            ),
            (
                Kind::Decide,
                vec![
                    col("x"),
                    lit(
                        r#"{"t": {"type": "score", "instructions": "How bad?", "criteria": ["one"]}}"#,
                    ),
                ],
                "Error during planning: ai_decide: `questions` is not a valid questions object: score criteria must contain between two and ten non-null levels at line 1 column 73. Each entry needs a `type` of 'noul', 'choice' or 'score', with `instructions` and, for choice and score, `criteria`.",
            ),
            (
                Kind::Decide,
                vec![
                    col("x"),
                    lit(
                        r#"{"t": {"type": "score", "instructions": "How bad?", "criteria": ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]}}"#,
                    ),
                ],
                "Error during planning: ai_decide: `questions` is not a valid questions object: score criteria must contain between two and ten non-null levels at line 1 column 122. Each entry needs a `type` of 'noul', 'choice' or 'score', with `instructions` and, for choice and score, `criteria`.",
            ),
            (
                Kind::If,
                vec![col("x"), lit("c"), named("modle", "m")],
                "Error during planning: ai_if: unknown argument `modle`. Usage: ai_if(input, condition[, model => 'name'][, on_error => 'fail' | 'null'])",
            ),
        ];
        for (kind, args, expected) in cases {
            let err = canonical_args(kind, &args).expect_err("invalid");
            assert_eq!(err.strip_backtrace(), expected);
        }
    }

    #[test]
    fn decide_questions_are_reserialized() {
        let canonical = canonical_args(
            Kind::Decide,
            &[
                col("x"),
                lit(r#"{ "u": {"type": "noul", "instructions": "Urgent?"} }"#),
            ],
        )
        .expect("valid")
        .expect("rewritten");
        assert_eq!(
            canonical[1],
            lit(r#"{"u":{"type":"noul","instructions":"Urgent?"}}"#)
        );
    }
}
