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
//!
//! A required constant may also be any expression with one value for the whole query,
//! such as an uncorrelated scalar subquery. `DataFusion` computes it once when the
//! query runs, so it is a [`ConstArg::Deferred`] while the query is planned, and the
//! function checks it by the same rules when it runs:
//!
//! ```text
//! ai_classify(body, <labels>)
//!                      |
//!       +--------------+----------------------+
//!       |                                     |
//!  ['a', 'b']                      (SELECT team FROM teams)
//!       |                                     |
//!  ConstArg::Known                  ConstArg::Deferred
//!  checked while planning           checked when the function runs
//! ```

use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::DataType;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::{
    Column, DataFusionError, Result, ScalarValue, plan_datafusion_err, plan_err,
};
use datafusion::functions_aggregate::expr_fn::array_agg;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprFunctionExt, LogicalPlanBuilder, ScalarFunctionArgs,
};
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

impl Layout {
    /// When the constant in `position` must be known. Required parameters come first and
    /// a call cannot skip them, so an argument in a required position fills that
    /// parameter whatever expression it is.
    pub(crate) fn resolution(&self, position: usize) -> Resolution {
        match self.params.get(position) {
            Some(&"questions") => Resolution::Literal(LiteralOnly::ResultType),
            _ if position < self.required => Resolution::PerQuery,
            _ => Resolution::Literal(LiteralOnly::Optional),
        }
    }
}

/// When the value of a constant parameter must be known.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Resolution {
    /// A literal is read while the query is planned. Any other expression with one value
    /// for the whole query, such as an uncorrelated scalar subquery, is read when the
    /// function runs. The same rules check both.
    PerQuery,
    /// Only a literal is accepted.
    Literal(LiteralOnly),
}

/// Why a parameter accepts only a literal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiteralOnly {
    /// It sets the type of the result (`ai_decide`'s `questions`), which `DataFusion`
    /// needs while it plans the query.
    ResultType,
    /// It is optional. `DataFusion` drops the optional parameters a call skips, and only
    /// a literal keeps the name it was passed by, so for any other expression the
    /// parameter it fills is not certain.
    Optional,
}

/// A constant argument of a decision call.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ConstArg {
    /// A literal, checked and normalized while the query is planned. `None` for NULL or
    /// an omitted optional argument.
    Known(Option<String>),
    /// An expression with one value for the whole query, such as an uncorrelated scalar
    /// subquery. Its value is known, and checked, only when the function runs.
    Deferred(Expr),
}

/// How a constant argument is written.
enum Written<'a> {
    /// A literal, or a list of literals: its text, `None` for NULL.
    Literal(Option<String>),
    /// Any other expression.
    Expression(&'a Expr),
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

/// A call's arguments in every position, with literal constants normalized and
/// deferred ones in the form the function reads them.
///
/// Returns `None` when `args` is already in that form, so the rule can report that it
/// changed nothing.
pub(crate) fn canonical_args(kind: Kind, args: &[Expr]) -> Result<Option<Vec<Expr>>> {
    let layout = kind.layout();
    let names: Vec<Option<String>> = args.iter().map(literal_parameter_name).collect();
    let positions = positions(kind, &names)?;
    let mut slots: Vec<Option<(usize, &Expr)>> = vec![None; layout.params.len()];
    for (index, (arg, position)) in args.iter().zip(positions).enumerate() {
        slots[position] = Some((index, arg));
    }

    let mut canonical = Vec::with_capacity(slots.len());
    for (position, slot) in slots.into_iter().enumerate() {
        let value = if position == 0 {
            // The input is per row; it is used as written.
            match slot {
                Some((_, expr)) => expr.clone(),
                None => return plan_err!("{}: missing `input`", kind.name()),
            }
        } else {
            match slot {
                Some((index, expr)) => match const_arg(kind, position, index, expr)? {
                    ConstArg::Known(text) => string_literal(text),
                    ConstArg::Deferred(expr) => expr,
                },
                None => string_literal(normalize_constant(kind, layout.params[position], None)?),
            }
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

/// Reads `expr`, passed as argument `index`, as the constant in `position`.
fn const_arg(kind: Kind, position: usize, index: usize, expr: &Expr) -> Result<ConstArg> {
    let layout = kind.layout();
    let param = layout.params[position];
    match written(kind, param, expr)? {
        Written::Literal(text) => normalize_constant(kind, param, text).map(ConstArg::Known),
        Written::Expression(expr) => match layout.resolution(position) {
            Resolution::PerQuery if is_query_constant(expr) => {
                per_query_value(param, expr).map(ConstArg::Deferred)
            }
            Resolution::PerQuery => Err(varies_by_row(kind, param)),
            Resolution::Literal(reason) => Err(literal_only(kind, param, index, reason)),
        },
    }
}

/// The text of a literal constant: a string, or — for `labels` and `levels` — a list of
/// strings, given as its JSON.
fn written<'a>(kind: Kind, param: &str, expr: &'a Expr) -> Result<Written<'a>> {
    match expr {
        Expr::Literal(scalar, _) => scalar_constant(kind, param, scalar).map(Written::Literal),
        // `['a', 'b']` is planned as `make_array('a', 'b')` until constants are folded.
        Expr::ScalarFunction(function)
            if function.func.name() == "make_array" && is_list_param(param) =>
        {
            let mut items = Vec::with_capacity(function.args.len());
            for item in &function.args {
                let Expr::Literal(scalar, _) = item else {
                    return Ok(Written::Expression(expr));
                };
                match scalar_constant(kind, param, scalar)? {
                    Some(text) => items.push(Value::String(text)),
                    None => return Err(contains_null(kind, param)),
                }
            }
            Ok(Written::Literal(Some(Value::Array(items).to_string())))
        }
        _ => Ok(Written::Expression(expr)),
    }
}

/// Whether `expr` has one value for the whole query: it reads no column, calls no
/// volatile or async function, and every subquery in it is uncorrelated.
pub(crate) fn is_query_constant(expr: &Expr) -> bool {
    let varies = expr.exists(|e| {
        Ok(match e {
            Expr::Literal(..)
            | Expr::Alias(_)
            | Expr::Cast(_)
            | Expr::TryCast(_)
            | Expr::BinaryExpr(_)
            | Expr::Case(_)
            | Expr::Not(_)
            | Expr::Negative(_)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_) => false,
            Expr::ScalarSubquery(subquery) => !subquery.outer_ref_columns.is_empty(),
            Expr::ScalarFunction(function) => {
                e.is_volatile_node() || function.func.as_async().is_some()
            }
            _ => true,
        })
    });
    matches!(varies, Ok(false))
}

/// A deferred constant in the form the function reads it. A subquery for `labels` that
/// returns one label per row (`SELECT DISTINCT team FROM teams`) is collected into one
/// sorted list, since a scalar subquery must return at most one row. `levels` are
/// ordered, and a subquery does not keep the order of its rows, so a subquery for
/// `levels` must return one list itself, such as `array_agg(level ORDER BY rank)`.
fn per_query_value(param: &str, expr: &Expr) -> Result<Expr> {
    let Expr::ScalarSubquery(subquery) = expr else {
        return Ok(expr.clone());
    };
    let schema = subquery.subquery.schema();
    if param != "labels" || schema.fields().len() != 1 || is_list(schema.field(0).data_type()) {
        return Ok(expr.clone());
    }
    let rows = Expr::Column(Column::from(schema.qualified_field(0)));
    let collected = array_agg(rows.clone())
        .order_by(vec![rows.sort(true, false)])
        .build()?;
    let plan = LogicalPlanBuilder::new_from_arc(Arc::clone(&subquery.subquery))
        .aggregate(Vec::<Expr>::new(), vec![collected])?
        .build()?;
    Ok(Expr::ScalarSubquery(subquery.with_plan(Arc::new(plan))))
}

fn is_list(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
    )
}

fn is_list_param(param: &str) -> bool {
    matches!(param, "labels" | "levels")
}

/// A literal that fits `param`, for its error messages.
fn example(param: &str) -> &'static str {
    match param {
        "labels" => " such as ['billing', 'technical']",
        "levels" => " such as ['low', 'medium', 'high']",
        "questions" => {
            r#" such as '{"urgent": {"type": "noul", "instructions": "Is this urgent?"}}'"#
        }
        "on_error" => " ('fail' or 'null')",
        _ => " string",
    }
}

/// A per-query parameter got an expression that can change from row to row.
fn varies_by_row(kind: Kind, param: &str) -> DataFusionError {
    plan_datafusion_err!(
        "{}: `{param}` must have one value for the whole query: a literal{}, or a subquery that does not refer to the outer query. This argument can change from row to row, because it reads a column or refers to the outer query. Usage: {}",
        kind.name(),
        example(param),
        kind.usage()
    )
}

/// A literal-only parameter got another expression.
fn literal_only(kind: Kind, param: &str, index: usize, reason: LiteralOnly) -> DataFusionError {
    match reason {
        LiteralOnly::ResultType => plan_datafusion_err!(
            "{}: `{param}` must be a literal{}, because it sets the type of the result. Usage: {}",
            kind.name(),
            example(param),
            kind.usage()
        ),
        LiteralOnly::Optional => plan_datafusion_err!(
            "{}: argument {} is not a literal. Optional arguments such as `model` and `on_error` must be literals, because only for a literal can Spice tell which parameter it fills. Usage: {}",
            kind.name(),
            index + 1,
            kind.usage()
        ),
    }
}

/// A constant has a value of the wrong type.
fn wrong_type(kind: Kind, param: &str, found: impl fmt::Display) -> DataFusionError {
    let expected = match param {
        "labels" => "a list of strings, or a string that holds a JSON list or object",
        "levels" => "a list of strings, or a string that holds a JSON list",
        "questions" => "a string that holds a JSON object",
        _ => "a string",
    };
    plan_datafusion_err!(
        "{}: `{param}` must be {expected}, not {found}. Usage: {}",
        kind.name(),
        kind.usage()
    )
}

fn contains_null(kind: Kind, param: &str) -> DataFusionError {
    plan_datafusion_err!(
        "{}: `{param}` cannot contain NULL. Usage: {}",
        kind.name(),
        kind.usage()
    )
}

/// The text of a constant value: a string, or — for `labels` and `levels` — a list of
/// strings, given as its JSON. `None` for NULL.
fn scalar_constant(kind: Kind, param: &str, scalar: &ScalarValue) -> Result<Option<String>> {
    if scalar.is_null() {
        return Ok(None);
    }
    match scalar {
        ScalarValue::Utf8(Some(text))
        | ScalarValue::LargeUtf8(Some(text))
        | ScalarValue::Utf8View(Some(text)) => Ok(Some(text.clone())),
        ScalarValue::Dictionary(_, value) => scalar_constant(kind, param, value),
        ScalarValue::List(list) if is_list_param(param) => {
            list_constant(kind, param, &list.value(0))
        }
        ScalarValue::LargeList(list) if is_list_param(param) => {
            list_constant(kind, param, &list.value(0))
        }
        ScalarValue::FixedSizeList(list) if is_list_param(param) => {
            list_constant(kind, param, &list.value(0))
        }
        other => Err(wrong_type(kind, param, other.data_type())),
    }
}

fn list_constant(
    kind: Kind,
    param: &str,
    values: &arrow::array::ArrayRef,
) -> Result<Option<String>> {
    let values = arrow::compute::cast(values, &DataType::Utf8)
        .map_err(|_| wrong_type(kind, param, format!("a list of {}", values.data_type())))?;
    let strings = values.as_string::<i32>();
    let mut items = Vec::with_capacity(strings.len());
    for item in strings {
        match item {
            Some(text) => items.push(Value::String(text.to_string())),
            None => return Err(contains_null(kind, param)),
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
    match scalar
        .map(|s| scalar_constant(kind, "questions", s))
        .transpose()?
    {
        Some(text) => parse_questions(kind, &required_text(kind, "questions", text)?),
        None => Err(literal_only(kind, "questions", 1, LiteralOnly::ResultType)),
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
/// before anything runs. Only literals have a value at this point. A list written as
/// `['a', 'b']` is not a literal yet, and a per-query parameter can also be a subquery,
/// so the planner rule checks every other argument of a per-query parameter.
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
            None => match layout.resolution(position) {
                Resolution::PerQuery => {}
                Resolution::Literal(reason) => {
                    return Err(literal_only(kind, param, index, reason));
                }
            },
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
    /// Reads the constants of an invocation.
    pub(crate) fn from_invocation(kind: Kind, args: &ScalarFunctionArgs) -> Result<Self> {
        let names: Vec<Option<String>> = args
            .arg_fields
            .iter()
            .map(|field| field.metadata().get(PARAMETER_NAME_KEY).cloned())
            .collect();
        Self::from_values(kind, &args.args, &names)
    }

    /// Reads constants from their values. A call the planner rule built has every
    /// argument in its position; any other is placed by `names`, the parameter names
    /// recorded on its argument fields.
    ///
    /// A deferred constant is checked here for the first time, by the rules that check a
    /// literal while the query is planned, so its errors have the same text. They are
    /// execution errors, as the query is running.
    pub(crate) fn from_values(
        kind: Kind,
        values: &[ColumnarValue],
        names: &[Option<String>],
    ) -> Result<Self> {
        Self::read_values(kind, values, names).map_err(|e| match e {
            DataFusionError::Plan(message) => DataFusionError::Execution(message),
            other => other,
        })
    }

    fn read_values(kind: Kind, values: &[ColumnarValue], names: &[Option<String>]) -> Result<Self> {
        let layout = kind.layout();
        let positions: Vec<usize> = if values.len() == layout.params.len() {
            (0..layout.params.len()).collect()
        } else {
            positions(kind, names)?
        };

        let mut constants: Vec<Option<String>> = vec![None; layout.params.len()];
        for (value, position) in values.iter().zip(positions) {
            if position == 0 {
                continue;
            }
            let param = layout.params[position];
            let text = match value {
                ColumnarValue::Scalar(scalar) => scalar_constant(kind, param, scalar)?,
                ColumnarValue::Array(_) => return Err(varies_by_row(kind, param)),
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
                    .ok_or_else(|| literal_only(kind, "questions", 1, LiteralOnly::ResultType))?,
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

/// The constants of a call whose arguments are already in every position, as
/// [`canonical_args`] leaves them. The input, in position 0, is `Known(None)`.
pub(crate) fn canonical_constants(kind: Kind, args: &[Expr]) -> Result<Vec<ConstArg>> {
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
                Ok(ConstArg::Known(None))
            } else {
                const_arg(kind, position, position, arg)
            }
        })
        .collect()
}

/// The text of every constant, when all are known while the query is planned.
pub(crate) fn known_constants(constants: Vec<ConstArg>) -> Option<Vec<Option<String>>> {
    constants
        .into_iter()
        .map(|constant| match constant {
            ConstArg::Known(text) => Some(text),
            ConstArg::Deferred(_) => None,
        })
        .collect()
}

/// Whether `args` are a typed call as the planner rule leaves it to run on its own: in
/// every position, with a constant known only when the query runs.
pub(crate) fn runs_with_deferred_constants(kind: Kind, args: &[Expr]) -> bool {
    kind != Kind::Decide
        && matches!(canonical_args(kind, args), Ok(None))
        && canonical_constants(kind, args).is_ok_and(|c| known_constants(c).is_none())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, StringArray};
    use datafusion::common::Spans;
    use datafusion::logical_expr::{LogicalPlan, Subquery, col, lit, out_ref_col, scalar_subquery};
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
                "Error during planning: ai_if: `condition` must have one value for the whole query: a literal string, or a subquery that does not refer to the outer query. This argument can change from row to row, because it reads a column or refers to the outer query. Usage: ai_if(input, condition[, model => 'name'][, on_error => 'fail' | 'null'])",
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
                Kind::Decide,
                vec![
                    col("x"),
                    lit(
                        r#"{"t": {"type": "score", "instructions": "How bad?", "criteria": ["low", null]}}"#,
                    ),
                ],
                "Error during planning: ai_decide: `questions` is not a valid questions object: a score level must be a string, an object, or an array, not null at line 1 column 79. Each entry needs a `type` of 'noul', 'choice' or 'score', with `instructions` and, for choice and score, `criteria`.",
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

    /// A plan returning one `team` row per name, for subqueries.
    fn teams(names: &[&str]) -> Arc<LogicalPlan> {
        let rows = names.iter().map(|name| vec![lit(*name)]).collect();
        Arc::new(
            LogicalPlanBuilder::values(rows)
                .expect("values")
                .build()
                .expect("plan"),
        )
    }

    /// `labels` from a subquery are deferred to when the function runs. One label per
    /// row is collected into one list, so the subquery can return many rows.
    #[test]
    fn labels_from_a_subquery_are_deferred_and_collected() {
        let canonical = canonical_args(
            Kind::Classify,
            &[col("x"), scalar_subquery(teams(&["billing", "technical"]))],
        )
        .expect("valid")
        .expect("rewritten");
        assert!(
            matches!(
                &canonical[1],
                Expr::ScalarSubquery(subquery)
                    if matches!(subquery.subquery.as_ref(), LogicalPlan::Aggregate(_))
                        && is_list(subquery.subquery.schema().field(0).data_type())
            ),
            "labels are collected into one list: {}",
            canonical[1]
        );
        assert_eq!(canonical[2], null(), "no instructions");

        let constants = canonical_constants(Kind::Classify, &canonical).expect("valid");
        assert!(matches!(constants[1], ConstArg::Deferred(_)));
        assert_eq!(known_constants(constants), None);
        // The rule leaves the call as it is on the next optimizer pass.
        assert_eq!(
            canonical_args(Kind::Classify, &canonical).expect("valid"),
            None
        );
        assert!(runs_with_deferred_constants(Kind::Classify, &canonical));
    }

    /// Levels are ordered and a subquery's rows are not, so a `levels` subquery is used
    /// as written; it must return one list.
    #[test]
    fn levels_from_a_subquery_are_not_collected() {
        let levels = scalar_subquery(teams(&["calm", "furious"]));
        let canonical = canonical_args(Kind::Score, &[col("x"), lit("How upset?"), levels.clone()])
            .expect("valid")
            .expect("rewritten");
        assert_eq!(canonical[2], levels);
        assert!(runs_with_deferred_constants(Kind::Score, &canonical));
    }

    #[test]
    fn literal_constants_are_known_while_planning() {
        let canonical = canonical_args(Kind::If, &[col("x"), lit("refund")])
            .expect("valid")
            .expect("rewritten");
        let constants = canonical_constants(Kind::If, &canonical).expect("valid");
        assert_eq!(
            known_constants(constants),
            Some(vec![
                None,
                Some("refund".to_string()),
                None,
                Some("fail".to_string())
            ])
        );
        assert!(!runs_with_deferred_constants(Kind::If, &canonical));
    }

    #[test]
    fn constants_that_are_not_one_value_per_query_are_refused() {
        let correlated = Expr::ScalarSubquery(Subquery {
            subquery: teams(&["billing", "technical"]),
            outer_ref_columns: vec![out_ref_col(DataType::Utf8, "tickets.team")],
            spans: Spans::new(),
        });
        let labels_vary = "Error during planning: ai_classify: `labels` must have one value for the whole query: a literal such as ['billing', 'technical'], or a subquery that does not refer to the outer query. This argument can change from row to row, because it reads a column or refers to the outer query. Usage: ai_classify(input, labels[, instructions => '...'][, model => 'name'][, on_error => 'fail' | 'null'])";
        let cases: Vec<(Kind, Vec<Expr>, &str)> = vec![
            (Kind::Classify, vec![col("x"), col("labels")], labels_vary),
            (Kind::Classify, vec![col("x"), correlated], labels_vary),
            (
                Kind::If,
                vec![col("x"), lit("refund"), scalar_subquery(teams(&["jev"]))],
                "Error during planning: ai_if: argument 3 is not a literal. Optional arguments such as `model` and `on_error` must be literals, because only for a literal can Spice tell which parameter it fills. Usage: ai_if(input, condition[, model => 'name'][, on_error => 'fail' | 'null'])",
            ),
            (
                Kind::If,
                vec![col("x"), lit(42_i64)],
                "Error during planning: ai_if: `condition` must be a string, not Int64. Usage: ai_if(input, condition[, model => 'name'][, on_error => 'fail' | 'null'])",
            ),
            (
                Kind::Decide,
                vec![col("x"), scalar_subquery(teams(&["{}"]))],
                r#"Error during planning: ai_decide: `questions` must be a literal such as '{"urgent": {"type": "noul", "instructions": "Is this urgent?"}}', because it sets the type of the result. Usage: ai_decide(input, questions[, model => 'name'][, on_error => 'fail' | 'null'])"#,
            ),
        ];
        for (kind, args, expected) in cases {
            let err = canonical_args(kind, &args).expect_err("invalid");
            assert_eq!(err.strip_backtrace(), expected);
        }
    }

    fn text_input() -> ColumnarValue {
        ColumnarValue::Array(Arc::new(StringArray::from(vec!["refund please"])) as ArrayRef)
    }

    fn label_list(labels: &[Option<&str>]) -> ColumnarValue {
        let items: Vec<ScalarValue> = labels
            .iter()
            .map(|label| ScalarValue::Utf8(label.map(ToString::to_string)))
            .collect();
        ColumnarValue::Scalar(ScalarValue::List(ScalarValue::new_list_nullable(
            &items,
            &DataType::Utf8,
        )))
    }

    /// A deferred constant reaches the function as a value, which is checked by the
    /// rules a literal is checked by while planning, with the same text.
    #[test]
    fn deferred_constants_are_checked_when_the_function_runs() {
        let call = CallArgs::from_values(
            Kind::Classify,
            &[
                text_input(),
                label_list(&[Some("billing"), Some("technical")]),
            ],
            &[None, None],
        )
        .expect("valid labels");
        assert!(matches!(
            call.questions.get(TYPED_QUESTION_ID),
            Some(Question::Choice { criteria, .. }) if criteria.len() == 2
        ));

        let too_many: Vec<String> = (0..256).map(|i| format!("label {i}")).collect();
        let too_many: Vec<Option<&str>> = too_many.iter().map(|l| Some(l.as_str())).collect();
        let usage = "Usage: ai_classify(input, labels[, instructions => '...'][, model => 'name'][, on_error => 'fail' | 'null'])";
        let cases: Vec<(Kind, ColumnarValue, String)> = vec![
            (
                Kind::Classify,
                label_list(&[Some("billing")]),
                "Execution error: ai_classify: `labels` must list between 2 and 255 labels; it lists 1. Include a fallback such as 'other' when no label may fit.".to_string(),
            ),
            (
                Kind::Classify,
                label_list(&too_many),
                "Execution error: ai_classify: `labels` must list between 2 and 255 labels; it lists 256. Include a fallback such as 'other' when no label may fit.".to_string(),
            ),
            (
                Kind::Classify,
                label_list(&[Some("billing"), Some("billing")]),
                "Execution error: ai_classify: label 'billing' is listed more than once. List each label once.".to_string(),
            ),
            (
                Kind::Classify,
                label_list(&[Some("billing"), None]),
                format!("Execution error: ai_classify: `labels` cannot contain NULL. {usage}"),
            ),
            // A subquery that returns no row is NULL.
            (
                Kind::Classify,
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                format!("Execution error: ai_classify: `labels` is required and cannot be empty or NULL. {usage}"),
            ),
            (
                Kind::Classify,
                ColumnarValue::Scalar(ScalarValue::Int64(Some(7))),
                format!("Execution error: ai_classify: `labels` must be a list of strings, or a string that holds a JSON list or object, not Int64. {usage}"),
            ),
            (
                Kind::Classify,
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["billing"])) as ArrayRef),
                format!("Execution error: ai_classify: `labels` must have one value for the whole query: a literal such as ['billing', 'technical'], or a subquery that does not refer to the outer query. This argument can change from row to row, because it reads a column or refers to the outer query. {usage}"),
            ),
            (
                Kind::If,
                ColumnarValue::Scalar(ScalarValue::Int64(Some(7))),
                "Execution error: ai_if: `condition` must be a string, not Int64. Usage: ai_if(input, condition[, model => 'name'][, on_error => 'fail' | 'null'])".to_string(),
            ),
        ];
        for (kind, value, expected) in cases {
            let err = CallArgs::from_values(kind, &[text_input(), value], &[None, None])
                .expect_err("invalid");
            assert_eq!(err.strip_backtrace(), expected);
        }
    }
}
