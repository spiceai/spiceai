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

//! Translation of `DataFusion` filters into `DynamoDB` condition expressions.
//!
//! `DynamoDB` evaluates a condition against an item's attributes, while
//! `DataFusion` evaluates the SQL predicate against the row
//! [`crate::arrow::dynamodb_items_to_arrow`] converts the item into. The two
//! disagree wherever the conversion does not carry an attribute over as it is:
//! a map in a `Utf8` column becomes JSON, a number that does not parse as the
//! column's type becomes NULL, a timestamp is stored as a string in whatever
//! offset it was written with. A condition is therefore reported exact only
//! where `DynamoDB` evaluates it as SQL does on every item, and otherwise
//! widened to a superset that `DataFusion` filters again — never narrowed,
//! which would drop rows the query should return.

use std::cmp::Ordering;
use std::collections::HashMap;

use arrow::datatypes::{DataType, TimeUnit};
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{AttributeValue, ScalarAttributeType};
use chrono::{DateTime, Datelike, NaiveDate, TimeDelta};
use datafusion::functions::string::starts_with::StartsWithFunc;
use datafusion::logical_expr::expr::{Between, InList, Like, ScalarFunction};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::scalar::ScalarValue;
use util::time_format::format_datetime;

use crate::table_schema::DynamoDBTableSchema;

/// The most operands `DynamoDB` accepts in an `IN` list.
const MAX_IN_OPERANDS: usize = 100;

/// The longest expression `DynamoDB` accepts, in bytes.
pub(crate) const MAX_EXPRESSION_BYTES: usize = 4096;

/// A bound on the UTC offset of a timestamp string the conversion reads, in
/// milliseconds: chrono accepts any offset under a day, beyond the ±14 hours
/// time zones use.
const MAX_OFFSET_MILLIS: i64 = 24 * 60 * 60 * 1_000;

/// A request's expression attribute names, by placeholder.
pub(crate) type AttributeNames = HashMap<String, String>;

/// A request's expression attribute values, by placeholder.
pub(crate) type AttributeValues = HashMap<String, AttributeValue>;

/// Expression attribute names and values for one request. `DynamoDB` refuses
/// a request that defines a placeholder its expressions do not use, so they are
/// allocated as the expressions are written, and a translation that fails part
/// way rolls its own back.
#[derive(Debug, Default)]
pub(crate) struct Placeholders {
    /// Placeholder and attribute name segment, in allocation order.
    names: Vec<(String, String)>,
    values: Vec<(String, AttributeValue)>,
    /// `attribute_type` operands already allocated, by type name.
    types: Vec<(&'static str, String)>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Checkpoint {
    names: usize,
    values: usize,
    types: usize,
}

impl Placeholders {
    /// The expression path of a column: each segment of a flattened column,
    /// otherwise the whole name. Every segment gets a generated placeholder, so
    /// any attribute name is expressible, including one with a space or a `-`.
    pub(crate) fn path(&mut self, schema: &DynamoDBTableSchema, column: &str) -> String {
        if schema.is_flattened_field(column) {
            column
                .split('.')
                .map(|segment| self.name(segment))
                .collect::<Vec<_>>()
                .join(".")
        } else {
            self.name(column)
        }
    }

    pub(crate) fn name(&mut self, segment: &str) -> String {
        if let Some((placeholder, _)) = self.names.iter().find(|(_, s)| s == segment) {
            return placeholder.clone();
        }
        let placeholder = format!("#n{}", self.names.len());
        self.names.push((placeholder.clone(), segment.to_string()));
        placeholder
    }

    pub(crate) fn value(&mut self, value: AttributeValue) -> String {
        let placeholder = format!(":v{}", self.values.len());
        self.values.push((placeholder.clone(), value));
        placeholder
    }

    /// The value naming a `DynamoDB` type for `attribute_type`.
    fn type_name(&mut self, name: &'static str) -> String {
        if let Some((_, placeholder)) = self.types.iter().find(|(t, _)| *t == name) {
            return placeholder.clone();
        }
        let placeholder = self.value(AttributeValue::S(name.to_string()));
        self.types.push((name, placeholder.clone()));
        placeholder
    }

    pub(crate) fn checkpoint(&self) -> Checkpoint {
        Checkpoint {
            names: self.names.len(),
            values: self.values.len(),
            types: self.types.len(),
        }
    }

    pub(crate) fn rollback(&mut self, checkpoint: Checkpoint) {
        self.names.truncate(checkpoint.names);
        self.values.truncate(checkpoint.values);
        self.types.truncate(checkpoint.types);
    }

    pub(crate) fn into_parts(self) -> (Option<AttributeNames>, Option<AttributeValues>) {
        (
            (!self.names.is_empty()).then(|| self.names.into_iter().collect()),
            (!self.values.is_empty()).then(|| self.values.into_iter().collect()),
        )
    }
}

/// A condition expression translated from a SQL predicate.
#[derive(Debug, Clone)]
pub(crate) struct Condition {
    pub(crate) expression: String,
    /// Whether the expression selects exactly the rows the predicate keeps;
    /// otherwise a superset of them.
    pub(crate) exact: bool,
    /// Whether the expression reads a primary-key attribute, which a Query's
    /// filter expression may not.
    pub(crate) reads_key: bool,
}

impl Condition {
    fn new(expression: String, exact: bool, reads_key: bool) -> Self {
        Self {
            expression,
            exact,
            reads_key,
        }
    }

    /// The most bytes `expression` can take in a request's filter expression,
    /// joined to the others: a request numbers its placeholders across every
    /// filter, which lengthens one written as `#n0` to at most `#n999`.
    fn request_bytes(&self) -> usize {
        let placeholders =
            self.expression.matches("#n").count() + self.expression.matches(":v").count();
        self.expression.len() + 2 * placeholders + " AND ".len()
    }
}

/// A predicate on a primary-key attribute that a Query states, either in its
/// key condition or by checking each item it reads.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum KeyPredicate {
    /// The partition key is one of these values.
    Partition(Vec<AttributeValue>),
    /// The partition key is none of these values.
    PartitionExcept(Vec<AttributeValue>),
    Sort(SortPredicate),
}

/// A predicate on the sort key.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SortPredicate {
    Eq(AttributeValue),
    /// At or above the value when inclusive, above it otherwise.
    Lower(AttributeValue, bool),
    /// At or below the value when inclusive, below it otherwise.
    Upper(AttributeValue, bool),
    Between(AttributeValue, AttributeValue),
    Prefix(String),
    /// One of these values. No key condition states it; it bounds one, and is
    /// checked item by item.
    OneOf(Vec<AttributeValue>),
    /// None of these values, checked item by item.
    Except(Vec<AttributeValue>),
}

/// How the column a sort key is read into compares the key's values, which a
/// check of an item's key against a predicate has to follow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum KeyReading {
    /// As stored: a string or a binary by its bytes, a number by its value.
    #[default]
    Stored,
    /// As an `Int64` column reads a number: its value when that is an `i64`,
    /// and NULL otherwise, which meets no predicate.
    Integer,
}

impl KeyReading {
    pub(crate) fn of(data_type: &DataType) -> Self {
        if *data_type == DataType::Int64 {
            Self::Integer
        } else {
            Self::Stored
        }
    }
}

/// Whether the key value `v`, read as `reading` says, meets `predicate`, when
/// that can be decided.
pub(crate) fn satisfies(
    v: &AttributeValue,
    predicate: &SortPredicate,
    reading: KeyReading,
) -> Option<bool> {
    use SortPredicate::{Between, Eq, Except, Lower, OneOf, Prefix, Upper};
    if reading == KeyReading::Integer
        && !matches!(v, AttributeValue::N(n) if n.parse::<i64>().is_ok())
    {
        return Some(false);
    }
    Some(match predicate {
        Eq(w) => compare(v, w)? == Ordering::Equal,
        Lower(w, inclusive) => match compare(v, w)? {
            Ordering::Greater => true,
            Ordering::Equal => *inclusive,
            Ordering::Less => false,
        },
        Upper(w, inclusive) => match compare(v, w)? {
            Ordering::Less => true,
            Ordering::Equal => *inclusive,
            Ordering::Greater => false,
        },
        Between(low, high) => {
            compare(v, low)? != Ordering::Less && compare(v, high)? != Ordering::Greater
        }
        Prefix(p) => match v {
            AttributeValue::S(s) => s.starts_with(p.as_str()),
            _ => return None,
        },
        OneOf(values) => {
            for w in values {
                if compare(v, w)? == Ordering::Equal {
                    return Some(true);
                }
            }
            false
        }
        Except(values) => !satisfies(v, &OneOf(values.clone()), reading)?,
    })
}

/// Orders two key values as `DynamoDB` does: strings and binaries by their
/// bytes, numbers by value.
pub(crate) fn compare(a: &AttributeValue, b: &AttributeValue) -> Option<Ordering> {
    match (a, b) {
        (AttributeValue::S(a), AttributeValue::S(b)) => Some(a.as_bytes().cmp(b.as_bytes())),
        (AttributeValue::B(a), AttributeValue::B(b)) => Some(a.as_ref().cmp(b.as_ref())),
        (AttributeValue::N(a), AttributeValue::N(b)) => {
            match (a.parse::<i128>(), b.parse::<i128>()) {
                (Ok(a), Ok(b)) => Some(a.cmp(&b)),
                _ => a.parse::<f64>().ok()?.partial_cmp(&b.parse::<f64>().ok()?),
            }
        }
        _ => None,
    }
}

/// The least string above every string beginning with `prefix`, when there is
/// one: `prefix` with its last character incremented.
pub(crate) fn prefix_end(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        let next = (u32::from(last) + 1..=u32::from(char::MAX)).find_map(char::from_u32);
        if let Some(next) = next {
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

/// How a column's values are produced from its attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// A string as itself, a map as JSON.
    Utf8,
    /// A number that parses as an `i64`.
    Int64,
    /// A number, parsed as the nearest `f64`.
    Float64,
    Boolean,
    Binary,
    /// A string that parses with the table's `time_format`.
    Timestamp,
    /// A `YYYY-MM-DD` string.
    Date32,
}

impl Kind {
    fn of(data_type: &DataType) -> Option<Self> {
        Some(match data_type {
            DataType::Utf8 => Self::Utf8,
            DataType::Int64 => Self::Int64,
            DataType::Float64 => Self::Float64,
            DataType::Boolean => Self::Boolean,
            DataType::Binary => Self::Binary,
            DataType::Timestamp(TimeUnit::Millisecond, _) => Self::Timestamp,
            DataType::Date32 => Self::Date32,
            _ => return None,
        })
    }

    /// Whether an attribute of `key_type` produces values of this kind.
    fn fits_key(self, key_type: &ScalarAttributeType) -> bool {
        matches!(
            (self, key_type),
            (
                Self::Utf8 | Self::Timestamp | Self::Date32,
                ScalarAttributeType::S
            ) | (Self::Int64 | Self::Float64, ScalarAttributeType::N)
                | (Self::Binary, ScalarAttributeType::B)
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyRole {
    Partition,
    Sort,
}

/// A column resolved to the attribute it is read from.
struct Attribute<'a> {
    name: &'a str,
    kind: Kind,
    key: Option<KeyRole>,
}

impl Attribute<'_> {
    /// A `Utf8` key attribute is a string, so a string comparison on it is
    /// exact; any other `Utf8` attribute may hold a map, which the conversion
    /// renders as JSON a string comparison cannot select.
    fn is_string_key(&self) -> bool {
        self.key.is_some() && self.kind == Kind::Utf8
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cmp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl Cmp {
    fn from_operator(op: Operator) -> Option<Self> {
        Some(match op {
            Operator::Eq => Self::Eq,
            Operator::NotEq => Self::NotEq,
            Operator::Lt => Self::Lt,
            Operator::LtEq => Self::LtEq,
            Operator::Gt => Self::Gt,
            Operator::GtEq => Self::GtEq,
            _ => return None,
        })
    }

    fn swapped(self) -> Self {
        match self {
            Self::Lt => Self::Gt,
            Self::LtEq => Self::GtEq,
            Self::Gt => Self::Lt,
            Self::GtEq => Self::LtEq,
            other => other,
        }
    }

    fn symbol(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::NotEq => "<>",
            Self::Lt => "<",
            Self::LtEq => "<=",
            Self::Gt => ">",
            Self::GtEq => ">=",
        }
    }
}

/// Translates filters over one table.
pub(crate) struct Translator<'a> {
    schema: &'a DynamoDBTableSchema,
}

impl<'a> Translator<'a> {
    pub(crate) fn new(schema: &'a DynamoDBTableSchema) -> Self {
        Self { schema }
    }

    /// How each of `filters` can be pushed down together. `DynamoDB` refuses
    /// a filter expression longer than 4 KB, so once the filters taken fill
    /// one, the rest are left to `DataFusion`.
    pub(crate) fn classify(&self, filters: &[&Expr]) -> Vec<TableProviderFilterPushDown> {
        let mut available = MAX_EXPRESSION_BYTES;
        filters
            .iter()
            .map(
                |expr| match self.condition(expr, &mut Placeholders::default()) {
                    Some(condition) if condition.request_bytes() <= available => {
                        available -= condition.request_bytes();
                        if condition.exact {
                            TableProviderFilterPushDown::Exact
                        } else {
                            TableProviderFilterPushDown::Inexact
                        }
                    }
                    _ => TableProviderFilterPushDown::Unsupported,
                },
            )
            .collect()
    }

    /// The filter expression selecting the rows `expr` keeps, or a superset.
    /// Allocates placeholders in `out` only when it succeeds.
    pub(crate) fn condition(&self, expr: &Expr, out: &mut Placeholders) -> Option<Condition> {
        let checkpoint = out.checkpoint();
        let condition = self.translate(expr, out);
        if condition.is_none() {
            out.rollback(checkpoint);
        }
        condition
    }

    fn translate(&self, expr: &Expr, out: &mut Placeholders) -> Option<Condition> {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
                Operator::And => match (self.condition(left, out), self.condition(right, out)) {
                    (Some(l), Some(r)) => Some(Condition::new(
                        format!("({} AND {})", l.expression, r.expression),
                        l.exact && r.exact,
                        l.reads_key || r.reads_key,
                    )),
                    // A conjunction keeps a subset of either side's rows.
                    (Some(one), None) | (None, Some(one)) => {
                        Some(Condition::new(one.expression, false, one.reads_key))
                    }
                    (None, None) => None,
                },
                Operator::Or => {
                    // A NULL disjunct is never true, so the other side keeps the
                    // same rows. Under a `NOT` it would not, which is why that
                    // side is reported inexact.
                    if is_null_predicate(right) || is_null_predicate(left) {
                        let rest = if is_null_predicate(right) {
                            left
                        } else {
                            right
                        };
                        let rest = self.condition(rest, out)?;
                        return Some(Condition::new(rest.expression, false, rest.reads_key));
                    }
                    let l = self.condition(left, out)?;
                    let r = self.condition(right, out)?;
                    Some(Condition::new(
                        format!("({} OR {})", l.expression, r.expression),
                        l.exact && r.exact,
                        l.reads_key || r.reads_key,
                    ))
                }
                op => {
                    let cmp = Cmp::from_operator(*op)?;
                    match (self.attribute(left), self.attribute(right)) {
                        (Some(attribute), None) => {
                            let condition = self.compare(&attribute, cmp, literal(right)?, out);
                            self.flattened(&attribute, condition, out)
                        }
                        (None, Some(attribute)) => {
                            let condition =
                                self.compare(&attribute, cmp.swapped(), literal(left)?, out);
                            self.flattened(&attribute, condition, out)
                        }
                        (Some(l), Some(r)) => {
                            let condition = self.compare_attributes(&l, cmp, &r, out);
                            let condition = self.flattened(&l, condition, out);
                            self.flattened(&r, condition, out)
                        }
                        (None, None) => None,
                    }
                }
            },
            // A boolean column used as a predicate is true exactly when it holds `true`.
            Expr::Column(_) => {
                let attribute = self.attribute(expr)?;
                let condition = (attribute.kind == Kind::Boolean)
                    .then(|| {
                        self.compare(&attribute, Cmp::Eq, &ScalarValue::Boolean(Some(true)), out)
                    })
                    .flatten();
                self.flattened(&attribute, condition, out)
            }
            Expr::Not(inner) => {
                if let Some(attribute) = self.attribute(inner).filter(|a| a.kind == Kind::Boolean) {
                    let condition =
                        self.compare(&attribute, Cmp::Eq, &ScalarValue::Boolean(Some(false)), out);
                    return self.flattened(&attribute, condition, out);
                }
                // `NOT` is two-valued in DynamoDB: it keeps an item whose inner
                // condition is false for a missing attribute, which SQL keeps
                // out as NULL. That is a superset only of an exact inner.
                let inner = self.condition(inner, out).filter(|inner| inner.exact)?;
                Some(Condition::new(
                    negation(&inner.expression),
                    false,
                    inner.reads_key,
                ))
            }
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                let attribute = self.attribute(inner)?;
                let condition = self.null_test(&attribute, matches!(expr, Expr::IsNull(_)), out);
                self.flattened(&attribute, condition, out)
            }
            Expr::IsTrue(inner)
            | Expr::IsFalse(inner)
            | Expr::IsNotTrue(inner)
            | Expr::IsNotFalse(inner) => {
                let attribute = self.attribute(inner).filter(|a| a.kind == Kind::Boolean)?;
                let value = matches!(expr, Expr::IsTrue(_) | Expr::IsNotTrue(_));
                let holds =
                    self.compare(&attribute, Cmp::Eq, &ScalarValue::Boolean(Some(value)), out)?;
                let condition = if matches!(expr, Expr::IsTrue(_) | Expr::IsFalse(_)) {
                    holds
                } else {
                    // `IS NOT TRUE` holds for NULL too, which a missing attribute
                    // gets from DynamoDB's two-valued `NOT`.
                    Condition::new(negation(&holds.expression), holds.exact, holds.reads_key)
                };
                self.flattened(&attribute, Some(condition), out)
            }
            Expr::Between(Between {
                expr,
                negated: false,
                low,
                high,
            }) => {
                let attribute = self.attribute(expr)?;
                let condition = self.between(&attribute, literal(low)?, literal(high)?, out);
                self.flattened(&attribute, condition, out)
            }
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => {
                let attribute = self.attribute(expr)?;
                let values = list.iter().map(literal).collect::<Option<Vec<_>>>()?;
                let condition = self.in_list(&attribute, &values, *negated, out);
                self.flattened(&attribute, condition, out)
            }
            Expr::Like(like) => {
                let attribute = self.attribute(&like.expr)?;
                let prefix = like_prefix(like)?;
                let condition = self.begins_with(&attribute, &prefix, like.negated, out);
                self.flattened(&attribute, condition, out)
            }
            Expr::ScalarFunction(ScalarFunction { func, args })
                if is_starts_with(func.inner().as_ref()) =>
            {
                let [value, prefix] = args.as_slice() else {
                    return None;
                };
                let attribute = self.attribute(value)?;
                let prefix = string(literal(prefix)?)?.to_string();
                let condition = self.begins_with(&attribute, &prefix, false, out);
                self.flattened(&attribute, condition, out)
            }
            _ => None,
        }
    }

    /// Whether unnesting flattens a map at `attribute`'s path into columns of
    /// its own, leaving the column NULL, rather than rendering it as JSON.
    fn maps_flattened(&self, attribute: &Attribute<'_>) -> bool {
        attribute.name.matches('.').count() < self.schema.unnest_depth()
    }

    /// `condition` on `attribute`, widened for a column unnesting flattens out
    /// of a map by the items whose column is read from a map key that contains
    /// a dot: `m.x.y` holds the `1` of `{"m": {"x.y": 1}}` as well as of
    /// `{"m": {"x": {"y": 1}}}`, and an expression path reaches only the
    /// second. Such an item has nothing at the path.
    fn flattened(
        &self,
        attribute: &Attribute<'_>,
        condition: Option<Condition>,
        out: &mut Placeholders,
    ) -> Option<Condition> {
        let condition = condition?;
        if !attribute.name.contains('.') || !self.schema.is_flattened_field(attribute.name) {
            return Some(condition);
        }
        let path = out.path(self.schema, attribute.name);
        Some(Condition::new(
            format!("({} OR attribute_not_exists({path}))", condition.expression),
            false,
            condition.reads_key,
        ))
    }

    /// The key-condition form of `expr`, when it is a predicate on a primary-key
    /// attribute that a key condition states exactly or as a superset.
    pub(crate) fn key_predicate(&self, expr: &Expr) -> Option<KeyPredicate> {
        match expr {
            // Only the other side of a NULL disjunct can be true.
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::Or,
                right,
            }) if is_null_predicate(left) || is_null_predicate(right) => {
                self.key_predicate(if is_null_predicate(right) {
                    left
                } else {
                    right
                })
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let cmp = Cmp::from_operator(*op)?;
                let (attribute, value, cmp) = match (self.attribute(left), self.attribute(right)) {
                    (Some(attribute), None) => (attribute, literal(right)?, cmp),
                    (None, Some(attribute)) => (attribute, literal(left)?, cmp.swapped()),
                    _ => return None,
                };
                match (attribute.key?, cmp) {
                    (KeyRole::Partition, Cmp::Eq) => {
                        Some(KeyPredicate::Partition(vec![Self::exact_key_value(
                            &attribute, value,
                        )?]))
                    }
                    (KeyRole::Partition, Cmp::NotEq) => {
                        Some(KeyPredicate::PartitionExcept(vec![Self::exact_key_value(
                            &attribute, value,
                        )?]))
                    }
                    (KeyRole::Partition, _) => None,
                    (KeyRole::Sort, _) => self
                        .sort_predicate(&attribute, cmp, value)
                        .map(KeyPredicate::Sort),
                }
            }
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => {
                let attribute = self.attribute(expr)?;
                if list.len() > MAX_IN_OPERANDS {
                    return None;
                }
                let values = list
                    .iter()
                    .map(|v| Self::exact_key_value(&attribute, literal(v)?))
                    .collect::<Option<Vec<_>>>()?;
                Some(match (attribute.key?, negated) {
                    (KeyRole::Partition, false) => KeyPredicate::Partition(values),
                    (KeyRole::Partition, true) => KeyPredicate::PartitionExcept(values),
                    (KeyRole::Sort, false) => KeyPredicate::Sort(SortPredicate::OneOf(values)),
                    (KeyRole::Sort, true) => KeyPredicate::Sort(SortPredicate::Except(values)),
                })
            }
            Expr::Between(Between {
                expr,
                negated: false,
                low,
                high,
            }) => {
                let attribute = self.attribute(expr)?;
                if attribute.key? != KeyRole::Sort {
                    return None;
                }
                let lower = self.sort_bound(&attribute, literal(low)?, true)?;
                let upper = self.sort_bound(&attribute, literal(high)?, false)?;
                Some(KeyPredicate::Sort(SortPredicate::Between(lower, upper)))
            }
            Expr::Like(like) => {
                let attribute = self.attribute(&like.expr)?;
                if like.negated || attribute.key? != KeyRole::Sort || !attribute.is_string_key() {
                    return None;
                }
                Some(KeyPredicate::Sort(SortPredicate::Prefix(like_prefix(
                    like,
                )?)))
            }
            Expr::ScalarFunction(ScalarFunction { func, args })
                if is_starts_with(func.inner().as_ref()) =>
            {
                let [value, prefix] = args.as_slice() else {
                    return None;
                };
                let attribute = self.attribute(value)?;
                if attribute.key? != KeyRole::Sort || !attribute.is_string_key() {
                    return None;
                }
                Some(KeyPredicate::Sort(SortPredicate::Prefix(
                    string(literal(prefix)?)?.to_string(),
                )))
            }
            _ => None,
        }
    }

    fn attribute<'e>(&self, expr: &'e Expr) -> Option<Attribute<'e>> {
        let Expr::Column(column) = expr else {
            return None;
        };
        let name = column.name.as_str();
        if self.schema.is_catch_all(name) {
            return None;
        }
        // A flattened column with more dots than unnesting descends levels is
        // read from a map key that contains a dot, and a path of its segments
        // reaches whatever else lies there.
        if self.schema.is_flattened_field(name)
            && name.matches('.').count() > self.schema.unnest_depth()
        {
            return None;
        }
        let field = self.schema.schema().field_with_name(name).ok()?;
        let kind = Kind::of(field.data_type())?;
        let key = if name == self.schema.partition_key() {
            Some(KeyRole::Partition)
        } else if Some(name) == self.schema.sort_key() {
            Some(KeyRole::Sort)
        } else {
            None
        };
        if let Some(role) = key {
            // A column declared with a type its key attribute cannot hold names
            // no value a key condition could be written against.
            let key_type = match role {
                KeyRole::Partition => self.schema.partition_key_type(),
                KeyRole::Sort => self.schema.sort_key_type(),
            };
            if !key_type.is_some_and(|t| kind.fits_key(t)) {
                return None;
            }
        }
        Some(Attribute { name, kind, key })
    }

    /// The value a key condition compares an exact key equality against.
    fn exact_key_value(attribute: &Attribute<'_>, value: &ScalarValue) -> Option<AttributeValue> {
        match attribute.kind {
            Kind::Utf8 => Some(AttributeValue::S(string(value)?.to_string())),
            // A number key holds an exact decimal; the item with key `k` is the
            // one the Int64 column reads as `k`.
            Kind::Int64 => Some(AttributeValue::N(integer(value)?.to_string())),
            Kind::Binary => match value {
                ScalarValue::Binary(Some(b)) | ScalarValue::LargeBinary(Some(b)) => {
                    Some(AttributeValue::B(Blob::new(b.clone())))
                }
                _ => None,
            },
            _ => None,
        }
    }

    fn sort_predicate(
        &self,
        attribute: &Attribute<'_>,
        cmp: Cmp,
        value: &ScalarValue,
    ) -> Option<SortPredicate> {
        Some(match cmp {
            Cmp::Eq => match attribute.kind {
                Kind::Utf8 | Kind::Int64 | Kind::Binary => {
                    SortPredicate::Eq(Self::exact_key_value(attribute, value)?)
                }
                Kind::Date32 => SortPredicate::Eq(AttributeValue::S(date(value)?)),
                // A widened equality is a range.
                _ => {
                    let (lower, upper) = self.widened(attribute, value)?;
                    SortPredicate::Between(lower, upper)
                }
            },
            Cmp::NotEq => SortPredicate::Except(vec![Self::exact_key_value(attribute, value)?]),
            Cmp::Gt | Cmp::GtEq => SortPredicate::Lower(
                self.sort_bound(attribute, value, true)?,
                cmp == Cmp::GtEq || !Self::bound_is_exact(attribute),
            ),
            Cmp::Lt | Cmp::LtEq => SortPredicate::Upper(
                self.sort_bound(attribute, value, false)?,
                cmp == Cmp::LtEq || !Self::bound_is_exact(attribute),
            ),
        })
    }

    /// Whether a sort-key bound is the literal itself rather than a widened one.
    fn bound_is_exact(attribute: &Attribute<'_>) -> bool {
        matches!(
            attribute.kind,
            Kind::Utf8 | Kind::Int64 | Kind::Binary | Kind::Date32
        )
    }

    /// A sort-key bound: the literal, or for a kind compared through a widened
    /// range, that range's end on the side of `lower`.
    fn sort_bound(
        &self,
        attribute: &Attribute<'_>,
        value: &ScalarValue,
        lower: bool,
    ) -> Option<AttributeValue> {
        match attribute.kind {
            Kind::Utf8 => Some(AttributeValue::S(string(value)?.to_string())),
            Kind::Int64 => Some(AttributeValue::N(integer(value)?.to_string())),
            Kind::Date32 => date(value).map(AttributeValue::S),
            Kind::Float64 | Kind::Timestamp => {
                let (low, high) = self.widened(attribute, value)?;
                Some(if lower { low } else { high })
            }
            Kind::Binary | Kind::Boolean => None,
        }
    }

    /// The narrowest range of stored values that holds every item whose column
    /// value equals `value`: a float within an ulp, a timestamp string in any
    /// offset.
    fn widened(
        &self,
        attribute: &Attribute<'_>,
        value: &ScalarValue,
    ) -> Option<(AttributeValue, AttributeValue)> {
        match attribute.kind {
            Kind::Float64 => {
                let x = float(value)?;
                Some((
                    AttributeValue::N(decimal(below(x))?),
                    AttributeValue::N(decimal(above(x))?),
                ))
            }
            Kind::Timestamp => {
                let (lower, upper) = self.timestamp_bounds(value)?;
                Some((AttributeValue::S(lower), AttributeValue::S(upper)))
            }
            _ => None,
        }
    }

    fn compare(
        &self,
        attribute: &Attribute<'_>,
        cmp: Cmp,
        value: &ScalarValue,
        out: &mut Placeholders,
    ) -> Option<Condition> {
        if value.is_null() {
            return None;
        }
        let reads_key = attribute.key.is_some();
        let path = out.path(self.schema, attribute.name);
        let condition =
            |expression: String, exact: bool| Some(Condition::new(expression, exact, reads_key));
        match attribute.kind {
            Kind::Utf8 => {
                let v = out.value(AttributeValue::S(string(value)?.to_string()));
                let comparison = format!("{path} {} {v}", cmp.symbol());
                if attribute.is_string_key() {
                    return condition(comparison, true);
                }
                // `<>` already holds for a map; the others need it added.
                if cmp == Cmp::NotEq {
                    return condition(comparison, false);
                }
                let map = Self::renders_map(&path, out);
                condition(format!("({comparison} OR {map})"), false)
            }
            Kind::Int64 => {
                let v = out.value(AttributeValue::N(integer(value)?.to_string()));
                // DynamoDB trims a number's zeros, so the items equal to `k` are
                // the ones read as `k`; but a number that is not an integer reads
                // as NULL, which any other comparison may still select.
                condition(format!("{path} {} {v}", cmp.symbol()), cmp == Cmp::Eq)
            }
            Kind::Float64 => {
                let x = float(value)?;
                // A stored decimal reads as the nearest f64, so `x` is reached
                // from anywhere within an ulp of it. `>` and `<` are supersets as
                // they are: rounding never moves a value across `x`.
                let bound = |x: f64, out: &mut Placeholders| {
                    Some(out.value(AttributeValue::N(decimal(x)?)))
                };
                let expression = match cmp {
                    Cmp::Eq => {
                        let (low, high) = (bound(below(x), out)?, bound(above(x), out)?);
                        format!("{path} BETWEEN {low} AND {high}")
                    }
                    Cmp::GtEq => format!("{path} >= {}", bound(below(x), out)?),
                    Cmp::LtEq => format!("{path} <= {}", bound(above(x), out)?),
                    // Arrow orders -0.0 below 0.0, which DynamoDB holds equal,
                    // so a zero bound takes in both zeros.
                    Cmp::Gt if x == 0.0 => format!("{path} >= {}", bound(x, out)?),
                    Cmp::Lt if x == 0.0 => format!("{path} <= {}", bound(x, out)?),
                    Cmp::NotEq if x == 0.0 => {
                        let n = out.type_name("N");
                        format!("attribute_type({path}, {n})")
                    }
                    cmp => format!("{path} {} {}", cmp.symbol(), bound(x, out)?),
                };
                condition(expression, false)
            }
            Kind::Boolean => {
                let ScalarValue::Boolean(Some(b)) = value else {
                    return None;
                };
                let expression = match cmp {
                    Cmp::Eq => format!("{path} = {}", out.value(AttributeValue::Bool(*b))),
                    Cmp::NotEq => format!("{path} = {}", out.value(AttributeValue::Bool(!*b))),
                    _ => return None,
                };
                condition(expression, true)
            }
            Kind::Binary => {
                if cmp != Cmp::Eq {
                    return None;
                }
                let v = out.value(Self::exact_key_value(attribute, value)?);
                condition(format!("{path} = {v}"), true)
            }
            Kind::Date32 => {
                let v = out.value(AttributeValue::S(date(value)?));
                // A string that is not a `YYYY-MM-DD` date reads as NULL.
                condition(format!("{path} {} {v}", cmp.symbol()), false)
            }
            Kind::Timestamp => {
                let (lower, upper) = self.timestamp_bounds(value)?;
                let expression = match cmp {
                    Cmp::Eq => {
                        let (low, high) = (
                            out.value(AttributeValue::S(lower)),
                            out.value(AttributeValue::S(upper)),
                        );
                        format!("{path} BETWEEN {low} AND {high}")
                    }
                    Cmp::Gt | Cmp::GtEq => {
                        format!("{path} >= {}", out.value(AttributeValue::S(lower)))
                    }
                    Cmp::Lt | Cmp::LtEq => {
                        format!("{path} <= {}", out.value(AttributeValue::S(upper)))
                    }
                    Cmp::NotEq => {
                        let s = out.type_name("S");
                        format!("attribute_type({path}, {s})")
                    }
                };
                condition(expression, false)
            }
        }
    }

    fn compare_attributes(
        &self,
        left: &Attribute<'_>,
        cmp: Cmp,
        right: &Attribute<'_>,
        out: &mut Placeholders,
    ) -> Option<Condition> {
        if left.kind != right.kind || !matches!(left.kind, Kind::Utf8 | Kind::Int64) {
            return None;
        }
        let reads_key = left.key.is_some() || right.key.is_some();
        let (l, r) = (
            out.path(self.schema, left.name),
            out.path(self.schema, right.name),
        );
        let comparison = format!("{l} {} {r}", cmp.symbol());
        if left.kind == Kind::Utf8 && cmp != Cmp::NotEq {
            let (l_map, r_map) = (Self::renders_map(&l, out), Self::renders_map(&r, out));
            return Some(Condition::new(
                format!("({comparison} OR {l_map} OR {r_map})"),
                false,
                reads_key,
            ));
        }
        Some(Condition::new(comparison, false, reads_key))
    }

    fn between(
        &self,
        attribute: &Attribute<'_>,
        low: &ScalarValue,
        high: &ScalarValue,
        out: &mut Placeholders,
    ) -> Option<Condition> {
        if low.is_null() || high.is_null() {
            return None;
        }
        let reads_key = attribute.key.is_some();
        let (low, high) = match attribute.kind {
            Kind::Utf8 => (
                AttributeValue::S(string(low)?.to_string()),
                AttributeValue::S(string(high)?.to_string()),
            ),
            Kind::Int64 => (
                AttributeValue::N(integer(low)?.to_string()),
                AttributeValue::N(integer(high)?.to_string()),
            ),
            Kind::Date32 => (
                AttributeValue::S(date(low)?),
                AttributeValue::S(date(high)?),
            ),
            Kind::Float64 | Kind::Timestamp => (
                self.widened(attribute, low)?.0,
                self.widened(attribute, high)?.1,
            ),
            Kind::Boolean | Kind::Binary => return None,
        };
        // DynamoDB refuses a range whose bounds are reversed; SQL keeps no row.
        if !in_order(&low, &high)? {
            return None;
        }
        let path = out.path(self.schema, attribute.name);
        let (low, high) = (out.value(low), out.value(high));
        let range = format!("{path} BETWEEN {low} AND {high}");
        if attribute.is_string_key() {
            return Some(Condition::new(range, true, reads_key));
        }
        if attribute.kind == Kind::Utf8 {
            let map = Self::renders_map(&path, out);
            return Some(Condition::new(
                format!("({range} OR {map})"),
                false,
                reads_key,
            ));
        }
        Some(Condition::new(range, false, reads_key))
    }

    fn in_list(
        &self,
        attribute: &Attribute<'_>,
        values: &[&ScalarValue],
        negated: bool,
        out: &mut Placeholders,
    ) -> Option<Condition> {
        if values.is_empty() || values.len() > MAX_IN_OPERANDS || values.iter().any(|v| v.is_null())
        {
            return None;
        }
        let encoded = values
            .iter()
            .map(|v| match attribute.kind {
                Kind::Utf8 => Some(AttributeValue::S(string(v)?.to_string())),
                Kind::Int64 => Some(AttributeValue::N(integer(v)?.to_string())),
                Kind::Boolean => match v {
                    ScalarValue::Boolean(Some(b)) => Some(AttributeValue::Bool(*b)),
                    _ => None,
                },
                Kind::Date32 => date(v).map(AttributeValue::S),
                // A float or timestamp is only reached through a widened range.
                Kind::Float64 | Kind::Timestamp | Kind::Binary => None,
            })
            .collect::<Option<Vec<_>>>()?;
        let reads_key = attribute.key.is_some();
        let path = out.path(self.schema, attribute.name);
        let list = encoded
            .into_iter()
            .map(|v| out.value(v))
            .collect::<Vec<_>>()
            .join(", ");
        let membership = format!("{path} IN ({list})");
        if negated {
            // DynamoDB's membership is exact on the stored value, so its
            // complement holds wherever SQL's does, and for missing attributes.
            return Some(Condition::new(negation(&membership), false, reads_key));
        }
        match attribute.kind {
            Kind::Utf8 if attribute.is_string_key() => {
                Some(Condition::new(membership, true, reads_key))
            }
            Kind::Utf8 => {
                let map = Self::renders_map(&path, out);
                Some(Condition::new(
                    format!("({membership} OR {map})"),
                    false,
                    reads_key,
                ))
            }
            Kind::Int64 | Kind::Boolean => Some(Condition::new(membership, true, reads_key)),
            _ => Some(Condition::new(membership, false, reads_key)),
        }
    }

    fn begins_with(
        &self,
        attribute: &Attribute<'_>,
        prefix: &str,
        negated: bool,
        out: &mut Placeholders,
    ) -> Option<Condition> {
        if attribute.kind != Kind::Utf8 {
            return None;
        }
        let reads_key = attribute.key.is_some();
        let path = out.path(self.schema, attribute.name);
        let p = out.value(AttributeValue::S(prefix.to_string()));
        let test = format!("begins_with({path}, {p})");
        if negated {
            // A map fails `begins_with`, so its negation keeps it.
            return Some(Condition::new(negation(&test), false, reads_key));
        }
        if attribute.is_string_key() {
            return Some(Condition::new(test, true, reads_key));
        }
        let map = Self::renders_map(&path, out);
        Some(Condition::new(
            format!("({test} OR {map})"),
            false,
            reads_key,
        ))
    }

    fn null_test(
        &self,
        attribute: &Attribute<'_>,
        is_null: bool,
        out: &mut Placeholders,
    ) -> Option<Condition> {
        // Every item has its key attributes, so a null test on one is decided
        // without reading; pushed, it would have to be stated by a key condition.
        if attribute.key.is_some() {
            return None;
        }
        // The attribute types the conversion reads as a value. Only where every
        // value of them converts is "none of them" exactly the NULL rows.
        let (types, all_convert): (&[&'static str], bool) = match attribute.kind {
            // A map unnesting flattens away leaves the column NULL.
            Kind::Utf8 if self.maps_flattened(attribute) => (&["S"], true),
            Kind::Utf8 => (&["S", "M"], true),
            Kind::Float64 => (&["N"], true),
            Kind::Boolean => (&["BOOL"], true),
            Kind::Binary => (&["B"], true),
            Kind::Int64 => (&["N"], false),
            Kind::Timestamp | Kind::Date32 => (&["S"], false),
        };
        if is_null && !all_convert {
            return None;
        }
        let reads_key = attribute.key.is_some();
        let path = out.path(self.schema, attribute.name);
        let tests = types
            .iter()
            .map(|t| {
                let t = out.type_name(t);
                format!("attribute_type({path}, {t})")
            })
            .collect::<Vec<_>>();
        let present = if tests.len() == 1 {
            tests.into_iter().next().unwrap_or_default()
        } else {
            format!("({})", tests.join(" OR "))
        };
        Some(if is_null {
            Condition::new(negation(&present), true, reads_key)
        } else {
            Condition::new(present, all_convert, reads_key)
        })
    }

    fn renders_map(path: &str, out: &mut Placeholders) -> String {
        let m = out.type_name("M");
        format!("attribute_type({path}, {m})")
    }

    /// The string bounds within which every stored timestamp at `value` lies.
    ///
    /// A timestamp is stored as a string in the offset it was written with, so
    /// the same instant is `…T12:00Z` in one item and `…T14:00+02:00` in
    /// another, and string order is not time order across offsets. Comparing
    /// the local date and time instead, widened by the largest offset, keeps
    /// every item the instant comparison would. Only a layout whose date and
    /// time sort as strings — fixed-width, year first — is compared this way.
    fn timestamp_bounds(&self, value: &ScalarValue) -> Option<(String, String)> {
        let millis = timestamp_millis(value)?;
        let layout = sortable_layout(&self.schema.time_format())?;
        let widening = if layout.zoned { MAX_OFFSET_MILLIS } else { 0 };
        let format = |millis: i64, nanos: i64| -> Option<String> {
            let dt = DateTime::from_timestamp_millis(millis)?
                .checked_add_signed(TimeDelta::nanoseconds(nanos))?;
            if !(0..=9999).contains(&dt.year()) {
                return None;
            }
            format_datetime(dt.fixed_offset(), &layout.local)
        };
        let lower = format(millis.checked_sub(widening)?, 0)?;
        // A layout finer than a millisecond writes digits the column drops, so
        // the upper bound is the millisecond's last instant. Anything after the
        // local date and time — an offset, a `Z` — sorts below `~`, so this
        // bounds every string whose local part is at most it.
        let upper = format!("{}~", format(millis.checked_add(widening)?, 999_999)?);
        Some((lower, upper))
    }
}

/// A Go time layout whose local date and time sort as strings.
struct SortableLayout {
    /// The layout of the local date and time, without the zone.
    local: String,
    /// Whether values carry an offset, so that string order is local time.
    zoned: bool,
}

fn sortable_layout(layout: &str) -> Option<SortableLayout> {
    let rest = layout.strip_prefix("2006-01-02")?;
    let rest = rest.strip_prefix('T').or_else(|| rest.strip_prefix(' '))?;
    let rest = rest.strip_prefix("15:04")?;
    let rest = rest.strip_prefix(":05").unwrap_or(rest);
    // A fixed-width fraction; Go's `.999` trims trailing zeros, which breaks
    // string order.
    let rest = match rest.strip_prefix('.') {
        Some(fraction) if fraction.starts_with('0') => fraction.trim_start_matches('0'),
        Some(_) => return None,
        None => rest,
    };
    let (zone, zoned) = match rest {
        "" => ("", false),
        "Z" => ("Z", false),
        "Z07:00" | "-07:00" | "Z0700" | "-0700" => (rest, true),
        _ => return None,
    };
    Some(SortableLayout {
        local: layout[..layout.len() - zone.len()].to_string(),
        zoned,
    })
}

/// Whether `s` is written in `layout` digit for digit, when the layout is one
/// whose values sort as strings. Every number in such a layout is zero-padded
/// to a fixed width, which is what Go means by `01` or `15`; chrono also reads
/// `2024-1-02`, which sorts apart from the date it names and so from the
/// strings a pushed-down comparison selects.
pub(crate) fn fits_layout(s: &str, layout: &str) -> bool {
    let Some(sortable) = sortable_layout(layout) else {
        return true;
    };
    let local = sortable.local.as_bytes();
    let value = s.as_bytes();
    value.len() >= local.len()
        && local.iter().zip(value).all(|(l, v)| {
            if l.is_ascii_digit() {
                v.is_ascii_digit()
            } else {
                l == v
            }
        })
}

/// `NOT expression`, parenthesized once: `DynamoDB` refuses an expression
/// with redundant parentheses, such as `NOT ((a OR b))`.
fn negation(expression: &str) -> String {
    if is_parenthesized(expression) {
        format!("(NOT {expression})")
    } else {
        format!("(NOT ({expression}))")
    }
}

/// Whether `expression` is one parenthesized group: the parenthesis it opens
/// with closes at its end. Names and values are placeholders, so every
/// parenthesis in an expression is structural.
fn is_parenthesized(expression: &str) -> bool {
    let bytes = expression.as_bytes();
    if bytes.first() != Some(&b'(') {
        return false;
    }
    let mut depth = 0_usize;
    for (i, b) in bytes.iter().enumerate() {
        match b {
            b'(' => depth += 1,
            b')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return i + 1 == bytes.len();
                }
            }
            _ => {}
        }
    }
    false
}

/// Whether `expr` is a NULL used as a predicate, which is never true: what
/// `DataFusion` leaves of `x = NULL` when it simplifies `x IN (1, NULL)`.
pub(crate) fn is_null_predicate(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(ScalarValue::Boolean(None) | ScalarValue::Null, _)
    )
}

fn literal(expr: &Expr) -> Option<&ScalarValue> {
    match expr {
        Expr::Literal(value, _) => Some(value),
        _ => None,
    }
}

fn string(value: &ScalarValue) -> Option<&str> {
    match value {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Some(s),
        _ => None,
    }
}

fn integer(value: &ScalarValue) -> Option<i128> {
    Some(match value {
        ScalarValue::Int8(Some(v)) => (*v).into(),
        ScalarValue::Int16(Some(v)) => (*v).into(),
        ScalarValue::Int32(Some(v)) => (*v).into(),
        ScalarValue::Int64(Some(v)) => (*v).into(),
        ScalarValue::UInt8(Some(v)) => (*v).into(),
        ScalarValue::UInt16(Some(v)) => (*v).into(),
        ScalarValue::UInt32(Some(v)) => (*v).into(),
        ScalarValue::UInt64(Some(v)) => (*v).into(),
        _ => return None,
    })
}

fn float(value: &ScalarValue) -> Option<f64> {
    let x = match value {
        ScalarValue::Float64(Some(v)) => *v,
        ScalarValue::Float32(Some(v)) => (*v).into(),
        _ => return None,
    };
    x.is_finite().then_some(x)
}

/// The lowest stored number that can read as `x`: the float below it, or
/// zero itself, since no number `DynamoDB` holds is close enough to round to it.
fn below(x: f64) -> f64 {
    if x == 0.0 { 0.0 } else { x.next_down() }
}

/// The highest stored number that can read as `x`; see [`below`].
fn above(x: f64) -> f64 {
    if x == 0.0 { 0.0 } else { x.next_up() }
}

/// Whether `low` is at or below `high`, when they compare.
fn in_order(low: &AttributeValue, high: &AttributeValue) -> Option<bool> {
    Some(match (low, high) {
        (AttributeValue::S(low), AttributeValue::S(high)) => low.as_bytes() <= high.as_bytes(),
        (AttributeValue::N(low), AttributeValue::N(high)) => {
            match (low.parse::<i128>(), high.parse::<i128>()) {
                (Ok(low), Ok(high)) => low <= high,
                _ => low.parse::<f64>().ok()? <= high.parse::<f64>().ok()?,
            }
        }
        _ => return None,
    })
}

/// `x` as a `DynamoDB` number, within the magnitudes a plain decimal renders
/// without an exponent and `DynamoDB` accepts.
fn decimal(x: f64) -> Option<String> {
    if x == 0.0 {
        return Some("0".to_string());
    }
    let magnitude = x.abs();
    ((1e-20..1e20).contains(&magnitude)).then(|| format!("{x}"))
}

/// A `Date32` literal as the `YYYY-MM-DD` string it compares as, for a year
/// that renders in four digits.
fn date(value: &ScalarValue) -> Option<String> {
    let ScalarValue::Date32(Some(days)) = value else {
        return None;
    };
    let date = NaiveDate::from_ymd_opt(1970, 1, 1)?
        .checked_add_signed(chrono::Duration::days(i64::from(*days)))?;
    (0..=9999)
        .contains(&date.year())
        .then(|| date.format("%Y-%m-%d").to_string())
}

fn timestamp_millis(value: &ScalarValue) -> Option<i64> {
    Some(match value {
        ScalarValue::TimestampSecond(Some(v), _) => v.checked_mul(1_000)?,
        ScalarValue::TimestampMillisecond(Some(v), _) => *v,
        ScalarValue::TimestampMicrosecond(Some(v), _) => v.div_euclid(1_000),
        ScalarValue::TimestampNanosecond(Some(v), _) => v.div_euclid(1_000_000),
        _ => return None,
    })
}

/// The literal prefix of a LIKE pattern that matches exactly the strings
/// beginning with it: literal characters then one trailing `%`, with `\` as the
/// escape, as `DataFusion` evaluates it.
fn like_prefix(like: &Like) -> Option<String> {
    if like.case_insensitive || like.escape_char.is_some_and(|c| c != '\\') {
        return None;
    }
    let pattern = string(literal(&like.pattern)?)?;
    let mut prefix = String::with_capacity(pattern.len());
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => prefix.push(chars.next().unwrap_or('\\')),
            // `LIKE '%'` holds for every string, which is no prefix to test.
            '%' => return (chars.as_str().is_empty() && !prefix.is_empty()).then_some(prefix),
            '_' => return None,
            c => prefix.push(c),
        }
    }
    // No wildcard: an equality, which DataFusion rewrites LIKE into anyway.
    None
}

/// Whether `udf` is `DataFusion`'s own `starts_with`. A function registered
/// under the same name is a different type, and is not translated.
fn is_starts_with(udf: &dyn datafusion::logical_expr::ScalarUDFImpl) -> bool {
    (udf as &dyn std::any::Any).is::<StartsWithFunc>()
}

#[cfg(test)]
mod tests;
