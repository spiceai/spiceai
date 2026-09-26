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

//! Translation of `DataFusion` filters into Cosmos DB `NoSQL` query conditions.
//!
//! Every condition is a superset of the rows its filter keeps, which
//! `DataFusion` then filters again: the rows are decoded from JSON by
//! `arrow-json`, whose reading of a document differs from how Cosmos DB
//! compares it. A numeric string decodes into an integer column as its number,
//! a fraction truncates, and a value of another type fails the query. So each
//! comparison is guarded by the JSON type it applies to, and a document whose
//! value the column cannot hold is kept, so that it fails the query as it would
//! unfiltered rather than being silently skipped. The type guards also make a
//! condition independent of how the service evaluates a comparison with null or
//! an undefined property, where the Cosmos DB emulator and the service disagree.

use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::functions::string::starts_with::StartsWithFunc;
use datafusion::logical_expr::expr::{Between, InList, Like, ScalarFunction};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown};
use datafusion::scalar::ScalarValue;
use serde_json::Value;

/// The alias every query names its documents by.
const DOCUMENT: &str = "c";

/// The most values an `IN` list is pushed down with.
const MAX_IN_VALUES: usize = 256;

/// Integers beyond 2^53 lose precision as the doubles Cosmos DB stores.
const EXACT_DOUBLE_INTEGERS: i64 = 1 << 53;

/// Query parameters, named `@p0`, `@p1`, … in the order they are allocated.
#[derive(Debug, Default)]
pub(crate) struct Parameters {
    values: Vec<Value>,
}

impl Parameters {
    fn add(&mut self, value: Value) -> String {
        self.values.push(value);
        format!("@p{}", self.values.len() - 1)
    }

    pub(crate) fn into_named(self) -> Vec<(String, Value)> {
        self.values
            .into_iter()
            .enumerate()
            .map(|(i, value)| (format!("@p{i}"), value))
            .collect()
    }
}

/// A reference to a top-level property: `c["name"]`, which reaches any
/// property name, reserved words and punctuation included.
pub(crate) fn property(name: &str) -> String {
    // A JSON string is a Cosmos DB string literal.
    format!(
        "{DOCUMENT}[{}]",
        serde_json::to_string(name).unwrap_or_else(|_| format!("\"{name}\""))
    )
}

/// How a column's values are decoded from JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// A JSON string; anything else fails decoding.
    Utf8,
    /// A JSON number, a fraction truncated, or a string holding one.
    Int64,
    /// A JSON number, or a string holding one.
    Float64,
    Boolean,
}

impl Kind {
    fn of(data_type: &DataType) -> Option<Self> {
        Some(match data_type {
            DataType::Utf8 => Self::Utf8,
            DataType::Int64 => Self::Int64,
            DataType::Float64 => Self::Float64,
            DataType::Boolean => Self::Boolean,
            _ => return None,
        })
    }

    /// The Cosmos DB type check for the JSON type this kind compares.
    fn type_check(self) -> &'static str {
        match self {
            Self::Utf8 => "IS_STRING",
            Self::Int64 | Self::Float64 => "IS_NUMBER",
            Self::Boolean => "IS_BOOL",
        }
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
            Self::NotEq => "!=",
            Self::Lt => "<",
            Self::LtEq => "<=",
            Self::Gt => ">",
            Self::GtEq => ">=",
        }
    }
}

pub(crate) struct Translator<'a> {
    schema: &'a Schema,
}

impl<'a> Translator<'a> {
    pub(crate) fn new(schema: &'a Schema) -> Self {
        Self { schema }
    }

    /// Every condition is a superset, so a filter is at best inexact.
    pub(crate) fn classify(&self, expr: &Expr) -> TableProviderFilterPushDown {
        match self.condition(expr, &mut Parameters::default()) {
            Some(_) => TableProviderFilterPushDown::Inexact,
            None => TableProviderFilterPushDown::Unsupported,
        }
    }

    /// A condition selecting at least the documents `expr` keeps. Allocates
    /// parameters only when it succeeds.
    pub(crate) fn condition(&self, expr: &Expr, params: &mut Parameters) -> Option<String> {
        let allocated = params.values.len();
        let condition = self.translate(expr, params);
        if condition.is_none() {
            params.values.truncate(allocated);
        }
        condition
    }

    fn translate(&self, expr: &Expr, params: &mut Parameters) -> Option<String> {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
                Operator::And => {
                    match (self.condition(left, params), self.condition(right, params)) {
                        (Some(l), Some(r)) => Some(format!("({l} AND {r})")),
                        // A conjunction keeps a subset of either side's rows.
                        (Some(one), None) | (None, Some(one)) => Some(one),
                        (None, None) => None,
                    }
                }
                // A NULL disjunct is never true, so the other side keeps the
                // same rows.
                Operator::Or if is_null_predicate(right) => self.condition(left, params),
                Operator::Or if is_null_predicate(left) => self.condition(right, params),
                Operator::Or => {
                    let l = self.condition(left, params)?;
                    let r = self.condition(right, params)?;
                    Some(format!("({l} OR {r})"))
                }
                op => {
                    let cmp = Cmp::from_operator(*op)?;
                    match (self.column(left), self.column(right)) {
                        (Some((name, kind)), None) => {
                            Self::compare(name, kind, cmp, literal(right)?, params)
                        }
                        (None, Some((name, kind))) => {
                            Self::compare(name, kind, cmp.swapped(), literal(left)?, params)
                        }
                        _ => None,
                    }
                }
            },
            // A boolean column used as a predicate holds `true`.
            Expr::Column(_) => {
                let (name, kind) = self
                    .column(expr)
                    .filter(|(_, kind)| *kind == Kind::Boolean)?;
                Self::compare(
                    name,
                    kind,
                    Cmp::Eq,
                    &ScalarValue::Boolean(Some(true)),
                    params,
                )
            }
            Expr::Not(inner) => {
                let (name, kind) = self
                    .column(inner)
                    .filter(|(_, kind)| *kind == Kind::Boolean)?;
                Self::compare(
                    name,
                    kind,
                    Cmp::Eq,
                    &ScalarValue::Boolean(Some(false)),
                    params,
                )
            }
            Expr::IsNull(inner) => {
                let (name, kind) = self.column(inner)?;
                let p = property(name);
                // A value the column cannot hold fails decoding, so it is kept.
                Some(format!(
                    "(NOT IS_DEFINED({p}) OR IS_NULL({p}) OR NOT {}({p}))",
                    kind.type_check()
                ))
            }
            Expr::IsNotNull(inner) => {
                let (name, _) = self.column(inner)?;
                let p = property(name);
                Some(format!("(IS_DEFINED({p}) AND NOT IS_NULL({p}))"))
            }
            Expr::Between(Between {
                expr,
                negated: false,
                low,
                high,
            }) => {
                let (name, kind) = self.column(expr)?;
                let low = Self::compare(name, kind, Cmp::GtEq, literal(low)?, params)?;
                let high = Self::compare(name, kind, Cmp::LtEq, literal(high)?, params)?;
                Some(format!("({low} AND {high})"))
            }
            Expr::InList(InList {
                expr,
                list,
                negated: false,
            }) => {
                let (name, kind) = self.column(expr)?;
                let values = list.iter().map(literal).collect::<Option<Vec<_>>>()?;
                Self::in_list(name, kind, &values, params)
            }
            Expr::Like(like) => {
                let (name, _) = self
                    .column(&like.expr)
                    .filter(|(_, kind)| *kind == Kind::Utf8)?;
                let prefix = like_prefix(like)?;
                Some(Self::starts_with(name, prefix, params))
            }
            Expr::ScalarFunction(ScalarFunction { func, args })
                if is_starts_with(func.inner().as_ref()) =>
            {
                let [value, prefix] = args.as_slice() else {
                    return None;
                };
                let (name, _) = self.column(value).filter(|(_, kind)| *kind == Kind::Utf8)?;
                let prefix = string(literal(prefix)?)?.to_string();
                (!prefix.is_empty()).then(|| Self::starts_with(name, prefix, params))
            }
            _ => None,
        }
    }

    fn column<'e>(&self, expr: &'e Expr) -> Option<(&'e str, Kind)> {
        let Expr::Column(column) = expr else {
            return None;
        };
        let field = self.schema.field_with_name(&column.name).ok()?;
        Some((column.name.as_str(), Kind::of(field.data_type())?))
    }

    fn compare(
        name: &str,
        kind: Kind,
        cmp: Cmp,
        value: &ScalarValue,
        params: &mut Parameters,
    ) -> Option<String> {
        if value.is_null() {
            return None;
        }
        let p = property(name);
        let typed = match kind {
            Kind::Utf8 => {
                let s = string(value)?;
                // Cosmos DB and Arrow may order strings differently beyond
                // ASCII (by UTF-16 code unit or by UTF-8 byte), but agree
                // wherever the bound is ASCII.
                if !matches!(cmp, Cmp::Eq | Cmp::NotEq) && !s.is_ascii() {
                    return None;
                }
                let v = params.add(Value::String(s.to_string()));
                format!("{p} {} {v}", cmp.symbol())
            }
            Kind::Int64 => {
                let k = integer(value)?;
                // A fraction decodes truncated toward zero, so the documents
                // an integer bound keeps lie a little beyond it.
                match cmp {
                    Cmp::Gt | Cmp::Lt => format!("{p} {} {}", cmp.symbol(), params.add(k.into())),
                    Cmp::GtEq => format!("{p} > {}", params.add((k - 1).into())),
                    Cmp::LtEq => format!("{p} < {}", params.add((k + 1).into())),
                    Cmp::Eq => {
                        let (low, high) = (params.add((k - 1).into()), params.add((k + 1).into()));
                        format!("({p} > {low} AND {p} < {high})")
                    }
                    Cmp::NotEq => String::from("true"),
                }
            }
            Kind::Float64 => {
                let x = float(value)?;
                // Cosmos DB holds -0 equal to 0, which Arrow orders apart.
                let cmp = match cmp {
                    Cmp::NotEq if x == 0.0 => None,
                    Cmp::Lt if x == 0.0 => Some(Cmp::LtEq),
                    Cmp::Gt if x == 0.0 => Some(Cmp::GtEq),
                    cmp => Some(cmp),
                };
                match cmp {
                    Some(cmp) => {
                        let v = params.add(serde_json::Number::from_f64(x).map(Value::Number)?);
                        format!("{p} {} {v}", cmp.symbol())
                    }
                    None => String::from("true"),
                }
            }
            Kind::Boolean => {
                let ScalarValue::Boolean(Some(b)) = value else {
                    return None;
                };
                let b = match cmp {
                    Cmp::Eq => *b,
                    Cmp::NotEq => !*b,
                    _ => return None,
                };
                format!("{p} = {}", params.add(Value::Bool(b)))
            }
        };
        Some(guarded(&p, kind, &typed))
    }

    fn in_list(
        name: &str,
        kind: Kind,
        values: &[&ScalarValue],
        params: &mut Parameters,
    ) -> Option<String> {
        if values.is_empty() || values.len() > MAX_IN_VALUES || values.iter().any(|v| v.is_null()) {
            return None;
        }
        let p = property(name);
        let members = match kind {
            Kind::Utf8 => values
                .iter()
                .map(|v| string(v).map(|s| Value::String(s.to_string())))
                .collect::<Option<Vec<_>>>()?,
            Kind::Boolean => values
                .iter()
                .map(|v| match v {
                    ScalarValue::Boolean(Some(b)) => Some(Value::Bool(*b)),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?,
            Kind::Float64 => values
                .iter()
                .map(|v| {
                    float(v)
                        .and_then(serde_json::Number::from_f64)
                        .map(Value::Number)
                })
                .collect::<Option<Vec<_>>>()?,
            // A truncated fraction reads as the integer below it, which a
            // membership test cannot see; each value is its own range.
            Kind::Int64 => {
                let ranges = values
                    .iter()
                    .map(|v| Self::compare(name, kind, Cmp::Eq, v, params))
                    .collect::<Option<Vec<_>>>()?;
                return Some(format!("({})", ranges.join(" OR ")));
            }
        };
        let list = members
            .into_iter()
            .map(|v| params.add(v))
            .collect::<Vec<_>>()
            .join(", ");
        Some(guarded(&p, kind, &format!("{p} IN ({list})")))
    }

    fn starts_with(name: &str, prefix: String, params: &mut Parameters) -> String {
        let p = property(name);
        let v = params.add(Value::String(prefix));
        guarded(&p, Kind::Utf8, &format!("STARTSWITH({p}, {v}, false)"))
    }
}

/// `typed` applied to values of the kind's JSON type, or else any value: one
/// the column cannot hold fails decoding as it would unfiltered, and a numeric
/// column decodes a string holding a number.
fn guarded(p: &str, kind: Kind, typed: &str) -> String {
    let check = kind.type_check();
    format!(
        "(({check}({p}) AND {typed}) OR (IS_DEFINED({p}) AND NOT IS_NULL({p}) AND NOT {check}({p})))"
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

/// Whether `expr` is a NULL used as a predicate, which is never true: what
/// `DataFusion` leaves of `x = NULL` when it simplifies `x IN (1, NULL)`.
pub(super) fn is_null_predicate(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(ScalarValue::Boolean(None) | ScalarValue::Null, _)
    )
}

fn integer(value: &ScalarValue) -> Option<i64> {
    let v: i64 = match value {
        ScalarValue::Int8(Some(v)) => (*v).into(),
        ScalarValue::Int16(Some(v)) => (*v).into(),
        ScalarValue::Int32(Some(v)) => (*v).into(),
        ScalarValue::Int64(Some(v)) => *v,
        ScalarValue::UInt8(Some(v)) => (*v).into(),
        ScalarValue::UInt16(Some(v)) => (*v).into(),
        ScalarValue::UInt32(Some(v)) => (*v).into(),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v).ok()?,
        _ => return None,
    };
    (v.unsigned_abs() < EXACT_DOUBLE_INTEGERS.unsigned_abs()).then_some(v)
}

fn float(value: &ScalarValue) -> Option<f64> {
    let x = match value {
        ScalarValue::Float64(Some(v)) => *v,
        ScalarValue::Float32(Some(v)) => (*v).into(),
        _ => return None,
    };
    x.is_finite().then_some(x)
}

/// The literal prefix of a LIKE pattern that matches exactly the strings
/// beginning with it: literal characters then one trailing `%`, with `\` as
/// the escape, as `DataFusion` evaluates it.
fn like_prefix(like: &Like) -> Option<String> {
    if like.negated || like.case_insensitive || like.escape_char.is_some_and(|c| c != '\\') {
        return None;
    }
    let pattern = string(literal(&like.pattern)?)?;
    let mut prefix = String::with_capacity(pattern.len());
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => prefix.push(chars.next().unwrap_or('\\')),
            '%' => return (chars.as_str().is_empty() && !prefix.is_empty()).then_some(prefix),
            '_' => return None,
            c => prefix.push(c),
        }
    }
    None
}

/// Whether `udf` is `DataFusion`'s own `starts_with`; a function registered
/// under the same name is a different type, and is not translated.
fn is_starts_with(udf: &dyn datafusion::logical_expr::ScalarUDFImpl) -> bool {
    (udf as &dyn std::any::Any).is::<StartsWithFunc>()
}

#[cfg(test)]
mod tests;
