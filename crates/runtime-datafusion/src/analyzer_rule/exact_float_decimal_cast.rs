/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Casts a float *literal* to a decimal through its decimal spelling, not its
//! binary value.
//!
//! `WHERE dec_col = 12345678.90` plans the literal as `Float64`, and type coercion
//! then compares both sides as a wide decimal such as `Decimal128(30, 15)`. Arrow's
//! `Float64 -> Decimal` cast scales the binary value, so the literal becomes
//! `12345678.899999999524864` and the filter silently matches nothing — about one
//! cent value in five misses this way (#14970). A bound `$1` with the same number
//! takes the same path once the parameter is substituted.
//!
//! This rule rewrites `CAST(<float literal> AS DECIMAL(p, s))` to the decimal
//! literal parsed exactly from the literal's decimal spelling, at the cast's
//! precision and scale. How a spelling with more fraction digits than the scale
//! is handled depends on where the spelling came from (below): a retained SQL
//! token spelling is the exact value, so it is rounded half away from zero to
//! the scale, as a decimal-to-decimal cast rounds; a spelling derived from the
//! float is left to the cast. Anything that does not parse at the cast's
//! precision is left to the cast as before, so overflow and out-of-range
//! behaviour is unchanged.
//!
//! The spelling comes from one of two places:
//!
//! - **The SQL token, when the `f64` lost it.** A token with more significant
//!   digits than an `f64` holds (`4.0700000000000003` reads as the same `f64` as
//!   `4.07`) cannot be recovered from the float. [`FloatLiteralSpellings`] reads
//!   such tokens from the parsed statement and records them on the planned
//!   literal under [`FLOAT_LITERAL_SPELLING_KEY`]; this rule parses that
//!   spelling, rounded half away from zero to the cast's scale as a decimal cast
//!   rounds, and removes the key.
//! - **The float itself, otherwise.** The shortest decimal string that reads back
//!   as the same `f64` (Rust's `Display`) is the token whenever the token had at
//!   most `f64::DIGITS` significant digits, since every such decimal reads as a
//!   distinct `f64`: `12345678.90` is recovered as `12345678.9`. A bound float
//!   parameter has no token, and is converted the same way. This spelling is used
//!   only when it fits the cast's scale without rounding. A float whose shortest
//!   spelling is longer than the type's decimal digits (`f64::DIGITS` /
//!   `f32::DIGITS` / ~3 for `f16`) is left to the cast.
//!
//! The logical plan does not record which token a literal was planned from, so
//! spellings are matched to literals by their `f64`. When one statement spells
//! the same `f64` two different ways and at least one of them is lossy
//! (`4.07` and `4.0700000000000003`), no literal of that `f64` can be told
//! apart from the other, and every such cast keeps the binary cast it had
//! before this rule.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::ControlFlow;

use datafusion::arrow::datatypes::{DataType, Decimal128Type, Decimal256Type};
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::expr::{Alias, Cast, TryCast};
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;
use datafusion::sql::parser::{CopyToSource, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, Value, Visit, visit_expressions};

/// Literal metadata carrying a float literal's SQL spelling, set by
/// [`FloatLiteralSpellings::retain`] when the planned `f64` cannot recover it.
/// An empty value means the statement spelled this `f64` more than one way, so
/// no single spelling is known and the cast is left alone.
pub const FLOAT_LITERAL_SPELLING_KEY: &str = "spice.float_literal_spelling";

/// Rewrites float-literal-to-decimal casts into exact decimal literals. See the
/// module docs.
#[derive(Debug, Default)]
pub struct ExactFloatLiteralDecimalCast;

impl ExactFloatLiteralDecimalCast {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl AnalyzerRule for ExactFloatLiteralDecimalCast {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| {
            // Top down, so a cast sees its literal's spelling before the literal
            // itself is visited and has the spelling removed.
            plan.map_expressions(|expr| expr.transform_down(|expr| Ok(rewrite_expr(expr))))
        })
        .map(|transformed| transformed.data)
    }

    fn name(&self) -> &'static str {
        "exact_float_literal_decimal_cast"
    }
}

fn rewrite_expr(expr: Expr) -> Transformed<Expr> {
    if let Expr::Cast(Cast { expr: inner, field }) | Expr::TryCast(TryCast { expr: inner, field }) =
        &expr
        && let Expr::Literal(value, metadata) = without_identity_float_cast(inner)
    {
        let data_type = field.data_type();
        let token_spelling = metadata
            .as_ref()
            .and_then(|metadata| metadata.inner().get(FLOAT_LITERAL_SPELLING_KEY));
        let decimal = match token_spelling {
            // The token is the exact value, so rounding it to the scale here is
            // the decimal-to-decimal cast, which the binary cast cannot match.
            Some(spelling) => decimal_scale(data_type)
                .and_then(|scale| round_to_scale(spelling, scale))
                .and_then(|spelling| exact_decimal_from_spelling(&spelling, data_type)),
            None => exact_decimal(value, data_type),
        };
        if let Some(decimal) = decimal {
            return Transformed::yes(Expr::Literal(decimal, without_spelling(metadata.as_ref())));
        }
    }
    // The spelling only matters to the rewrite above; drop it from any literal
    // it did not apply to so it never reaches the physical plan. This includes
    // a literal that is not inside a cast, and the literal of a cast this
    // function left unchanged: `transform_down` visits that literal next.
    match expr {
        Expr::Literal(value, Some(metadata)) if has_spelling(&metadata) => {
            Transformed::yes(Expr::Literal(value, without_spelling(Some(&metadata))))
        }
        expr => Transformed::no(expr),
    }
}

/// `expr` with a cast of a float literal to its own type removed.
///
/// Such a cast leaves the value unchanged, so the literal under it still spells
/// the decimal the outer cast is after. Flight SQL wraps each bound `$N` as
/// `CAST($N AS DOUBLE)` to type the placeholder, and without this the bound
/// float would take the binary cast that the bare `$1` of HTTP and SQL does not.
fn without_identity_float_cast(expr: &Expr) -> &Expr {
    if let Expr::Cast(Cast { expr: inner, field }) | Expr::TryCast(TryCast { expr: inner, field }) =
        expr
        && matches!(
            field.data_type(),
            DataType::Float16 | DataType::Float32 | DataType::Float64
        )
        && let Expr::Literal(value, _) = inner.as_ref()
        && value.data_type() == *field.data_type()
    {
        return inner;
    }
    expr
}

fn has_spelling(metadata: &FieldMetadata) -> bool {
    metadata.inner().contains_key(FLOAT_LITERAL_SPELLING_KEY)
}

fn without_spelling(metadata: Option<&FieldMetadata>) -> Option<FieldMetadata> {
    let mut inner = metadata?.inner().clone();
    inner.remove(FLOAT_LITERAL_SPELLING_KEY);
    (!inner.is_empty()).then(|| FieldMetadata::new(inner))
}

/// The decimal `value` spells, at `data_type`'s precision and scale, when it is a
/// finite float literal whose shortest round-trip spelling fits that scale.
pub(crate) fn exact_decimal(value: &ScalarValue, data_type: &DataType) -> Option<ScalarValue> {
    // Cap significant digits by the source float's precision. Reusing
    // `f64::DIGITS` for f32/f16 rewrites values past those types' reliable
    // decimal range (e.g. `Float32(123456.79)` → a spelling unequal to the
    // stored binary value).
    let (spelling, max_digits) = match value {
        ScalarValue::Float64(Some(v)) if v.is_finite() => (v.to_string(), f64::DIGITS as usize),
        ScalarValue::Float32(Some(v)) if v.is_finite() => (v.to_string(), f32::DIGITS as usize),
        ScalarValue::Float16(Some(v)) if v.is_finite() => (v.to_string(), 3),
        _ => return None,
    };
    if !is_unambiguous_spelling(&spelling, max_digits) {
        return None;
    }
    exact_decimal_from_spelling(&spelling, data_type)
}

/// `spelling`, a plain decimal string, parsed exactly at `data_type`'s precision
/// and scale, when it fits that scale without rounding.
fn exact_decimal_from_spelling(spelling: &str, data_type: &DataType) -> Option<ScalarValue> {
    if spelling.is_empty() {
        return None;
    }
    let fraction_digits = spelling.split_once('.').map_or(0, |(_, f)| f.len());
    match data_type {
        DataType::Decimal128(precision, scale) => {
            if usize::try_from(*scale).ok()? < fraction_digits {
                return None;
            }
            let parsed = datafusion::arrow::compute::kernels::cast_utils::parse_decimal::<
                Decimal128Type,
            >(spelling, *precision, *scale)
            .ok()?;
            Some(ScalarValue::Decimal128(Some(parsed), *precision, *scale))
        }
        DataType::Decimal256(precision, scale) => {
            if usize::try_from(*scale).ok()? < fraction_digits {
                return None;
            }
            let parsed = datafusion::arrow::compute::kernels::cast_utils::parse_decimal::<
                Decimal256Type,
            >(spelling, *precision, *scale)
            .ok()?;
            Some(ScalarValue::Decimal256(Some(parsed), *precision, *scale))
        }
        _ => None,
    }
}

fn decimal_scale(data_type: &DataType) -> Option<usize> {
    match data_type {
        DataType::Decimal128(_, scale) | DataType::Decimal256(_, scale) => {
            usize::try_from(*scale).ok()
        }
        _ => None,
    }
}

/// A plain decimal `spelling` rounded half away from zero to at most `scale`
/// fraction digits, as Arrow's decimal-to-decimal cast rounds. `None` for an
/// empty or malformed spelling.
fn round_to_scale(spelling: &str, scale: usize) -> Option<String> {
    let (sign, unsigned) = match spelling.strip_prefix('-') {
        Some(unsigned) => ("-", unsigned),
        None => ("", spelling),
    };
    let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if integer.is_empty()
        || !integer
            .bytes()
            .chain(fraction.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return None;
    }
    if fraction.len() <= scale {
        return Some(spelling.to_string());
    }
    let round_up = fraction.as_bytes()[scale] >= b'5';
    let mut digits: Vec<u8> = format!("{integer}{}", &fraction[..scale]).into_bytes();
    if round_up {
        let mut carry = true;
        for digit in digits.iter_mut().rev() {
            if *digit == b'9' {
                *digit = b'0';
            } else {
                *digit += 1;
                carry = false;
                break;
            }
        }
        if carry {
            digits.insert(0, b'1');
        }
    }
    let digits = String::from_utf8(digits).ok()?;
    let (integer, fraction) = digits.split_at(digits.len() - scale);
    Some(if fraction.is_empty() {
        format!("{sign}{integer}")
    } else {
        format!("{sign}{integer}.{fraction}")
    })
}

/// The SQL spellings of a statement's number tokens that the `f64` they are
/// planned as cannot recover.
///
/// `DataFusion` plans a fractional token as `Float64`, which keeps about 17
/// significant digits, so `4.0700000000000003` and `4.07` plan as the same
/// literal. [`Self::collect`] reads the tokens from the parsed statement before
/// it is planned, and [`Self::retain`] records each such token's spelling on the
/// literal it was planned as, for [`ExactFloatLiteralDecimalCast`] to cast from.
/// Tokens the `f64` does recover (every token of at most `f64::DIGITS`
/// significant digits) are not recorded: the rule recovers them from the float.
#[derive(Debug, Default)]
pub struct FloatLiteralSpellings {
    /// Keyed by the bits of the token's `f64` magnitude. `None` when the
    /// statement spells that `f64` more than one way.
    by_bits: HashMap<u64, Option<String>>,
}

impl FloatLiteralSpellings {
    /// Reads the number tokens of `statement` whose `f64` loses their value.
    #[must_use]
    pub fn collect(statement: &DFStatement) -> Self {
        let mut spellings: HashMap<u64, (BTreeSet<String>, bool)> = HashMap::new();
        let mut record = |token: &str| {
            if let Some((bits, spelling, recovered)) = token_spelling(token) {
                let (seen, lossy) = spellings.entry(bits).or_default();
                seen.insert(spelling);
                *lossy |= !recovered;
            }
        };
        visit_statement(statement, &mut record);
        Self {
            by_bits: spellings
                .into_iter()
                .filter(|(_, (_, lossy))| *lossy)
                .map(|(bits, (seen, _))| {
                    let only = (seen.len() == 1).then(|| seen.into_iter().next()).flatten();
                    (bits, only)
                })
                .collect(),
        }
    }

    /// Whether no token of the statement lost its value to the `f64`.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_bits.is_empty()
    }

    /// Records the collected spellings on the `Float64` literals of `plan` they
    /// were planned as.
    ///
    /// A literal that is a whole projection or `VALUES` entry is left alone:
    /// those positions name an output column, and the spelling must not become
    /// part of its field metadata. A cast or other expression in either
    /// position still has its float literals marked.
    ///
    /// # Errors
    ///
    /// Propagates errors from rewriting the plan.
    pub fn retain(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        if self.is_empty() {
            return Ok(plan);
        }
        plan.transform_up_with_subqueries(|plan| match plan {
            LogicalPlan::Values(_) | LogicalPlan::Projection(_) => plan.map_expressions(|expr| {
                if is_bare_literal(&expr) {
                    Ok(Transformed::no(expr))
                } else {
                    expr.transform_up(|expr| Ok(self.mark(expr)))
                }
            }),
            plan => plan.map_expressions(|expr| expr.transform_up(|expr| Ok(self.mark(expr)))),
        })
        .map(|transformed| transformed.data)
    }

    fn mark(&self, expr: Expr) -> Transformed<Expr> {
        let Expr::Literal(ScalarValue::Float64(Some(value)), metadata) = &expr else {
            return Transformed::no(expr);
        };
        let Some(spelling) = self.by_bits.get(&value.abs().to_bits()) else {
            return Transformed::no(expr);
        };
        let spelling = match spelling {
            Some(spelling) if value.is_sign_negative() => format!("-{spelling}"),
            Some(spelling) => spelling.clone(),
            None => String::new(),
        };
        let mut inner = metadata
            .as_ref()
            .map_or_else(BTreeMap::new, |metadata| metadata.inner().clone());
        inner.insert(FLOAT_LITERAL_SPELLING_KEY.to_string(), spelling);
        Transformed::yes(Expr::Literal(
            ScalarValue::Float64(Some(*value)),
            Some(FieldMetadata::new(inner)),
        ))
    }
}

fn is_bare_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(..) => true,
        Expr::Alias(Alias { expr, .. }) => is_bare_literal(expr),
        _ => false,
    }
}

fn visit_statement(statement: &DFStatement, record: &mut impl FnMut(&str)) {
    match statement {
        DFStatement::Statement(statement) => visit_numbers(statement.as_ref(), record),
        DFStatement::CopyTo(copy) => {
            if let CopyToSource::Query(query) = &copy.source {
                visit_numbers(query.as_ref(), record);
            }
        }
        DFStatement::Explain(explain) => visit_statement(&explain.statement, record),
        DFStatement::CreateExternalTable(_) | DFStatement::Reset(_) => {}
    }
}

fn visit_numbers(node: &impl Visit, record: &mut impl FnMut(&str)) {
    let _ = visit_expressions(node, |expr| {
        if let SqlExpr::Value(value) = expr
            && let Value::Number(token, _) = &value.value
        {
            record(token);
        }
        ControlFlow::<()>::Continue(())
    });
}

/// For a number token `DataFusion` plans as `Float64`: the bits of that `f64`'s
/// magnitude, the token as a plain decimal, and whether the `f64`'s shortest
/// spelling recovers it.
fn token_spelling(token: &str) -> Option<(u64, String, bool)> {
    let token = token.replace('_', "");
    // Integer tokens plan as Int64 / UInt64, not as floats.
    if token.parse::<u64>().is_ok() {
        return None;
    }
    let value = token
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())?;
    let spelling = plain_decimal(&token)?;
    let recovered = plain_decimal(&value.to_string()).is_some_and(|shortest| shortest == spelling);
    Some((value.abs().to_bits(), spelling, recovered))
}

/// An unsigned decimal token (`012.50`, `1.5e-3`, `.5`) as a canonical plain
/// decimal: no exponent, no leading integer zeros, no trailing fraction zeros.
fn plain_decimal(token: &str) -> Option<String> {
    let (mantissa, exponent) = match token.find(['e', 'E']) {
        Some(at) => (&token[..at], token[at + 1..].parse::<i64>().ok()?),
        None => (token, 0),
    };
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if !integer
        .bytes()
        .chain(fraction.bytes())
        .all(|b| b.is_ascii_digit())
        || integer.len() + fraction.len() == 0
    {
        return None;
    }
    let digits = format!("{integer}{fraction}");
    // `digits` scaled by 10^-scale is the value. `checked_sub` rejects an
    // exponent of `i64::MIN`, which overflows the subtraction.
    let scale = i64::try_from(fraction.len()).ok()?.checked_sub(exponent)?;
    // Bound the expansion: a token this far out of range is not finite anyway.
    if scale.unsigned_abs() > 400 {
        return None;
    }
    let (integer, fraction) = if scale <= 0 {
        let zeros = usize::try_from(-scale).ok()?;
        (format!("{digits}{}", "0".repeat(zeros)), String::new())
    } else {
        let scale = usize::try_from(scale).ok()?;
        let padded = if digits.len() <= scale {
            format!("{}{digits}", "0".repeat(scale - digits.len() + 1))
        } else {
            digits
        };
        let (integer, fraction) = padded.split_at(padded.len() - scale);
        (integer.to_string(), fraction.to_string())
    };
    let integer = match integer.trim_start_matches('0') {
        "" => "0",
        trimmed => trimmed,
    };
    let fraction = fraction.trim_end_matches('0');
    Some(if fraction.is_empty() {
        integer.to_string()
    } else {
        format!("{integer}.{fraction}")
    })
}

/// Every decimal with at most `max_digits` significant digits reads as a
/// distinct value of that float width, so a float whose shortest spelling is
/// that short can only have come from that spelling (or one that differs below
/// the float's precision). Longer spellings — `1e30` is
/// `1000000000000000019884624838656.0` as well — are ambiguous, and those keep
/// the cast's numeric conversion. `max_digits` is [`f64::DIGITS`],
/// [`f32::DIGITS`], or 3 for `f16`.
fn is_unambiguous_spelling(spelling: &str, max_digits: usize) -> bool {
    let unsigned = spelling.trim_start_matches('-');
    let integer_digits = unsigned
        .split_once('.')
        .map_or(unsigned, |(integer, _)| integer)
        .trim_start_matches('0')
        .len();
    let significant_digits = unsigned
        .chars()
        .filter(char::is_ascii_digit)
        .collect::<String>()
        .trim_start_matches('0')
        .trim_end_matches('0')
        .len();
    integer_digits <= max_digits && significant_digits <= max_digits
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Decimal128Array, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    #[test]
    fn float_literals_cast_to_the_decimal_they_spell() {
        for (value, precision, scale, expected) in [
            (
                12_345_678.90_f64,
                30_u8,
                15_i8,
                12_345_678_900_000_000_000_000_i128,
            ),
            (4.07, 30, 15, 4_070_000_000_000_000),
            (4.07, 10, 2, 407),
            (0.1, 38, 10, 1_000_000_000),
            (-12_345_678.9, 30, 15, -12_345_678_900_000_000_000_000),
            (100_000.0, 10, 2, 10_000_000),
        ] {
            assert_eq!(
                exact_decimal(
                    &ScalarValue::Float64(Some(value)),
                    &DataType::Decimal128(precision, scale)
                ),
                Some(ScalarValue::Decimal128(Some(expected), precision, scale)),
                "{value} as Decimal128({precision}, {scale})"
            );
        }
    }

    #[test]
    fn literals_that_do_not_fit_the_scale_or_precision_keep_the_cast() {
        // More fraction digits than the scale: the cast's rounding decides.
        assert_eq!(
            exact_decimal(
                &ScalarValue::Float64(Some(1.234)),
                &DataType::Decimal128(10, 2)
            ),
            None
        );
        // Overflows the precision: the cast's error decides.
        assert_eq!(
            exact_decimal(
                &ScalarValue::Float64(Some(123_456.0)),
                &DataType::Decimal128(4, 0)
            ),
            None
        );
        // More significant digits than an f64 pins down: several decimals share
        // this float, so the cast's numeric conversion decides.
        assert_eq!(
            exact_decimal(
                &ScalarValue::Float64(Some(1e30)),
                &DataType::Decimal128(38, 0)
            ),
            None
        );
        assert_eq!(
            exact_decimal(
                &ScalarValue::Float64(Some(0.1 + 0.2)),
                &DataType::Decimal128(38, 18)
            ),
            None
        );
        assert_eq!(
            exact_decimal(
                &ScalarValue::Float64(Some(f64::NAN)),
                &DataType::Decimal128(10, 2)
            ),
            None
        );
        assert_eq!(
            exact_decimal(&ScalarValue::Float64(None), &DataType::Decimal128(10, 2)),
            None
        );
        // Float32 has only 6 reliable decimal digits; 123456.79 displays as
        // that spelling but stores 123456.7890625, so rewriting would compare
        // against the wrong decimal.
        assert_eq!(
            exact_decimal(
                &ScalarValue::Float32(Some(123_456.79)),
                &DataType::Decimal128(38, 10)
            ),
            None
        );
        // A short Float32 spelling within f32::DIGITS still rewrites.
        assert_eq!(
            exact_decimal(
                &ScalarValue::Float32(Some(4.07)),
                &DataType::Decimal128(10, 2)
            ),
            Some(ScalarValue::Decimal128(Some(407), 10, 2))
        );
    }

    /// Every cent value up to a large bound must survive the rewrite exactly —
    /// the plain cast gets about one in five of these wrong.
    #[test]
    fn every_cent_value_round_trips() {
        for cents in (0_i128..2_000_000).step_by(7) {
            let spelling = format!("{}.{:02}", cents / 100, cents % 100);
            let value: f64 = spelling.parse().expect("valid float");
            assert_eq!(
                exact_decimal(
                    &ScalarValue::Float64(Some(value)),
                    &DataType::Decimal128(30, 15)
                ),
                Some(ScalarValue::Decimal128(
                    Some(cents * 10_i128.pow(13)),
                    30,
                    15
                )),
                "{spelling}"
            );
        }
    }

    /// `i64::MIN` as an exponent overflows `fraction_len - exponent` before the
    /// expansion bound can reject it. Collection visits every numeric token.
    #[test]
    fn an_extreme_exponent_does_not_overflow_the_scale() {
        assert_eq!(plain_decimal("1e-9223372036854775808"), None);
        assert_eq!(plain_decimal("0.0e-9223372036854775808"), None);
        assert_eq!(token_spelling("1e-9223372036854775808"), None);
    }

    /// A spelling the cast does not consume has to leave the literal.
    /// `transform_down` visits the literal after the cast, and a non-cast
    /// expression is only that literal.
    #[test]
    fn a_spelling_the_cast_does_not_use_is_removed() {
        use datafusion::common::tree_node::TreeNodeRecursion;
        use datafusion::logical_expr::{Operator, expr::BinaryExpr};

        let marked = Expr::Literal(
            ScalarValue::Float64(Some(4.07)),
            Some(FieldMetadata::new(BTreeMap::from([(
                FLOAT_LITERAL_SPELLING_KEY.to_string(),
                "4.0700000000000003".to_string(),
            )]))),
        );
        let has_spelling_key = |expr: &Expr| {
            let mut leftover = false;
            expr.apply(|expr| {
                if let Expr::Literal(_, Some(metadata)) = expr {
                    leftover |= metadata.inner().contains_key(FLOAT_LITERAL_SPELLING_KEY);
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .expect("walk");
            leftover
        };
        let rewritten = |expr: Expr| {
            expr.transform_down(|expr| Ok(rewrite_expr(expr)))
                .expect("rewrite")
                .data
        };

        let cast = rewritten(Expr::Cast(Cast::new(
            Box::new(marked.clone()),
            DataType::Int64,
        )));
        assert!(!has_spelling_key(&cast), "cast: {cast:?}");

        let sum = rewritten(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(marked),
            Operator::Plus,
            Box::new(Expr::Literal(ScalarValue::Int64(Some(1)), None)),
        )));
        assert!(!has_spelling_key(&sum), "sum: {sum:?}");
    }

    #[test]
    fn plain_decimal_normalizes_token_spellings() {
        for (token, expected) in [
            ("4.07", Some("4.07")),
            ("004.0700", Some("4.07")),
            (".5", Some("0.5")),
            ("100.", Some("100")),
            ("1.5e3", Some("1500")),
            ("15E-4", Some("0.0015")),
            ("0.0", Some("0")),
            ("1e", None),
            ("1.2.3", None),
        ] {
            assert_eq!(plain_decimal(token).as_deref(), expected, "{token}");
        }
    }

    #[test]
    fn round_to_scale_rounds_half_away_from_zero() {
        for (spelling, scale, expected) in [
            ("4.07", 4, Some("4.07")),
            ("4.0750", 2, Some("4.08")),
            ("4.0749", 2, Some("4.07")),
            ("-4.075", 2, Some("-4.08")),
            ("9.995", 2, Some("10.00")),
            ("0.5", 0, Some("1")),
            ("12", 0, Some("12")),
            ("", 2, None),
            ("1.x", 2, None),
        ] {
            assert_eq!(
                round_to_scale(spelling, scale).as_deref(),
                expected,
                "{spelling} at {scale}"
            );
        }
    }

    #[test]
    fn only_tokens_the_float_loses_are_recorded() {
        // At most f64::DIGITS significant digits: the float recovers it.
        assert_eq!(
            token_spelling("12345678.90"),
            Some((12_345_678.9_f64.to_bits(), "12345678.9".to_string(), true))
        );
        // Same f64 bits as 4.07, but a different decimal.
        assert_eq!(
            token_spelling("4.0700000000000003"),
            Some((4.07_f64.to_bits(), "4.0700000000000003".to_string(), false))
        );
        // Integers plan as Int64 / UInt64.
        assert_eq!(token_spelling("42"), None);
    }

    /// The f64 for `4.0700000000000003` is the f64 for `4.07`, so only the
    /// recorded token spelling can tell them apart at a scale that can.
    #[test]
    fn a_recorded_token_spelling_wins_over_the_float() {
        let data_type = DataType::Decimal128(38, 17);
        let spelled = |spelling: &str| {
            Expr::Cast(Cast::new(
                Box::new(Expr::Literal(
                    ScalarValue::Float64(Some(4.07)),
                    Some(FieldMetadata::new(BTreeMap::from([(
                        FLOAT_LITERAL_SPELLING_KEY.to_string(),
                        spelling.to_string(),
                    )]))),
                )),
                data_type.clone(),
            ))
        };
        assert_eq!(
            rewrite_expr(spelled("4.0700000000000003")).data,
            Expr::Literal(
                ScalarValue::Decimal128(Some(407_000_000_000_000_030), 38, 17),
                None
            )
        );
        assert_eq!(
            rewrite_expr(spelled("-4.0700000000000003")).data,
            Expr::Literal(
                ScalarValue::Decimal128(Some(-407_000_000_000_000_030), 38, 17),
                None
            )
        );
        // More fraction digits than the scale: the token is rounded to the
        // scale the way a decimal cast rounds, not through the binary value.
        let at_scale = |spelling: &str, scale: i8| {
            rewrite_expr(Expr::Cast(Cast::new(
                Box::new(Expr::Literal(
                    ScalarValue::Float64(spelling.parse().ok()),
                    Some(FieldMetadata::new(BTreeMap::from([(
                        FLOAT_LITERAL_SPELLING_KEY.to_string(),
                        spelling.to_string(),
                    )]))),
                )),
                DataType::Decimal128(38, scale),
            )))
            .data
        };
        assert_eq!(
            at_scale("4.0700000000000003", 15),
            Expr::Literal(
                ScalarValue::Decimal128(Some(4_070_000_000_000_000), 38, 15),
                None
            )
        );
        assert_eq!(
            at_scale("4.0700000000000005", 16),
            Expr::Literal(
                ScalarValue::Decimal128(Some(40_700_000_000_000_005), 38, 16),
                None
            )
        );
        assert_eq!(
            at_scale("-9.99999999999999999", 2),
            Expr::Literal(ScalarValue::Decimal128(Some(-1000), 38, 2), None)
        );
        // Spelled two ways in one statement: no spelling is known.
        assert_eq!(
            exact_decimal_from_spelling("", &data_type),
            None,
            "an ambiguous token leaves the cast"
        );
        // Without a recorded spelling the float's shortest spelling is used.
        assert_eq!(
            rewrite_expr(Expr::Cast(Cast::new(
                Box::new(Expr::Literal(ScalarValue::Float64(Some(4.07)), None)),
                data_type,
            )))
            .data,
            Expr::Literal(
                ScalarValue::Decimal128(Some(407_000_000_000_000_000), 38, 17),
                None
            )
        );
    }

    /// The ids of the rows `predicate` keeps from a `Decimal128(38, 17)` column
    /// holding 4.07 (id 1) and 4.0700000000000003 (id 2), planned the way the
    /// runtime plans SQL: token spellings retained before analysis. Without
    /// `exact`, plain `DataFusion` plans it.
    async fn matching_ids_with(predicate: &str, exact: bool) -> Vec<i64> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("dec", DataType::Decimal128(38, 17), false),
        ]));
        let values = Decimal128Array::from(vec![407_000_000_000_000_000, 407_000_000_000_000_030])
            .with_precision_and_scale(38, 17)
            .expect("valid decimal");
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::new(values)],
        )
        .expect("valid batch");

        let ctx = SessionContext::new();
        if exact {
            ctx.add_analyzer_rule(Arc::new(ExactFloatLiteralDecimalCast::new()));
        }
        ctx.register_batch("t", batch).expect("register");
        let state = ctx.state();
        let sql = format!("SELECT id FROM t WHERE {predicate} ORDER BY id");
        let statement = state
            .sql_to_statement(&sql, &datafusion::common::config::Dialect::PostgreSQL)
            .expect("parse");
        let spellings = if exact {
            FloatLiteralSpellings::collect(&statement)
        } else {
            FloatLiteralSpellings::default()
        };
        let plan = spellings
            .retain(state.statement_to_plan(statement).await.expect("plan"))
            .expect("retain");
        let batches = ctx
            .execute_logical_plan(plan)
            .await
            .expect("execute")
            .collect()
            .await
            .expect("collect");
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("ids")
                    .values()
                    .to_vec()
            })
            .collect()
    }

    async fn matching_ids(predicate: &str) -> Vec<i64> {
        matching_ids_with(predicate, true).await
    }

    #[tokio::test]
    async fn a_long_literal_matches_the_decimal_it_spells_not_its_float() {
        assert_eq!(matching_ids("dec = 4.07").await, vec![1]);
        assert_eq!(
            matching_ids("dec = 4.0700000000000003").await,
            vec![2],
            "the token is 4.0700000000000003, even though its f64 is 4.07's"
        );
        assert_eq!(matching_ids("-dec = -4.0700000000000003").await, vec![2]);
        // Both spellings in one statement share one f64, and the plan does not
        // record which token each literal came from: neither spelling is
        // known, so each comparison keeps the binary cast it had without the
        // rule (see the module docs).
        let ambiguous = "dec = 4.0700000000000003 OR dec = 4.07";
        assert_eq!(
            matching_ids(ambiguous).await,
            matching_ids_with(ambiguous, false).await,
        );
    }

    #[tokio::test]
    async fn a_projected_long_literal_keeps_its_field_metadata_clean() {
        let ctx = SessionContext::new();
        ctx.add_analyzer_rule(Arc::new(ExactFloatLiteralDecimalCast::new()));
        let state = ctx.state();
        let sql = "SELECT 4.0700000000000003 AS bare, \
                   CAST(4.0700000000000003 AS DECIMAL(38, 17)) AS exact";
        let statement = state
            .sql_to_statement(sql, &datafusion::common::config::Dialect::PostgreSQL)
            .expect("parse");
        let plan = FloatLiteralSpellings::collect(&statement)
            .retain(state.statement_to_plan(statement).await.expect("plan"))
            .expect("retain");
        let batches = ctx
            .execute_logical_plan(plan)
            .await
            .expect("execute")
            .collect()
            .await
            .expect("collect");
        let batch = &batches[0];
        for field in batch.schema().fields() {
            assert!(
                !field.metadata().contains_key(FLOAT_LITERAL_SPELLING_KEY),
                "{field:?}"
            );
        }
        let exact = batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal");
        assert_eq!(exact.value(0), 407_000_000_000_000_030);
    }

    async fn planned(sql: &str) -> RecordBatch {
        let ctx = SessionContext::new();
        ctx.add_analyzer_rule(Arc::new(ExactFloatLiteralDecimalCast::new()));
        let state = ctx.state();
        let statement = state
            .sql_to_statement(sql, &datafusion::common::config::Dialect::PostgreSQL)
            .expect("parse");
        let plan = FloatLiteralSpellings::collect(&statement)
            .retain(state.statement_to_plan(statement).await.expect("plan"))
            .expect("retain");
        let batches = ctx
            .execute_logical_plan(plan)
            .await
            .expect("execute")
            .collect()
            .await
            .expect("collect");
        batches.into_iter().next().expect("one batch")
    }

    /// A cast inside `VALUES` is not a bare entry. Skipping the whole `Values`
    /// plan drops the token, and `4.0700000000000003` is then cast as `4.07`.
    #[tokio::test]
    async fn a_values_cast_keeps_the_token_not_the_float() {
        let batch = planned("VALUES (CAST(4.0700000000000003 AS DECIMAL(38, 17)))").await;
        for field in batch.schema().fields() {
            assert!(
                !field.metadata().contains_key(FLOAT_LITERAL_SPELLING_KEY),
                "{field:?}"
            );
        }
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal");
        assert_eq!(column.value(0), 407_000_000_000_000_030);
    }

    /// A bare `VALUES` entry names the output column, so its spelling stays off
    /// the field.
    #[tokio::test]
    async fn a_bare_values_literal_keeps_its_field_metadata_clean() {
        let batch = planned("VALUES (4.0700000000000003)").await;
        for field in batch.schema().fields() {
            assert!(
                !field.metadata().contains_key(FLOAT_LITERAL_SPELLING_KEY),
                "{field:?}"
            );
        }
    }

    async fn count_matches(sql: &str, params: Option<Vec<ScalarValue>>, scale: i8) -> i64 {
        let precision = 10 + u8::try_from(scale).expect("scale");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "dec",
            DataType::Decimal128(precision, scale),
            false,
        )]));
        let unit = 10_i128.pow(u32::try_from(scale).expect("scale"));
        let values = Decimal128Array::from(vec![
            12_345_678 * unit + 9 * unit / 10,
            4 * unit + 7 * unit / 100,
        ])
        .with_precision_and_scale(precision, scale)
        .expect("valid decimal");
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(values)]).expect("valid batch");

        let ctx = SessionContext::new();
        ctx.add_analyzer_rule(Arc::new(ExactFloatLiteralDecimalCast::new()));
        ctx.register_batch("t", batch).expect("register");
        let mut df = ctx.sql(sql).await.expect("plan");
        if let Some(params) = params {
            df = df.with_param_values(params).expect("bind");
        }
        let batches = df.collect().await.expect("execute");
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count")
            .value(0)
    }

    #[tokio::test]
    async fn decimal_column_matches_a_fractional_literal_across_scales() {
        for scale in [2_i8, 4, 6, 15] {
            for literal in ["12345678.90", "4.07"] {
                let sql = format!("SELECT count(*) FROM t WHERE dec = {literal}");
                assert_eq!(
                    count_matches(&sql, None, scale).await,
                    1,
                    "{sql}, scale {scale}"
                );
            }
        }
    }

    #[tokio::test]
    async fn decimal_column_matches_a_bound_float_parameter_across_scales() {
        for scale in [2_i8, 4, 6, 15] {
            for value in [12_345_678.90_f64, 4.07] {
                let count = count_matches(
                    "SELECT count(*) FROM t WHERE dec = $1",
                    Some(vec![ScalarValue::Float64(Some(value))]),
                    scale,
                )
                .await;
                assert_eq!(count, 1, "$1 = {value}, scale {scale}");
            }
        }
    }

    /// Flight SQL binds `$1` as `CAST($1 AS DOUBLE)`. Casting a float to its own
    /// type keeps its value, so the bound float still casts through its spelling.
    #[tokio::test]
    async fn decimal_column_matches_a_bound_float_parameter_under_an_identity_cast() {
        for scale in [2_i8, 4, 6, 15] {
            for value in [12_345_678.90_f64, 4.07] {
                let count = count_matches(
                    "SELECT count(*) FROM t WHERE dec = CAST($1 AS DOUBLE)",
                    Some(vec![ScalarValue::Float64(Some(value))]),
                    scale,
                )
                .await;
                assert_eq!(count, 1, "CAST($1 AS DOUBLE) = {value}, scale {scale}");
            }
            let count = count_matches(
                "SELECT count(*) FROM t WHERE dec = CAST($1 AS FLOAT)",
                Some(vec![ScalarValue::Float32(Some(4.07))]),
                scale,
            )
            .await;
            assert_eq!(count, 1, "CAST($1 AS FLOAT) = 4.07f32, scale {scale}");
        }
    }
}
