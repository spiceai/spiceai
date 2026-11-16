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

//! Predicates over a JSON response body, for the `http` step's `expect: { json }`.
//!
//! Each assertion names the values it checks with a JSON Pointer (RFC 6901),
//! where a `*` segment stands for every element of an array or every member of
//! an object, and states what those values must be:
//!
//! ```yaml
//! json:
//!   - { pointer: /0/n, equals: 7 }
//!   - { pointer: /*/task, equals: sql_query }          # every row
//!   - { pointer: /*/labels/protocol, equals: http, count: '>=1' }
//!   - { pointer: /0/start_time, matches: '^\d{4}-' }
//!   - { pointer: '', length: 3 }                        # the whole body
//!   - { pointer: /0/missing, exists: false }
//! ```
//!
//! Without `count`, every value the pointer reaches must satisfy the
//! predicates, and it must reach at least one, so a pointer that matches
//! nothing fails rather than passing vacuously.

use anyhow::{Context, Result, bail, ensure};
use serde_json::{Map, Number, Value};
use std::cmp::Ordering;

use crate::script::LineCount;

/// One assertion: the values at `pointer`, and what they must be.
#[derive(Debug, Clone)]
pub struct JsonAssertion {
    pointer: String,
    predicates: Vec<Predicate>,
    /// How many of the reached values must satisfy every predicate; `None`
    /// means all of them, and at least one.
    count: Option<LineCount>,
    /// `exists: false`: the pointer must reach nothing.
    absent: bool,
}

#[derive(Debug, Clone)]
enum Predicate {
    Equals(Value),
    Matches(regex::Regex),
    Type(JsonType),
    Length(LineCount),
    Above(Number),
    Below(Number),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum JsonType {
    Null,
    Boolean,
    Number,
    Integer,
    String,
    Array,
    Object,
}

impl JsonType {
    fn of(value: &Value) -> &'static str {
        match value {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Number(n) if n.is_i64() || n.is_u64() => "integer",
            Value::Number(_) => "number",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        }
    }

    fn holds(self, value: &Value) -> bool {
        match self {
            Self::Null => value.is_null(),
            Self::Boolean => value.is_boolean(),
            Self::Number => value.is_number(),
            Self::Integer => value.as_number().is_some_and(|n| n.is_i64() || n.is_u64()),
            Self::String => value.is_string(),
            Self::Array => value.is_array(),
            Self::Object => value.is_object(),
        }
    }
}

impl JsonAssertion {
    /// Parses one entry of `json:`.
    pub fn parse(value: Value) -> Result<Self> {
        let Value::Object(mut map) = value else {
            bail!("each `json` assertion is a mapping with a `pointer`");
        };
        let pointer = match map.remove("pointer") {
            Some(Value::String(p)) => p,
            Some(_) => {
                bail!("`pointer` must be a string, such as /0/name or '' for the whole body")
            }
            None => bail!("each `json` assertion needs a `pointer`"),
        };
        ensure!(
            pointer.is_empty() || pointer.starts_with('/'),
            "`pointer` '{pointer}' must be empty or start with '/' (a JSON Pointer)"
        );
        let mut predicates = Vec::new();
        if let Some(v) = map.remove("equals") {
            predicates.push(Predicate::Equals(v));
        }
        if let Some(v) = map.remove("matches") {
            let Value::String(pattern) = v else {
                bail!("`matches` must be a regular expression");
            };
            predicates.push(Predicate::Matches(
                regex::Regex::new(&pattern).with_context(|| format!("`matches`: /{pattern}/"))?,
            ));
        }
        if let Some(v) = map.remove("type") {
            predicates.push(Predicate::Type(serde_json::from_value(v).context(
                "`type` is one of null, boolean, number, integer, string, array, object",
            )?));
        }
        if let Some(v) = map.remove("length") {
            predicates.push(Predicate::Length(LineCount::parse(&v)?));
        }
        for (key, make) in [
            ("above", Predicate::Above as fn(Number) -> Predicate),
            ("below", Predicate::Below),
        ] {
            if let Some(v) = map.remove(key) {
                let Value::Number(bound) = v else {
                    bail!("`{key}` must be a number");
                };
                predicates.push(make(bound));
            }
        }
        let count = map
            .remove("count")
            .map(|v| LineCount::parse(&v))
            .transpose()?;
        let absent = match map.remove("exists") {
            None | Some(Value::Bool(true)) => false,
            Some(Value::Bool(false)) => true,
            Some(_) => bail!("`exists` must be true or false"),
        };
        if let Some(key) = map.keys().next() {
            bail!("unknown `json` assertion option `{key}`");
        }
        ensure!(
            !absent || (predicates.is_empty() && count.is_none()),
            "`exists: false` takes no other predicate or `count`"
        );
        Ok(Self {
            pointer,
            predicates,
            count,
            absent,
        })
    }

    /// Checks `body`, returning what is wrong, if anything.
    pub fn check(&self, body: &Value) -> Option<String> {
        let reached = resolve(body, &self.pointer);
        if self.absent {
            return (!reached.is_empty()).then(|| {
                format!(
                    "{} should reach nothing, reached {}",
                    self.shown_pointer(),
                    shown(&reached)
                )
            });
        }
        let failing: Vec<(&Value, String)> = reached
            .iter()
            .filter_map(|v| self.first_failure(v).map(|why| (*v, why)))
            .collect();
        let passing = reached.len() - failing.len();
        match self.count {
            None if reached.is_empty() => Some(format!("{} reached nothing", self.shown_pointer())),
            None => failing.first().map(|(_, why)| {
                format!(
                    "{}: {why} ({} of {} values fail)",
                    self.shown_pointer(),
                    failing.len(),
                    reached.len()
                )
            }),
            Some(count) => (!count.holds(passing)).then(|| {
                format!(
                    "{}: {passing} of {} values satisfy it, expected {count}{}",
                    self.shown_pointer(),
                    reached.len(),
                    failing
                        .first()
                        .map(|(_, why)| format!("; first failure: {why}"))
                        .unwrap_or_default()
                )
            }),
        }
    }

    fn shown_pointer(&self) -> String {
        format!("json '{}'", self.pointer)
    }

    fn first_failure(&self, value: &Value) -> Option<String> {
        self.predicates.iter().find_map(|p| p.failure(value))
    }
}

impl Predicate {
    fn failure(&self, value: &Value) -> Option<String> {
        match self {
            Self::Equals(want) => (!json_equal(value, want))
                .then(|| format!("expected {want}, got {}", one_value(value))),
            Self::Matches(pattern) => {
                let text = match value {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                (!pattern.is_match(&text))
                    .then(|| format!("{} does not match /{pattern}/", one_value(value)))
            }
            Self::Type(want) => (!want.holds(value)).then(|| {
                format!(
                    "expected {}, got {} {}",
                    format!("{want:?}").to_ascii_lowercase(),
                    JsonType::of(value),
                    one_value(value)
                )
            }),
            Self::Length(want) => {
                let len = match value {
                    Value::Array(a) => a.len(),
                    Value::Object(o) => o.len(),
                    Value::String(s) => s.chars().count(),
                    other => {
                        return Some(format!(
                            "length of {} {} is undefined",
                            JsonType::of(other),
                            one_value(other)
                        ));
                    }
                };
                (!want.holds(len)).then(|| format!("length {len}, expected {want}"))
            }
            Self::Above(bound) => number_failure(value, Ordering::Greater, "above", bound),
            Self::Below(bound) => number_failure(value, Ordering::Less, "below", bound),
        }
    }
}

fn number_failure(value: &Value, want: Ordering, what: &str, bound: &Number) -> Option<String> {
    match value {
        Value::Number(n) if compare_numbers(n, bound) == Some(want) => None,
        Value::Number(n) => Some(format!("{n} is not {what} {bound}")),
        _ => Some(format!(
            "expected a number {what} {bound}, got {} {}",
            JsonType::of(value),
            one_value(value)
        )),
    }
}

/// Equality as the script means it: numbers compare by value (`7` equals
/// `7.0`), and containers compare member by member under the same rule.
fn json_equal(got: &Value, want: &Value) -> bool {
    match (got, want) {
        (Value::Number(a), Value::Number(b)) => compare_numbers(a, b) == Some(Ordering::Equal),
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(x, y)| json_equal(x, y))
        }
        (Value::Object(a), Value::Object(b)) => objects_equal(a, b),
        _ => got == want,
    }
}

/// Orders two JSON numbers by exact value. An integer is never rounded to a
/// float: an integer and a float compare through the float's whole part, which
/// converts to `i128` exactly, and then its fraction.
fn compare_numbers(a: &Number, b: &Number) -> Option<Ordering> {
    match (integer(a), integer(b)) {
        (Some(a), Some(b)) => Some(a.cmp(&b)),
        (Some(a), None) => b
            .as_f64()
            .and_then(|b| compare_float(b, a))
            .map(Ordering::reverse),
        (None, Some(b)) => a.as_f64().and_then(|a| compare_float(a, b)),
        (None, None) => a.as_f64()?.partial_cmp(&b.as_f64()?),
    }
}

/// A JSON integer, signed or unsigned, without rounding.
fn integer(number: &Number) -> Option<i128> {
    number
        .as_i64()
        .map(i128::from)
        .or_else(|| number.as_u64().map(i128::from))
}

/// Orders `float` against the integer `int` exactly.
fn compare_float(float: f64, int: i128) -> Option<Ordering> {
    // 2^127; `i128::MAX as f64` rounds up to it, so the upper bound is exclusive.
    const LIMIT: f64 = 170_141_183_460_469_231_731_687_303_715_884_105_728.0;
    if float.is_nan() {
        return None;
    }
    if float >= LIMIT {
        return Some(Ordering::Greater);
    }
    if float < -LIMIT {
        return Some(Ordering::Less);
    }
    #[expect(
        clippy::cast_possible_truncation,
        reason = "a whole number in [-2^127, 2^127) converts to i128 exactly"
    )]
    let whole = float.trunc() as i128;
    Some(whole.cmp(&int).then(float.fract().partial_cmp(&0.0)?))
}

fn objects_equal(a: &Map<String, Value>, b: &Map<String, Value>) -> bool {
    a.len() == b.len()
        && a.iter()
            .all(|(k, v)| b.get(k).is_some_and(|w| json_equal(v, w)))
}

/// Every value `pointer` reaches in `root`, where a `*` segment fans out over
/// an array's elements or an object's members.
fn resolve<'a>(root: &'a Value, pointer: &str) -> Vec<&'a Value> {
    let mut current = vec![root];
    if pointer.is_empty() {
        return current;
    }
    for raw in pointer[1..].split('/') {
        let segment = raw.replace("~1", "/").replace("~0", "~");
        let mut next = Vec::new();
        for value in current {
            match (value, raw) {
                (Value::Array(items), "*") => next.extend(items),
                (Value::Object(members), "*") => next.extend(members.values()),
                (Value::Array(items), _) => {
                    if let Some(item) = array_index(&segment).and_then(|i| items.get(i)) {
                        next.push(item);
                    }
                }
                (Value::Object(members), _) => {
                    if let Some(member) = members.get(&segment) {
                        next.push(member);
                    }
                }
                _ => {}
            }
        }
        current = next;
    }
    current
}

/// A JSON Pointer array index: `0`, or digits without a leading zero (RFC 6901).
/// `parse::<usize>` alone would also take `01` and `+1`.
fn array_index(segment: &str) -> Option<usize> {
    let canonical = segment == "0"
        || (segment.bytes().all(|b| b.is_ascii_digit()) && !segment.starts_with('0'));
    if canonical {
        segment.parse().ok()
    } else {
        None
    }
}

fn one_value(value: &Value) -> String {
    let text = value.to_string();
    if text.chars().count() > 200 {
        format!("{}…", text.chars().take(200).collect::<String>())
    } else {
        text
    }
}

fn shown(values: &[&Value]) -> String {
    let first: Vec<String> = values.iter().take(3).map(|v| one_value(v)).collect();
    let more = values.len().saturating_sub(3);
    if more > 0 {
        format!("{} and {more} more", first.join(", "))
    } else {
        first.join(", ")
    }
}

/// Parses `body` as JSON and runs every assertion, returning the failures.
pub fn check_all(assertions: &[JsonAssertion], body: &str) -> Vec<String> {
    if assertions.is_empty() {
        return Vec::new();
    }
    let parsed: Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => {
            let text: String = body.chars().take(200).collect();
            return vec![format!("body is not JSON ({e}): {text}")];
        }
    };
    assertions.iter().filter_map(|a| a.check(&parsed)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn assertion(yaml: &str) -> JsonAssertion {
        let value: Value = yaml::from_str(yaml).expect("assertion yaml");
        JsonAssertion::parse(value).expect("assertion parses")
    }

    fn rows() -> Value {
        json!([
            { "task": "sql_query", "n": 7, "ms": 1.5, "labels": { "protocol": "http" }, "err": null },
            { "task": "sql_query", "n": 7.0, "ms": 0.25, "labels": { "protocol": "flight" }, "err": null },
        ])
    }

    #[test]
    fn every_reached_value_must_satisfy_the_predicates() {
        let body = rows();
        assert_eq!(
            assertion("{ pointer: /*/task, equals: sql_query }").check(&body),
            None
        );
        assert_eq!(assertion("{ pointer: /*/n, equals: 7 }").check(&body), None);
        assert_eq!(
            assertion("{ pointer: /*/labels/protocol, equals: http }").check(&body),
            Some(
                "json '/*/labels/protocol': expected \"http\", got \"flight\" (1 of 2 values fail)"
                    .to_string()
            )
        );
        assert_eq!(
            assertion("{ pointer: /*/labels/protocol, equals: http, count: 1 }").check(&body),
            None
        );
        assert_eq!(
            assertion("{ pointer: /*/labels/protocol, equals: grpc, count: '>=1' }").check(&body),
            Some(
                "json '/*/labels/protocol': 0 of 2 values satisfy it, expected >= 1; first failure: expected \"grpc\", got \"http\""
                    .to_string()
            )
        );
    }

    #[test]
    fn an_array_index_is_zero_or_digits_without_a_leading_zero_or_sign() {
        let body = rows();
        assert_eq!(
            assertion("{ pointer: /1/ms, equals: 0.25 }").check(&body),
            None
        );
        for index in ["01", "+1", "1 ", "-0"] {
            assert_eq!(
                assertion(&format!("{{ pointer: '/{index}/ms', exists: false }}")).check(&body),
                None,
                "index '{index}'"
            );
        }
    }

    #[test]
    fn a_pointer_that_reaches_nothing_fails_unless_absence_is_the_point() {
        let body = rows();
        assert_eq!(
            assertion("{ pointer: /*/nope, equals: 1 }").check(&body),
            Some("json '/*/nope' reached nothing".to_string())
        );
        assert_eq!(
            assertion("{ pointer: /*/nope, exists: false }").check(&body),
            None
        );
        assert_eq!(
            assertion("{ pointer: /0/err, exists: false }").check(&body),
            Some("json '/0/err' should reach nothing, reached null".to_string())
        );
        assert_eq!(
            assertion("{ pointer: /*/err, equals: null }").check(&body),
            None
        );
    }

    #[test]
    fn types_lengths_ranges_and_patterns() {
        let body = rows();
        assert_eq!(
            assertion("{ pointer: '', length: 2, type: array }").check(&body),
            None
        );
        assert_eq!(
            assertion("{ pointer: '', length: '>=3' }").check(&body),
            Some("json '': length 2, expected >= 3 (1 of 1 values fail)".to_string())
        );
        assert_eq!(
            assertion("{ pointer: /0/n, type: integer }").check(&body),
            None
        );
        assert_eq!(
            assertion("{ pointer: /*/ms, type: integer }").check(&body),
            Some("json '/*/ms': expected integer, got number 1.5 (2 of 2 values fail)".to_string())
        );
        assert_eq!(
            assertion("{ pointer: /*/ms, above: 0, below: 2 }").check(&body),
            None
        );
        assert_eq!(
            assertion("{ pointer: /*/task, above: 0 }").check(&body),
            Some("json '/*/task': expected a number above 0, got string \"sql_query\" (2 of 2 values fail)".to_string())
        );
        assert_eq!(
            assertion("{ pointer: /0/labels, matches: 'protocol' }").check(&body),
            None
        );
        assert_eq!(
            assertion("{ pointer: /*/task, matches: '^sql_' }").check(&body),
            None
        );
    }

    #[test]
    fn pointer_escapes_and_wildcard_over_objects() {
        let body = json!({ "a/b": { "x": 1, "y": 2 }, "t~": [] });
        assert_eq!(
            assertion("{ pointer: /a~1b/*, above: 0 }").check(&body),
            None
        );
        assert_eq!(assertion("{ pointer: /t~0, length: 0 }").check(&body), None);
    }

    #[test]
    fn rejects_malformed_assertions() {
        for (yaml, error) in [
            ("{ equals: 1 }", "each `json` assertion needs a `pointer`"),
            (
                "{ pointer: a, equals: 1 }",
                "`pointer` 'a' must be empty or start with '/' (a JSON Pointer)",
            ),
            (
                "{ pointer: /a, exists: false, equals: 1 }",
                "`exists: false` takes no other predicate or `count`",
            ),
            (
                "{ pointer: /a, equal: 1 }",
                "unknown `json` assertion option `equal`",
            ),
        ] {
            let value: Value = yaml::from_str(yaml).expect("yaml");
            let err = JsonAssertion::parse(value).expect_err("rejected");
            assert_eq!(err.to_string(), error, "{yaml}");
        }
    }

    /// Mixed integer and float operands compare by exact value, in either
    /// order: a float equals an integer only when it is that integer.
    #[test]
    fn numbers_compare_exactly_across_integer_and_float() {
        let num = |text: &str| -> Value { serde_json::from_str(text).expect("number") };
        for (a, b, equal) in [
            ("7", "7.0", true),
            ("-7", "-7.0", true),
            ("7", "7.5", false),
            ("9007199254740993", "9007199254740992.0", false),
            ("9007199254740992", "9007199254740992.0", true),
            ("-9007199254740993", "-9007199254740992.0", false),
            ("18446744073709551615", "18446744073709551616.0", false),
            ("18446744073709551615", "-1", false),
            ("9223372036854775807", "9223372036854775808.0", false),
            ("-9223372036854775808", "-9223372036854775808.0", true),
            ("1", "1e300", false),
        ] {
            assert_eq!(json_equal(&num(a), &num(b)), equal, "{a} vs {b}");
            assert_eq!(json_equal(&num(b), &num(a)), equal, "{b} vs {a}");
        }
    }

    /// `above` and `below` order integers, floats and the two mixed exactly.
    #[test]
    fn bounds_compare_exactly_across_integer_and_float() {
        for (n, bounds, holds) in [
            ("9007199254740993", "above: 9007199254740992", true),
            ("9007199254740992", "below: 9007199254740993", true),
            ("9007199254740993", "below: 9007199254740992", false),
            ("9007199254740993", "above: 9007199254740992.0", true),
            ("9007199254740992", "above: 9007199254740992.0", false),
            ("9007199254740992.0", "below: 9007199254740993", true),
            // A script's integers stop at `i64::MAX` (the YAML parser's limit); a
            // response's go to `u64::MAX`.
            ("18446744073709551615", "above: 9223372036854775807", true),
            ("9223372036854775807", "below: 9223372036854775806", false),
            ("-9223372036854775808", "below: -9223372036854775807", true),
            ("-1", "below: 18446744073709551615", true),
            ("1.5", "above: 1, below: 2", true),
            ("2", "above: 1.5, below: 2.5", true),
            ("2", "below: 2.0", false),
            ("1e300", "above: 18446744073709551615", true),
        ] {
            let body: Value = serde_json::from_str(&format!("{{\"n\": {n}}}")).expect("body");
            let check = assertion(&format!("{{ pointer: /n, {bounds} }}")).check(&body);
            assert_eq!(check.is_none(), holds, "{n} {bounds}: {check:?}");
        }
    }

    #[test]
    fn a_body_that_is_not_json_is_one_failure() {
        let failures = check_all(&[assertion("{ pointer: '', type: array }")], "oops");
        assert_eq!(failures.len(), 1);
        assert!(failures[0].starts_with("body is not JSON"), "{failures:?}");
    }
}
