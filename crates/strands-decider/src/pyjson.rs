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
//! The text Python's string and JSON functions produce, where the prompt depends on it
//! byte for byte: the model was trained on prompts Python rendered.

use std::fmt::Write;

use serde_json::{Number, Value};

/// Python's `str.isspace` for one character: Unicode white space, plus the four ASCII
/// separators (U+001C to U+001F) that Python counts and Rust does not.
pub(crate) fn is_space(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

/// Python's `str.strip()`.
pub(crate) fn strip(text: &str) -> &str {
    text.trim_matches(is_space)
}

/// Python's `" ".join(text.split())`: runs of white space become one space, with none at
/// either end.
pub(crate) fn collapse_space(text: &str) -> String {
    text.split(is_space)
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

/// Python's `json.dumps(value, indent=2, ensure_ascii=False)`. Object keys keep their order.
pub(crate) fn dumps_indented(value: &Value) -> String {
    let mut out = String::new();
    write_value(&mut out, value, 0);
    out
}

fn write_value(out: &mut String, value: &Value, depth: usize) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Number(number) => write_number(out, number),
        Value::String(text) => write_string(out, text),
        Value::Array(items) => {
            if items.is_empty() {
                out.push_str("[]");
                return;
            }
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                newline(out, depth + 1);
                write_value(out, item, depth + 1);
            }
            newline(out, depth);
            out.push(']');
        }
        Value::Object(fields) => {
            if fields.is_empty() {
                out.push_str("{}");
                return;
            }
            out.push('{');
            for (i, (key, item)) in fields.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                newline(out, depth + 1);
                write_string(out, key);
                out.push_str(": ");
                write_value(out, item, depth + 1);
            }
            newline(out, depth);
            out.push('}');
        }
    }
}

fn newline(out: &mut String, depth: usize) {
    out.push('\n');
    for _ in 0..depth {
        out.push_str("  ");
    }
}

/// With `ensure_ascii=False`, Python escapes only the quote, the backslash and the control
/// characters below U+0020.
fn write_string(out: &mut String, text: &str) {
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Integers as they are; floats as Python's `repr` writes them: the shortest digits that
/// read back to the same float, in positional notation for exponents -4 to 15 and in
/// scientific notation (`1e+16`, `1.5e-05`) outside them.
fn write_number(out: &mut String, number: &Number) {
    if number.is_i64() || number.is_u64() {
        out.push_str(&number.to_string());
        return;
    }
    let Some(x) = number.as_f64() else {
        out.push_str(&number.to_string());
        return;
    };
    out.push_str(&float_repr(x));
}

fn float_repr(x: f64) -> String {
    if x == 0.0 {
        return if x.is_sign_negative() { "-0.0" } else { "0.0" }.to_string();
    }
    // Rust's `{:e}` gives the shortest round-trip digits, as `d.ddde[-]x`.
    let sci = format!("{:e}", x.abs());
    let (mantissa, exponent) = sci.split_once('e').unwrap_or((&sci, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let sign = if x < 0.0 { "-" } else { "" };
    if (-4..16).contains(&exponent) {
        let n = i32::try_from(digits.len()).unwrap_or(i32::MAX);
        let body = if exponent < 0 {
            let zeros = usize::try_from(-exponent - 1).unwrap_or(0);
            format!("0.{}{digits}", "0".repeat(zeros))
        } else if exponent + 1 >= n {
            let zeros = usize::try_from(exponent + 1 - n).unwrap_or(0);
            format!("{digits}{}.0", "0".repeat(zeros))
        } else {
            let point = usize::try_from(exponent + 1).unwrap_or(0);
            format!("{}.{}", &digits[..point], &digits[point..])
        };
        format!("{sign}{body}")
    } else {
        let mantissa = if digits.len() > 1 {
            format!("{}.{}", &digits[..1], &digits[1..])
        } else {
            digits
        };
        let exp_sign = if exponent < 0 { '-' } else { '+' };
        format!("{sign}{mantissa}e{exp_sign}{:02}", exponent.abs())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn floats_print_as_python_repr() {
        // Expected values from CPython 3.12's repr().
        for (x, expected) in [
            (1.0, "1.0"),
            (0.5, "0.5"),
            (-2.25, "-2.25"),
            (100.0, "100.0"),
            (123.456, "123.456"),
            (0.0001, "0.0001"),
            (0.00001, "1e-05"),
            (1.5e-7, "1.5e-07"),
            (1e15, "1000000000000000.0"),
            (1e16, "1e+16"),
            (1.2345e20, "1.2345e+20"),
            (0.1 + 0.2, "0.30000000000000004"),
        ] {
            assert_eq!(float_repr(x), expected, "{x}");
        }
    }

    #[test]
    fn json_matches_python_dumps() {
        // Expected text from CPython 3.12:
        // json.dumps(v, indent=2, ensure_ascii=False) with the same key order.
        let value = json!({"b": [1, 2.5, {"x": null}], "a": {}, "é": "\"q\"\n\u{1}", "l": []});
        assert_eq!(
            dumps_indented(&value),
            "{\n  \"b\": [\n    1,\n    2.5,\n    {\n      \"x\": null\n    }\n  ],\n  \"a\": {},\n  \"é\": \"\\\"q\\\"\\n\\u0001\",\n  \"l\": []\n}"
        );
    }

    #[test]
    fn white_space_follows_python() {
        assert_eq!(strip("\u{1f} a b \n"), "a b");
        assert_eq!(collapse_space("  a\t\tb\n c\u{1c}d "), "a b c d");
        assert_eq!(collapse_space(""), "");
    }
}
