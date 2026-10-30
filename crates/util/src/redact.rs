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

//! Taking known secret values out of text that may repeat them, such as an
//! error a source returned or a recorded result.

use std::borrow::Cow;
use std::fmt::Write as _;

/// What a value taken out is replaced with.
pub const REDACTED: &str = "<redacted>";

/// A part of a value shorter than this is not taken out on its own: bare, it
/// would match unrelated text such as a line number.
pub const SHORTEST_PART: usize = 4;

/// How many times over JSON can have escaped a value in text it is taken out
/// of: a recorded result is JSON, a tool's answer in it can be JSON, and that
/// answer can quote what a source sent back.
const JSON_DEPTH: usize = 4;

/// `message` without `values`: each value, and each part of it that a source
/// can repeat on its own, as a database client repeats a connection string's
/// `password=...` without the rest of it. JSON can carry them escaped, by
/// whichever encoder wrote it and once for each level JSON nests in JSON's
/// strings, as a recorded result and a tool's answer in it do: each JSON string
/// in `message` that holds one once decoded is written again without it, and
/// each escaped spelling is taken out as written too, for a string cut short.
#[must_use]
pub fn redact_values<'m>(message: &'m str, values: &[String]) -> Cow<'m, str> {
    let spellings: Vec<&str> = values
        .iter()
        .flat_map(|value| {
            std::iter::once(value.as_str()).chain(
                value
                    .split(|c: char| c.is_whitespace() || matches!(c, ';' | '&' | '?'))
                    .filter(|part| part.chars().count() >= SHORTEST_PART),
            )
        })
        .filter(|spelling| !spelling.is_empty())
        .collect();
    if spellings.is_empty() {
        return Cow::Borrowed(message);
    }
    redact_spellings(message, &spellings, JSON_DEPTH)
}

/// `message` without `spellings`, as written and as JSON escapes them, with
/// each JSON string in it redacted the same way once decoded, down to `depth`
/// levels of JSON inside JSON. The JSON strings go first: taking a spelling
/// out as written could cut into the escape of another value in a string,
/// such as `u00e4` in `\u00e4`, so that the string no longer decodes.
fn redact_spellings<'m>(message: &'m str, spellings: &[&str], depth: usize) -> Cow<'m, str> {
    match redact_json_strings(message, spellings, depth) {
        Cow::Owned(redacted) => Cow::Owned(replace_spellings(&redacted, spellings).into_owned()),
        Cow::Borrowed(_) => replace_spellings(message, spellings),
    }
}

/// `message` with each of `spellings` it holds, as written or as JSON escapes
/// it up to [`JSON_DEPTH`] times, a letter outside ASCII as itself or as
/// `\uXXXX`, replaced with [`REDACTED`].
fn replace_spellings<'m>(message: &'m str, spellings: &[&str]) -> Cow<'m, str> {
    let mut present: Vec<Cow<'_, str>> = Vec::new();
    for &spelling in spellings {
        if message.contains(spelling) {
            present.push(Cow::Borrowed(spelling));
        }
        for first in [json_escaped(spelling), ascii_escaped(spelling)]
            .into_iter()
            .flatten()
        {
            let mut escaped = Some(first);
            for _ in 0..JSON_DEPTH {
                let Some(current) = escaped else {
                    break;
                };
                escaped = json_escaped(&current);
                if message.contains(current.as_str()) {
                    present.push(Cow::Owned(current));
                }
            }
        }
    }
    if present.is_empty() {
        return Cow::Borrowed(message);
    }
    // The longest first, so a quoted value goes whole rather than leaving its
    // quotes behind.
    present.sort_by_key(|value| std::cmp::Reverse(value.len()));
    present.dedup();
    let mut message = message.to_string();
    for value in present {
        message = message.replace(value.as_ref(), REDACTED);
    }
    Cow::Owned(message)
}

/// `value` as JSON writes it inside a string, when that differs from `value`.
fn json_escaped(value: &str) -> Option<String> {
    let quoted = serde_json::to_string(value).ok()?;
    let escaped = quoted.get(1..quoted.len() - 1)?;
    (escaped != value).then(|| escaped.to_string())
}

/// `value` as an encoder that writes only ASCII, such as Python's
/// `json.dumps`, writes it inside a JSON string: as [`json_escaped`] writes
/// it, with each letter outside ASCII as `\uXXXX`. `None` when `value` is
/// ASCII, so that both write it the same way.
fn ascii_escaped(value: &str) -> Option<String> {
    if value.is_ascii() {
        return None;
    }
    let escaped = json_escaped(value);
    let mut ascii = String::new();
    for letter in escaped.as_deref().unwrap_or(value).chars() {
        if letter.is_ascii() {
            ascii.push(letter);
        } else {
            for unit in letter.encode_utf16(&mut [0; 2]) {
                let _ = write!(ascii, "\\u{unit:04x}");
            }
        }
    }
    Some(ascii)
}

/// `text` with each JSON string in it that, decoded, holds one of `spellings`
/// written again without it: the decoded string is redacted as
/// [`redact_spellings`] redacts, one level less deep, so a value escaped by
/// any encoder, or in JSON inside the string, is found. Unchanged when no
/// JSON string in `text` holds one. A quote that pairs with another into a
/// string that holds none may not open a string at all, as in plain text
/// before JSON, so the scan goes on from the quote after it.
fn redact_json_strings<'t>(text: &'t str, spellings: &[&str], depth: usize) -> Cow<'t, str> {
    if depth == 0 {
        return Cow::Borrowed(text);
    }
    let mut redacted = String::new();
    let mut copied = 0;
    let mut from = 0;
    while let Some(offset) = text[from..].find('"') {
        let start = from + offset;
        let Some(end) = closing_quote(text, start) else {
            break;
        };
        let Ok(decoded) = serde_json::from_str::<String>(&text[start..=end]) else {
            // Not a JSON string; the quote that ended it may open one.
            from = start + 1;
            continue;
        };
        if let Cow::Owned(clean) = redact_spellings(&decoded, spellings, depth - 1) {
            redacted.push_str(&text[copied..start]);
            redacted.push_str(
                &serde_json::to_string(&clean).unwrap_or_else(|_| format!("\"{REDACTED}\"")),
            );
            copied = end + 1;
            from = end + 1;
        } else {
            from = start + 1;
        }
    }
    if copied == 0 {
        return Cow::Borrowed(text);
    }
    redacted.push_str(&text[copied..]);
    Cow::Owned(redacted)
}

/// The index of the quote that closes the JSON string `text` opens at
/// `start`: the next one no backslash escapes.
fn closing_quote(text: &str, start: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut i = start + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2,
            b'"' => return Some(i),
            _ => i += 1,
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A connection string goes whole, and each part of it a source can repeat
    /// alone goes too; a short part stays, so a port or a line number does.
    #[test]
    fn a_value_and_its_parts_are_taken_out() {
        let values = ["dbname=d host=db.internal port=5432 password=hunter2".to_string()];
        assert_eq!(
            redact_values(
                "Unable to connect at \"dbname=d host=db.internal port=5432 password=hunter2\": line 1",
                &values
            ),
            "Unable to connect at \"<redacted>\": line 1"
        );
        assert_eq!(
            redact_values("password=hunter2 refused for d", &values),
            "<redacted> refused for d"
        );
        assert!(matches!(
            redact_values("nothing to take out", &values),
            Cow::Borrowed(_)
        ));
        assert!(matches!(
            redact_values(
                r#"[{"type": "text", "text": "nothing \"to\" take out"}]"#,
                &values
            ),
            Cow::Borrowed(_)
        ));
    }

    /// A value JSON escapes twice, as a recorded result escapes a tool's answer
    /// that is JSON itself, is taken out, and both stay JSON.
    #[test]
    fn a_value_in_json_inside_json_is_taken_out() {
        let password = "pa\"ss\\word".to_string();
        let answer = serde_json::json!({"error": format!("refused: pg_pass = '{password}'")});
        let recorded =
            serde_json::json!([{"type": "text", "text": answer.to_string()}]).to_string();
        assert!(recorded.contains(r#"pa\\\"ss\\\\word"#), "{recorded}");
        let redacted = redact_values(&recorded, std::slice::from_ref(&password));
        let redacted_answer =
            serde_json::json!({"error": "refused: pg_pass = '<redacted>'"}).to_string();
        assert_eq!(
            redacted,
            serde_json::json!([{"type": "text", "text": redacted_answer}]).to_string()
        );
    }

    /// A value another encoder escaped differently, as Python's `json.dumps`
    /// writes a letter outside ASCII, is found once its JSON string is decoded.
    #[test]
    fn a_value_any_encoder_escaped_is_taken_out() {
        let password = "päss\"word".to_string();
        assert_eq!(
            redact_values(
                r#"Error calling tool peer__sql: {"error": "refused: pg_pass = 'p\u00e4ss\"word'"}"#,
                std::slice::from_ref(&password)
            ),
            r#"Error calling tool peer__sql: {"error": "refused: pg_pass = '<redacted>'"}"#
        );
    }

    /// A value that is also part of another value's escape, such as `u00e4`
    /// in Python's `\u00e4`, does not break the escape apart before the
    /// other value is found: in a whole JSON string, also as .NET writes it
    /// (`\u00E4`, which no spelling as written matches), and in one cut
    /// short.
    #[test]
    fn a_value_inside_another_value_s_escape_leaves_none_of_it() {
        let values = ["u00e4".to_string(), "leak-p\u{e4}ssword".to_string()];
        assert_eq!(
            redact_values(
                r#"Error calling tool peer__sql: {"error": "refused: pg_db = 'u00e4', pg_pass = 'leak-p\u00e4ssword'"}"#,
                &values
            ),
            r#"Error calling tool peer__sql: {"error": "refused: pg_db = '<redacted>', pg_pass = '<redacted>'"}"#
        );
        assert_eq!(
            redact_values(
                r#"{"error": "refused: pg_db = 'u00e4', pg_pass = 'leak-p\u00e4ssword' and more"#,
                &values
            ),
            r#"{"error": "refused: pg_db = '<redacted>', pg_pass = '<redacted>' and more"#
        );
        let values = ["u00E4".to_string(), "leak-p\u{e4}ssword".to_string()];
        assert_eq!(
            redact_values(
                r#"{"error": "refused: pg_db = 'u00E4', pg_pass = 'leak-p\u00E4ssword'"}"#,
                &values
            ),
            r#"{"error": "refused: pg_db = '<redacted>', pg_pass = '<redacted>'"}"#
        );
    }

    /// A quote in plain text before JSON, as an error that quotes the token
    /// it stopped at writes one, does not hide the JSON string after it,
    /// which here escapes the value as .NET does, so that only decoding the
    /// string finds it.
    #[test]
    fn a_value_in_json_after_a_stray_quote_is_taken_out() {
        let password = "p\u{e4}ssword".to_string();
        assert_eq!(
            redact_values(
                r#"Unexpected token '"'; details: {"error": "pg_pass = 'p\u00E4ssword'"}"#,
                std::slice::from_ref(&password)
            ),
            r#"Unexpected token '"'; details: {"error": "pg_pass = '<redacted>'"}"#
        );
    }

    /// A JSON string cut short, as a truncated record cuts one, cannot be
    /// decoded, so each escaped spelling is taken out as written.
    #[test]
    fn a_value_escaped_in_a_string_cut_short_is_taken_out() {
        let password = "pa\"ss".to_string();
        assert_eq!(
            redact_values(
                r#"{"text": "{\"error\": \"pg_pass = 'pa\\\"ss' and more"#,
                std::slice::from_ref(&password)
            ),
            r#"{"text": "{\"error\": \"pg_pass = '<redacted>' and more"#
        );
    }
}
